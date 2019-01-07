/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.druid.indexing.common.task;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Joiner;
import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import org.apache.druid.indexer.TaskStatus;
import org.apache.druid.indexing.common.TaskLock;
import org.apache.druid.indexing.common.TaskLockType;
import org.apache.druid.indexing.common.actions.LockAcquireAction;
import org.apache.druid.indexing.common.actions.LockListAction;
import org.apache.druid.indexing.common.actions.LockTryAcquireAction;
import org.apache.druid.indexing.common.actions.SegmentListUsedAction;
import org.apache.druid.indexing.common.actions.TaskActionClient;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.JodaUtils;
import org.apache.druid.query.Query;
import org.apache.druid.query.QueryRunner;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.timeline.TimelineLookup;
import org.apache.druid.timeline.TimelineObjectHolder;
import org.apache.druid.timeline.VersionedIntervalTimeline;
import org.apache.druid.timeline.partition.PartitionChunk;
import org.joda.time.Interval;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

public abstract class AbstractTask implements Task
{
  private static final Joiner ID_JOINER = Joiner.on("_");

  @JsonIgnore
  private final String id;

  @JsonIgnore
  private final String groupId;

  @JsonIgnore
  private final TaskResource taskResource;

  @JsonIgnore
  private final String dataSource;

  private final Map<String, Object> context;

  private final Map<Interval, Set<Integer>> inputSegmentPartitionIds = new HashMap<>(); // TODO: Intset

  private boolean initializedLock;

  private boolean changeSegmentGranularity;

  protected AbstractTask(String id, String dataSource, Map<String, Object> context)
  {
    this(id, null, null, dataSource, context);
  }

  protected AbstractTask(
      String id,
      @Nullable String groupId,
      @Nullable TaskResource taskResource,
      String dataSource,
      @Nullable Map<String, Object> context
  )
  {
    this.id = Preconditions.checkNotNull(id, "id");
    this.groupId = groupId == null ? id : groupId;
    this.taskResource = taskResource == null ? new TaskResource(id, 1) : taskResource;
    this.dataSource = Preconditions.checkNotNull(dataSource, "dataSource");
    this.context = context == null ? new HashMap<>() : context;
  }

  public static String getOrMakeId(String id, final String typeName, String dataSource)
  {
    return getOrMakeId(id, typeName, dataSource, null);
  }

  static String getOrMakeId(String id, final String typeName, String dataSource, @Nullable Interval interval)
  {
    if (id != null) {
      return id;
    }

    final List<Object> objects = new ArrayList<>();
    objects.add(typeName);
    objects.add(dataSource);
    if (interval != null) {
      objects.add(interval.getStart());
      objects.add(interval.getEnd());
    }
    objects.add(DateTimes.nowUtc().toString());

    return joinId(objects);
  }

  @JsonProperty
  @Override
  public String getId()
  {
    return id;
  }

  @JsonProperty
  @Override
  public String getGroupId()
  {
    return groupId;
  }

  @JsonProperty("resource")
  @Override
  public TaskResource getTaskResource()
  {
    return taskResource;
  }

  @Override
  public String getNodeType()
  {
    return null;
  }

  @JsonProperty
  @Override
  public String getDataSource()
  {
    return dataSource;
  }

  @Override
  public <T> QueryRunner<T> getQueryRunner(Query<T> query)
  {
    return null;
  }

  @Override
  public String getClasspathPrefix()
  {
    return null;
  }

  @Override
  public boolean canRestore()
  {
    return false;
  }

  @Override
  public void stopGracefully()
  {
    // Should not be called when canRestore = false.
    throw new UnsupportedOperationException("Cannot stop gracefully");
  }

  @Override
  public String toString()
  {
    return "AbstractTask{" +
           "id='" + id + '\'' +
           ", groupId='" + groupId + '\'' +
           ", taskResource=" + taskResource +
           ", dataSource='" + dataSource + '\'' +
           ", context=" + context +
           '}';
  }

  /**
   * Start helper methods
   *
   * @param objects objects to join
   *
   * @return string of joined objects
   */
  static String joinId(List<Object> objects)
  {
    return ID_JOINER.join(objects);
  }

  static String joinId(Object...objects)
  {
    return ID_JOINER.join(objects);
  }

  protected boolean tryLockWithIntervals(TaskActionClient client, Set<Interval> intervals) throws IOException
  {
    return tryLockWithIntervals(client, new ArrayList<>(intervals));
  }

  protected boolean tryLockWithIntervals(TaskActionClient client, List<Interval> intervals) throws IOException
  {
    if (initializedLock) {
      return true;
    }

    if (isOverwriteMode()) {
      // TODO: check changeSegmentGranularity and get timeChunkLock here

      // TODO: race - a new segment can be added after getInputSegments. change to lockAllSegmentsInIntervals
      return tryLockWithSegments(client, getInputSegments(client, intervals));
    } else {
      initializedLock = true;
      return true;
    }
  }

  protected boolean tryLockWithSegments(TaskActionClient client, List<DataSegment> segments) throws IOException
  {
    if (initializedLock) {
      return true;
    }

    if (isOverwriteMode()) {
      if (segments.isEmpty()) {
        initializedLock = true;
        return true;
      }

      // Create a timeline to find latest segments only
      final List<Interval> intervals = segments.stream().map(DataSegment::getInterval).collect(Collectors.toList());
      final TimelineLookup<String, DataSegment> timeline = VersionedIntervalTimeline.forSegments(segments);
      final List<DataSegment> visibleSegments = timeline.lookup(JodaUtils.umbrellaInterval(intervals))
                                                 .stream()
                                                 .map(TimelineObjectHolder::getObject)
                                                 .flatMap(partitionHolder -> StreamSupport.stream(partitionHolder.spliterator(), false))
                                                 .map(PartitionChunk::getObject)
                                                 .collect(Collectors.toList());

      changeSegmentGranularity = changeSegmentGranularity(intervals);
      if (changeSegmentGranularity) {
        for (Interval interval : JodaUtils.condenseIntervals(intervals)) {
          final TaskLock lock = client.submit(LockTryAcquireAction.createTimeChunkRequest(TaskLockType.EXCLUSIVE, interval));
          if (lock == null) {
            return false;
          }
        }
        initializedLock = true;
        return true;
      } else {
        for (DataSegment segment : visibleSegments) {
          inputSegmentPartitionIds.computeIfAbsent(segment.getInterval(), k -> new HashSet<>())
                                  .add(segment.getShardSpec().getPartitionNum());
        }
        for (Entry<Interval, Set<Integer>> entry : inputSegmentPartitionIds.entrySet()) {
          final TaskLock lock = client.submit(
              LockTryAcquireAction.createSegmentRequest(TaskLockType.EXCLUSIVE, entry.getKey(), visibleSegments.get(0).getVersion(), entry.getValue())
          );
          if (lock == null) {
            return false;
          }
        }
        initializedLock = true;
        return true;
      }
    } else {
      initializedLock = true;
      return true;
    }
  }

  protected void lockWithIntervals(TaskActionClient client, Set<Interval> intervals, long timeoutMs) throws IOException
  {
    lockWithIntervals(client, new ArrayList<>(intervals), timeoutMs);
  }

  protected void lockWithIntervals(TaskActionClient client, List<Interval> intervals, long timeoutMs) throws IOException
  {
    if (initializedLock) {
      return;
    }

    if (isOverwriteMode()) {
      final List<DataSegment> usedSegments = client.submit(new SegmentListUsedAction(getDataSource(), null, intervals));

      // Create a timeline to find latest segments only
      final TimelineLookup<String, DataSegment> timeline = VersionedIntervalTimeline.forSegments(usedSegments);
      final List<DataSegment> segments = timeline.lookup(JodaUtils.umbrellaInterval(intervals))
                                                 .stream()
                                                 .map(TimelineObjectHolder::getObject)
                                                 .flatMap(partitionHolder -> StreamSupport.stream(partitionHolder.spliterator(), false))
                                                 .map(PartitionChunk::getObject)
                                                 .collect(Collectors.toList());

      lockWithSegments(client, segments, timeoutMs);
    }
  }

  protected void lockWithSegments(TaskActionClient client, List<DataSegment> segments, long timeoutMs) throws IOException
  {
    if (initializedLock) {
      return;
    }

    if (isOverwriteMode()) {
      if (segments.isEmpty()) {
        initializedLock = true;
        return;
      }

      final List<Interval> intervals = segments.stream().map(DataSegment::getInterval).collect(Collectors.toList());
      changeSegmentGranularity = changeSegmentGranularity(intervals);
      if (changeSegmentGranularity) {
        for (Interval interval : JodaUtils.condenseIntervals(intervals)) {
          final TaskLock lock = client.submit(LockAcquireAction.createTimeChunkRequest(TaskLockType.EXCLUSIVE, interval, timeoutMs));
          if (lock == null) {
            throw new ISE("Failed to get a lock for interval[%s]", interval);
          }
        }
      } else {
        for (DataSegment segment : segments) {
          inputSegmentPartitionIds.computeIfAbsent(segment.getInterval(), k -> new HashSet<>())
                                  .add(segment.getShardSpec().getPartitionNum());
        }
        for (Entry<Interval, Set<Integer>> entry : inputSegmentPartitionIds.entrySet()) {
          final Interval interval = entry.getKey();
          final Set<Integer> partitionIds = entry.getValue();
          final TaskLock lock = client.submit(
              LockAcquireAction.createSegmentRequest(TaskLockType.EXCLUSIVE, interval, segments.get(0).getVersion(), partitionIds, timeoutMs)
          );
          if (lock == null) {
            throw new ISE("Failed to get a lock for interval[%s] and partitionIds[%s] with version[%s]", interval, partitionIds, segments.get(0).getVersion());
          }
        }
      }
    }

    initializedLock = true;
  }

  protected boolean isChangeSegmentGranularity()
  {
    Preconditions.checkState(initializedLock, "Lock must be initialized before calling this method");
    return changeSegmentGranularity;
  }

  public TaskStatus success()
  {
    return TaskStatus.success(getId());
  }

  @Override
  public boolean equals(Object o)
  {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    AbstractTask that = (AbstractTask) o;

    if (!id.equals(that.id)) {
      return false;
    }

    if (!groupId.equals(that.groupId)) {
      return false;
    }

    if (!dataSource.equals(that.dataSource)) {
      return false;
    }

    return context.equals(that.context);
  }

  @Override
  public int hashCode()
  {
    return Objects.hashCode(id, groupId, dataSource, context);
  }

  public Map<Interval, Set<Integer>> getAllInputPartitionIds()
  {
    return inputSegmentPartitionIds;
  }

  @Nullable
  public Set<Integer> getInputPartitionIdsFor(Interval interval)
  {
    return inputSegmentPartitionIds.get(interval);
  }

  public static List<TaskLock> getTaskLocks(TaskActionClient client) throws IOException
  {
    return client.submit(new LockListAction());
  }

  @Override
  @JsonProperty
  public Map<String, Object> getContext()
  {
    return context;
  }
}
