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

package org.apache.druid.indexing.common.actions;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonSubTypes.Type;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.fasterxml.jackson.core.type.TypeReference;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableSet;
import org.apache.druid.indexing.common.LockGranularity;
import org.apache.druid.indexing.common.TaskLockType;
import org.apache.druid.indexing.common.task.Task;
import org.apache.druid.indexing.overlord.IndexerMetadataStorageCoordinator;
import org.apache.druid.indexing.overlord.LockRequestForNewSegment;
import org.apache.druid.indexing.overlord.LockResult;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.java.util.common.granularity.Granularity;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.segment.realtime.appenderator.SegmentIdentifier;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.timeline.partition.NumberedShardSpecFactory;
import org.apache.druid.timeline.partition.ShardSpecFactory;
import org.apache.druid.timeline.partition.ShardSpecFactory.Context;
import org.apache.druid.timeline.partition.ShardSpecFactory.EmptyContext;
import org.joda.time.DateTime;
import org.joda.time.Interval;

import javax.annotation.Nullable;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Allocates a pending segment for a given timestamp. The preferredSegmentGranularity is used if there are no prior
 * segments for the given timestamp, or if the prior segments for the given timestamp are already at the
 * preferredSegmentGranularity. Otherwise, the prior segments will take precedence.
 * <p/>
 * This action implicitly acquires locks when it allocates segments. You do not have to acquire them beforehand,
 * although you *do* have to release them yourself.
 * <p/>
 * If this action cannot acquire an appropriate lock, or if it cannot expand an existing segment set, it returns null.
 */
public class SegmentAllocateAction implements TaskAction<SegmentIdentifier>
{
  private static final Logger log = new Logger(SegmentAllocateAction.class);

  // Prevent spinning forever in situations where the segment list just won't stop changing.
  private static final int MAX_ATTEMPTS = 90;

  private final LockGranularity lockGranularity;
  private final String dataSource;
  private final DateTime timestamp;
  private final Granularity queryGranularity;
  private final TryIntervalsProvider tryIntervalsProvider;
  private final String sequenceName;
  private final String previousSegmentId;
  private final boolean skipSegmentLineageCheck;
  private final ShardSpecFactory shardSpecFactory;
  private final ShardSpecFactory.Context context;
  private final Set<Integer> overshadowingSegments;
  private final boolean firstSegmentInTimeChunk;

  @JsonCreator
  public SegmentAllocateAction(
      @JsonProperty("lockGranularity") @Nullable LockGranularity lockGranularity, // nullable for compatibility
      @JsonProperty("dataSource") String dataSource,
      @JsonProperty("timestamp") DateTime timestamp,
      @JsonProperty("queryGranularity") Granularity queryGranularity,
      @JsonProperty("preferredSegmentGranularity") @Deprecated @Nullable Granularity preferredSegmentGranularity,
      @JsonProperty("tryIntervalsProvider") @Nullable TryIntervalsProvider tryIntervalsProvider,
      @JsonProperty("sequenceName") String sequenceName,
      @JsonProperty("previousSegmentId") String previousSegmentId,
      @JsonProperty("skipSegmentLineageCheck") boolean skipSegmentLineageCheck,
      @JsonProperty("shardSpecFactory") @Nullable ShardSpecFactory shardSpecFactory,
      @JsonProperty("context") @Nullable Context context,
      @JsonProperty("overshadowingSegments") @Nullable Set<Integer> overshadowingSegments, // for backward compatibility
      @JsonProperty("firstSegmentInTimeChunk") boolean firstSegmentInTimeChunk // false if it's null
  )
  {
    this.lockGranularity = lockGranularity == null ? LockGranularity.TIME_CHUNK : lockGranularity;
    this.dataSource = Preconditions.checkNotNull(dataSource, "dataSource");
    this.timestamp = Preconditions.checkNotNull(timestamp, "timestamp");
    this.queryGranularity = Preconditions.checkNotNull(queryGranularity, "queryGranularity");
    Preconditions.checkArgument(preferredSegmentGranularity != null || tryIntervalsProvider != null, "TODO: proper error message");
    this.tryIntervalsProvider = tryIntervalsProvider == null ? new GranularityBasedIntervalsProvider(preferredSegmentGranularity) : tryIntervalsProvider;
    this.sequenceName = Preconditions.checkNotNull(sequenceName, "sequenceName");
    this.previousSegmentId = previousSegmentId;
    this.skipSegmentLineageCheck = skipSegmentLineageCheck;
    this.shardSpecFactory = shardSpecFactory;
    this.context = context;
    this.overshadowingSegments = overshadowingSegments == null ? Collections.emptySet() : overshadowingSegments;
    this.firstSegmentInTimeChunk = firstSegmentInTimeChunk;
  }

  public SegmentAllocateAction(
      @Nullable LockGranularity lockGranularity,
      String dataSource,
      DateTime timestamp,
      Granularity queryGranularity,
      @Nullable TryIntervalsProvider tryIntervalsProvider,
      String sequenceName,
      String previousSegmentId,
      boolean skipSegmentLineageCheck,
      Set<Integer> overshadowingSegments,
      boolean firstSegmentInTimeChunk
  )
  {
    this(
        lockGranularity,
        dataSource,
        timestamp,
        queryGranularity,
        null,
        tryIntervalsProvider,
        sequenceName,
        previousSegmentId,
        skipSegmentLineageCheck,
        new NumberedShardSpecFactory(0),
        EmptyContext.instance(),
        overshadowingSegments,
        firstSegmentInTimeChunk
    );
  }

  @JsonProperty
  public LockGranularity getLockGranularity()
  {
    return lockGranularity;
  }

  @JsonProperty
  public String getDataSource()
  {
    return dataSource;
  }

  @JsonProperty
  public DateTime getTimestamp()
  {
    return timestamp;
  }

  @JsonProperty
  public Granularity getQueryGranularity()
  {
    return queryGranularity;
  }

  @JsonProperty
  @Nullable
  public TryIntervalsProvider getTryIntervalsProvider()
  {
    return tryIntervalsProvider;
  }

  @JsonProperty
  public String getSequenceName()
  {
    return sequenceName;
  }

  @JsonProperty
  public String getPreviousSegmentId()
  {
    return previousSegmentId;
  }

  @JsonProperty
  public boolean isSkipSegmentLineageCheck()
  {
    return skipSegmentLineageCheck;
  }

  @Nullable
  @JsonProperty
  public ShardSpecFactory getShardSpecFactory()
  {
    return shardSpecFactory;
  }

  @JsonProperty
  public Set<Integer> getOvershadowingSegments()
  {
    return overshadowingSegments;
  }

  @JsonProperty
  public boolean isFirstSegmentInTimeChunk()
  {
    return firstSegmentInTimeChunk;
  }

  @Override
  public TypeReference<SegmentIdentifier> getReturnTypeReference()
  {
    return new TypeReference<SegmentIdentifier>()
    {
    };
  }

  @Override
  public SegmentIdentifier perform(
      final Task task,
      final TaskActionToolbox toolbox
  )
  {
    int attempt = 0;
    while (true) {
      attempt++;

      if (!task.getDataSource().equals(dataSource)) {
        throw new IAE("Task dataSource must match action dataSource, [%s] != [%s].", task.getDataSource(), dataSource);
      }

      final IndexerMetadataStorageCoordinator msc = toolbox.getIndexerMetadataStorageCoordinator();

      // 1) if something overlaps our timestamp, use that
      // 2) otherwise try preferredSegmentGranularity & going progressively smaller

      final Interval rowInterval = queryGranularity.bucket(timestamp);

      final SegmentIdentifier identifier;
      final Set<DataSegment> usedSegmentsForRow;
      if (lockGranularity == LockGranularity.TIME_CHUNK && firstSegmentInTimeChunk) {
        identifier = tryAllocateFirstSegment(toolbox, task, rowInterval);
        usedSegmentsForRow = Collections.emptySet();
      } else {
        usedSegmentsForRow = msc
            .getUsedSegmentsForInterval(dataSource, rowInterval)
            .stream()
            .filter(segment -> !overshadowingSegments.contains(segment.getShardSpec().getPartitionNum()))
            .collect(Collectors.toSet());

        identifier = usedSegmentsForRow.isEmpty() ?
                     tryAllocateFirstSegment(toolbox, task, rowInterval) :
                     tryAllocateSubsequentSegment(toolbox, task, rowInterval, usedSegmentsForRow.iterator().next());
      }

      // TODO: should call tryAllocateFirstSegment if it's the first call and firstSegmentInTimeChunk=true
      if (identifier != null) {
        return identifier;
      }

      // Could not allocate a pending segment. There's a chance that this is because someone else inserted a segment
      // overlapping with this row between when we called "mdc.getUsedSegmentsForInterval" and now. Check it again,
      // and if it's different, repeat.

      if (!ImmutableSet.copyOf(msc.getUsedSegmentsForInterval(dataSource, rowInterval)).equals(usedSegmentsForRow)) {
        if (attempt < MAX_ATTEMPTS) {
          final long shortRandomSleep = 50 + (long) (Math.random() * 450);
          log.debug(
              "Used segment set changed for rowInterval[%s]. Retrying segment allocation in %,dms (attempt = %,d).",
              rowInterval,
              shortRandomSleep,
              attempt
          );
          try {
            Thread.sleep(shortRandomSleep);
          }
          catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw Throwables.propagate(e);
          }
        } else {
          log.error(
              "Used segment set changed for rowInterval[%s]. Not trying again (attempt = %,d).",
              rowInterval,
              attempt
          );
          return null;
        }
      } else {
        return null;
      }
    }
  }

  @JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "type")
  @JsonSubTypes(value = {
      @Type(name = "granularityBased", value = GranularityBasedIntervalsProvider.class),
      @Type(name = "singleInterval", value = SingleIntervalProvider.class)
  })
  public interface TryIntervalsProvider
  {
    List<Interval> getTryIntervals(DateTime timestamp);
  }

  public static class GranularityBasedIntervalsProvider implements TryIntervalsProvider
  {
    private final Granularity preferredSegmentGranularity;

    @JsonCreator
    public GranularityBasedIntervalsProvider(@JsonProperty("preferredSegmentGranularity") Granularity preferredSegmentGranularity)
    {
      this.preferredSegmentGranularity = preferredSegmentGranularity;
    }

    @JsonProperty
    public Granularity getPreferredSegmentGranularity()
    {
      return preferredSegmentGranularity;
    }

    @Override
    public List<Interval> getTryIntervals(DateTime timestamp)
    {
      // No existing segments for this row, but there might still be nearby ones that conflict with our preferred
      // segment granularity. Try that first, and then progressively smaller ones if it fails.
      return Granularity.granularitiesFinerThan(preferredSegmentGranularity)
                        .stream()
                        .map(granularity -> granularity.bucket(timestamp))
                        .collect(Collectors.toList());
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
      GranularityBasedIntervalsProvider that = (GranularityBasedIntervalsProvider) o;
      return Objects.equals(preferredSegmentGranularity, that.preferredSegmentGranularity);
    }

    @Override
    public int hashCode()
    {
      return Objects.hash(preferredSegmentGranularity);
    }
  }

  public static class SingleIntervalProvider implements TryIntervalsProvider
  {
    private final Interval interval;

    @JsonCreator
    public SingleIntervalProvider(@JsonProperty("interval") Interval interval)
    {
      this.interval = interval;
    }

    @JsonProperty
    public Interval getInterval()
    {
      return interval;
    }

    @Override
    public List<Interval> getTryIntervals(DateTime timestamp)
    {
      return Collections.singletonList(interval);
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
      SingleIntervalProvider that = (SingleIntervalProvider) o;
      return Objects.equals(interval, that.interval);
    }

    @Override
    public int hashCode()
    {
      return Objects.hash(interval);
    }
  }

  public static TryIntervalsProvider from(Granularity segmentGranularity, @Nullable Interval totalInputInterval)
  {
    if (segmentGranularity.equals(Granularities.ALL)) {
      return new SingleIntervalProvider(Preconditions.checkNotNull(totalInputInterval, "totalInputInterval"));
    } else {
      return new GranularityBasedIntervalsProvider(segmentGranularity);
    }
  }

  private SegmentIdentifier tryAllocateFirstSegment(TaskActionToolbox toolbox, Task task, Interval rowInterval)
  {
    final List<Interval> tryIntervals = tryIntervalsProvider.getTryIntervals(timestamp);

    for (Interval tryInterval : tryIntervals) {
      if (tryInterval.contains(rowInterval)) {
        final SegmentIdentifier identifier = tryAllocate(toolbox, task, tryInterval, rowInterval, false);
        if (identifier != null) {
          return identifier;
        }
      }
    }
    return null;
  }

  private SegmentIdentifier tryAllocateSubsequentSegment(
      TaskActionToolbox toolbox,
      Task task,
      Interval rowInterval,
      DataSegment usedSegment
  )
  {
    // Existing segment(s) exist for this row; use the interval of the first one.
    if (!usedSegment.getInterval().contains(rowInterval)) {
      log.error("The interval of existing segment[%s] doesn't contain rowInterval[%s]", usedSegment, rowInterval);
      return null;
    } else {
      // If segment allocation failed here, it is highly likely an unrecoverable error. We log here for easier
      // debugging.
      return tryAllocate(toolbox, task, usedSegment.getInterval(), rowInterval, true);
    }
  }

  private SegmentIdentifier tryAllocate(
      TaskActionToolbox toolbox,
      Task task,
      Interval tryInterval,
      Interval rowInterval,
      boolean logOnFail
  )
  {
    // This action is always used by appending tasks, which cannot change the segment granularity of existing
    // dataSources. So, all lock requests should be segmentLock.
    final LockResult lockResult = toolbox.getTaskLockbox().tryLock(
        task,
        // TODO: type safe
        new LockRequestForNewSegment<>(
            lockGranularity,
            TaskLockType.EXCLUSIVE,
            task.getGroupId(),
            dataSource,
            tryInterval,
            shardSpecFactory,
            task.getPriority(),
            1,
            sequenceName,
            previousSegmentId,
            skipSegmentLineageCheck,
            overshadowingSegments,
            i -> context
        )
    );

    if (lockResult.isRevoked()) {
      // We had acquired a lock but it was preempted by other locks
      throw new ISE("The lock for interval[%s] is preempted and no longer valid", tryInterval);
    }

    if (lockResult.isOk()) {
      final List<SegmentIdentifier> identifiers = lockResult.getNewSegmentIds();
      if (!identifiers.isEmpty()) {
        if (identifiers.size() == 1) {
          return identifiers.get(0);
        } else {
          throw new ISE("WTH? multiple segmentIds[%s] were created?", identifiers);
        }
      } else {
        final String msg = StringUtils.format(
            "Could not allocate pending segment for rowInterval[%s], segmentInterval[%s].",
            rowInterval,
            tryInterval
        );
        if (logOnFail) {
          log.error(msg);
        } else {
          log.debug(msg);
        }
        return null;
      }
    } else {
      final String msg = StringUtils.format(
          "Could not acquire lock for rowInterval[%s], segmentInterval[%s].",
          rowInterval,
          tryInterval
      );
      if (logOnFail) {
        log.error(msg);
      } else {
        log.debug(msg);
      }
      return null;
    }
  }

  @Override
  public boolean isAudited()
  {
    return false;
  }

  @Override
  public String toString()
  {
    return "SegmentAllocateAction{" +
           "lockGranularity=" + lockGranularity +
           ", dataSource='" + dataSource + '\'' +
           ", timestamp=" + timestamp +
           ", queryGranularity=" + queryGranularity +
           ", tryIntervalsProvider=" + tryIntervalsProvider +
           ", sequenceName='" + sequenceName + '\'' +
           ", previousSegmentId='" + previousSegmentId + '\'' +
           ", skipSegmentLineageCheck=" + skipSegmentLineageCheck +
           ", shardSpecFactory=" + shardSpecFactory +
           ", context=" + context +
           ", overshadowingSegments=" + overshadowingSegments +
           ", firstSegmentInTimeChunk=" + firstSegmentInTimeChunk +
           '}';
  }
}
