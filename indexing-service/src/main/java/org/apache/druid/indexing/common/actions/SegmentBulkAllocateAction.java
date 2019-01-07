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
import com.fasterxml.jackson.core.type.TypeReference;
import org.apache.druid.indexing.common.LockGranularity;
import org.apache.druid.indexing.common.TaskLockType;
import org.apache.druid.indexing.common.task.Task;
import org.apache.druid.indexing.overlord.LockRequest;
import org.apache.druid.indexing.overlord.LockRequestForNewSegment;
import org.apache.druid.indexing.overlord.LockResult;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.segment.realtime.appenderator.SegmentIdentifier;
import org.apache.druid.timeline.partition.HashBasedNumberedShardSpecFactory;
import org.apache.druid.timeline.partition.HashBasedNumberedShardSpecFactory.HashBasedNumberedShardSpecContext;
import org.apache.druid.timeline.partition.NoneShardSpecFactory;
import org.apache.druid.timeline.partition.ShardSpecFactory.EmptyContext;
import org.joda.time.Interval;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

public class SegmentBulkAllocateAction implements TaskAction<List<SegmentIdentifier>>
{
  // interval -> # of segments to allocate
  private final Map<Interval, Integer> allocateSpec;
  private final String baseSequenceName;
  private final boolean changeSegmentGranularity;
  private final List<String> partitionDimensions;
  private final Map<Interval, Set<Integer>> overshadowingSegments;

  @JsonCreator
  public SegmentBulkAllocateAction(
      @JsonProperty("allocateSpec") Map<Interval, Integer> allocateSpec,
      @JsonProperty("baseSequenceName") String baseSequenceName,
      @JsonProperty("changeSegmentGranularity") boolean changeSegmentGranularity,
      @JsonProperty("partitionDimensions") List<String> partitionDimensions,
      @JsonProperty("overshadowingSegments") Map<Interval, Set<Integer>> overshadowingSegments
  )
  {
    this.allocateSpec = allocateSpec;
    this.baseSequenceName = baseSequenceName;
    this.changeSegmentGranularity = changeSegmentGranularity;
    this.partitionDimensions = partitionDimensions;
    this.overshadowingSegments = overshadowingSegments;
  }

  @JsonProperty
  public Map<Interval, Integer> getAllocateSpec()
  {
    return allocateSpec;
  }

  @JsonProperty
  public String getBaseSequenceName()
  {
    return baseSequenceName;
  }

  @JsonProperty
  public boolean isChangeSegmentGranularity()
  {
    return changeSegmentGranularity;
  }

  @JsonProperty
  public List<String> getPartitionDimensions()
  {
    return partitionDimensions;
  }

  @JsonProperty
  public Map<Interval, Set<Integer>> getOvershadowingSegments()
  {
    return overshadowingSegments;
  }

  @Override
  public TypeReference<List<SegmentIdentifier>> getReturnTypeReference()
  {
    return new TypeReference<List<SegmentIdentifier>>()
    {
    };
  }

  // TODO: Map<Interval, SegmentIdentifier>
  @Override
  public List<SegmentIdentifier> perform(Task task, TaskActionToolbox toolbox)
  {
    final List<SegmentIdentifier> segmentIds = new ArrayList<>();

    for (Entry<Interval, Integer> entry : allocateSpec.entrySet()) {
      final Interval interval = entry.getKey();
      final int numSegmentsToAllocate = entry.getValue();
      final LockRequest lockRequest;
      if (numSegmentsToAllocate == 1) {
        lockRequest = new LockRequestForNewSegment<>(
            changeSegmentGranularity ? LockGranularity.TIME_CHUNK : LockGranularity.SEGMENT,
            TaskLockType.EXCLUSIVE,
            task.getGroupId(),
            task.getDataSource(),
            interval,
            NoneShardSpecFactory.instance(),
            task.getPriority(),
            numSegmentsToAllocate,
            baseSequenceName,
            null,
            true,
            overshadowingSegments.get(interval),
            i -> EmptyContext.instance()
        );
      } else {
        lockRequest = new LockRequestForNewSegment<>(
            changeSegmentGranularity ? LockGranularity.TIME_CHUNK : LockGranularity.SEGMENT,
            TaskLockType.EXCLUSIVE,
            task.getGroupId(),
            task.getDataSource(),
            interval,
            new HashBasedNumberedShardSpecFactory(partitionDimensions, numSegmentsToAllocate),
            task.getPriority(),
            numSegmentsToAllocate,
            baseSequenceName,
            null,
            true,
            overshadowingSegments.get(interval),
            HashBasedNumberedShardSpecContext::new
        );
      }

      final LockResult lockResult = toolbox.getTaskLockbox().tryLock(task, lockRequest);

      if (lockResult.isRevoked()) {
        // The lock was preempted by other tasks
        throw new ISE("The lock for interval[%s] is preempted and no longer valid", interval);
      }

      if (lockResult.isOk()) {
        final List<SegmentIdentifier> identifiers = lockResult.getNewSegmentIds();
        if (!identifiers.isEmpty()) {
          if (identifiers.size() == numSegmentsToAllocate) {
            segmentIds.addAll(identifiers);
          } else {
            throw new ISE(
                "WTH? we requested [%s] segmentIds, but got [%s] with request[%s]",
                numSegmentsToAllocate,
                identifiers.size(),
                lockRequest
            );
          }
        } else {
          throw new ISE("Cannot allocate new pending segmentIds with request[%s]", lockRequest);
        }
      } else {
        throw new ISE("Could not acquire lock with request[%s]", lockRequest);
      }
    }

    return segmentIds;
  }

  @Override
  public boolean isAudited()
  {
    return false;
  }

  @Override
  public String toString()
  {
    return "SegmentBulkAllocateAction{" +
           "allocateSpec=" + allocateSpec +
           ", baseSequenceName='" + baseSequenceName + '\'' +
           ", changeSegmentGranularity=" + changeSegmentGranularity +
           ", partitionDimensions=" + partitionDimensions +
           ", overshadowingSegments=" + overshadowingSegments +
           '}';
  }
}
