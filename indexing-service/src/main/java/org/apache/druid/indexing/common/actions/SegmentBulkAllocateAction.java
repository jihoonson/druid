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
import org.apache.druid.timeline.partition.ShardSpecFactory;
import org.apache.druid.timeline.partition.ShardSpecFactory.Context;
import org.apache.druid.timeline.partition.ShardSpecFactory.EmptyContext;
import org.joda.time.Interval;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.function.IntFunction;

public class SegmentBulkAllocateAction implements TaskAction<Map<Interval, List<SegmentIdentifier>>>
{
  // interval -> # of segments to allocate
  private final Map<Interval, Integer> allocateSpec;
  private final String baseSequenceName;
  private final List<String> partitionDimensions;
  private final Map<Interval, Set<Integer>> overshadowingSegments;

  @JsonCreator
  public SegmentBulkAllocateAction(
      @JsonProperty("allocateSpec") Map<Interval, Integer> allocateSpec,
      @JsonProperty("baseSequenceName") String baseSequenceName,
      @JsonProperty("partitionDimensions") List<String> partitionDimensions,
      @JsonProperty("overshadowingSegments") Map<Interval, Set<Integer>> overshadowingSegments
  )
  {
    this.allocateSpec = allocateSpec;
    this.baseSequenceName = baseSequenceName;
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
  public TypeReference<Map<Interval, List<SegmentIdentifier>>> getReturnTypeReference()
  {
    return new TypeReference<Map<Interval, List<SegmentIdentifier>>>()
    {
    };
  }

  @Override
  public Map<Interval, List<SegmentIdentifier>> perform(Task task, TaskActionToolbox toolbox)
  {
    final Map<Interval, List<SegmentIdentifier>> segmentIds = new HashMap<>(allocateSpec.size());

    for (Entry<Interval, Integer> entry : allocateSpec.entrySet()) {
      final Interval interval = entry.getKey();
      final int numSegmentsToAllocate = entry.getValue();
      final ShardSpecFactory shardSpecFactory = createShardSpecFactory(numSegmentsToAllocate);
      final IntFunction<Context> contextFn = createShardSpecFactoryContextFn(numSegmentsToAllocate);
      //noinspection unchecked
      final LockRequest lockRequest = new LockRequestForNewSegment<>(
          TaskLockType.EXCLUSIVE,
          task.getGroupId(),
          task.getDataSource(),
          interval,
          shardSpecFactory,
          task.getPriority(),
          numSegmentsToAllocate,
          baseSequenceName,
          null,
          true,
          overshadowingSegments.get(interval),
          contextFn
      );

      final LockResult lockResult = toolbox.getTaskLockbox().tryLock(task, lockRequest);

      if (lockResult.isRevoked()) {
        // The lock was preempted by other tasks
        throw new ISE("The lock for interval[%s] is preempted and no longer valid", interval);
      }

      if (lockResult.isOk()) {
        final List<SegmentIdentifier> identifiers = lockResult.getNewSegmentIds();
        if (!identifiers.isEmpty()) {
          if (identifiers.size() == numSegmentsToAllocate) {
            segmentIds.put(interval, identifiers);
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

  private ShardSpecFactory createShardSpecFactory(int numSegmentsToAllocate)
  {
    if (numSegmentsToAllocate == 1) {
      return NoneShardSpecFactory.instance();
    } else {
      return new HashBasedNumberedShardSpecFactory(partitionDimensions, numSegmentsToAllocate);
    }
  }

  private IntFunction<Context> createShardSpecFactoryContextFn(int numSegmentsToAllocate)
  {
    if (numSegmentsToAllocate == 1) {
      return i -> EmptyContext.instance();
    } else {
      return HashBasedNumberedShardSpecContext::new;
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
    return "SegmentBulkAllocateAction{" +
           "allocateSpec=" + allocateSpec +
           ", baseSequenceName='" + baseSequenceName + '\'' +
           ", partitionDimensions=" + partitionDimensions +
           ", overshadowingSegments=" + overshadowingSegments +
           '}';
  }
}
