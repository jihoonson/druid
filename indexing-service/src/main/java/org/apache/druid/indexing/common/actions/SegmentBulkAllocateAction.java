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
import org.apache.druid.indexing.overlord.LockResult;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.Pair;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.segment.realtime.appenderator.SegmentIdentifier;
import org.apache.druid.timeline.partition.HashBasedNumberedShardSpec;
import org.apache.druid.timeline.partition.NoneShardSpec;
import org.joda.time.Interval;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

public class SegmentBulkAllocateAction implements TaskAction<List<SegmentIdentifier>>
{
  // interval -> # of segments to allocate
  private final Map<Interval, Integer> allocateSpec;
  private final String baseSequenceName;

  @JsonCreator
  public SegmentBulkAllocateAction(
      @JsonProperty("allocateSpec") Map<Interval, Integer> allocateSpec,
      @JsonProperty("baseSequenceName") String baseSequenceName
  )
  {
    this.allocateSpec = allocateSpec;
    this.baseSequenceName = baseSequenceName;
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

  @Override
  public TypeReference<List<SegmentIdentifier>> getReturnTypeReference()
  {
    return new TypeReference<List<SegmentIdentifier>>()
    {
    };
  }

  @Override
  public List<SegmentIdentifier> perform(Task task, TaskActionToolbox toolbox)
  {
    return toolbox.doSynchronized(() -> {
      final List<SegmentIdentifier> allocatedIds = new ArrayList<>();

      for (Entry<Interval, Integer> entry : allocateSpec.entrySet()) {
        final Interval interval = entry.getKey();
        final int numSegmentsToAllocate = entry.getValue();

        for (int i = 0; i < numSegmentsToAllocate; i++) {
          final String sequenceName = StringUtils.format("%s_%s_%d", baseSequenceName, interval, i);
          // TODO: probably doInCriticalSection??
          final Pair<String, Integer> maxVersionAndPartitionId = toolbox.getIndexerMetadataStorageCoordinator().findMaxVersionAndAvailablePartitionId(
              task.getDataSource(),
              sequenceName,
              null,
              interval,
              true
          );

          if (maxVersionAndPartitionId.lhs == null) {
            // TODO: log?
            return Collections.emptyList();
          }

          // This action is always used by overwriting tasks without changing segment granularity, and so all lock
          // requests should be segmentLock.
          final LockResult lockResult = toolbox.getTaskLockbox().tryLock(
              LockGranularity.SEGMENT,
              TaskLockType.EXCLUSIVE,
              task,
              interval,
              maxVersionAndPartitionId.lhs,
              Collections.singletonList(maxVersionAndPartitionId.rhs)
          );

          if (lockResult.isRevoked()) {
            // We had acquired a lock but it was preempted by other locks
            throw new ISE("The lock for interval[%s] is preempted and no longer valid", interval);
          }

          if (lockResult.isOk()) {
            final SegmentIdentifier identifier = toolbox.getIndexerMetadataStorageCoordinator().allocatePendingSegment(
                task.getDataSource(),
                sequenceName,
                null,
                interval,
                maxVersionAndPartitionId.lhs,
                (maxPartitions, objectMapper) -> {
                  if (numSegmentsToAllocate == 1) {
                    return NoneShardSpec.instance();
                  } else {
                    return new HashBasedNumberedShardSpec(maxVersionAndPartitionId.rhs, numSegmentsToAllocate, null, objectMapper);
                  }
                },
                true
            );
            if (identifier != null) {
              allocatedIds.add(identifier);
            } else {
//            final String msg = StringUtils.format(
//                "Could not allocate pending segment for interval[%s],.",
//                interval
//            );
//            if (logOnFail) {
//              log.error(msg);
//            } else {
//              log.debug(msg);
//            }
              return Collections.emptyList();
            }
          } else {
//          final String msg = StringUtils.format(
//              "Could not acquire lock for interval[%s].",
//              interval
//          );
//          if (logOnFail) {
//            log.error(msg);
//          } else {
//            log.debug(msg);
//          }
            return Collections.emptyList();
          }
        }
      }

      return allocatedIds;
    });
  }

  @Override
  public boolean isAudited()
  {
    return false;
  }

  // TODO: toString
}
