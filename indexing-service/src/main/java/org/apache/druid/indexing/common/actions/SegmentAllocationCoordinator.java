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

import com.google.inject.Inject;
import org.apache.druid.indexing.common.LockGranularity;
import org.apache.druid.indexing.common.TaskLockType;
import org.apache.druid.indexing.common.task.Task;
import org.apache.druid.indexing.overlord.IndexerMetadataStorageCoordinator;
import org.apache.druid.indexing.overlord.LockResult;
import org.apache.druid.indexing.overlord.TaskLockbox;
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
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

// TODO: rename??? or remove???
public class SegmentAllocationCoordinator
{
  private final TaskLockbox lockbox;
  private final IndexerMetadataStorageCoordinator metadataStorageCoordinator;
  // dataSource -> list of time chunks
  private final ConcurrentMap<String, ConcurrentMap<Interval, AtomicReference<String>>> timeChunks = new ConcurrentHashMap<>();

  @Inject
  public SegmentAllocationCoordinator(
      TaskLockbox lockbox,
      IndexerMetadataStorageCoordinator metadataStorageCoordinator
  )
  {
    this.lockbox = lockbox;
    this.metadataStorageCoordinator = metadataStorageCoordinator;
  }

  public List<SegmentIdentifier> lockAndAllocateSegment(
      Task task,
      String baseSequenceName,
      List<SegmentAllocateSpec> allocateSpecs
  )
  {
    final String dataSource = task.getDataSource();

    final List<SegmentIdentifier> allocatedIds = new ArrayList<>(allocateSpecs.size());
    for (SegmentAllocateSpec spec : allocateSpecs) {
      final Interval interval = spec.interval;
      for (int i = 0; i < spec.numSegmentsToAllocate; i++) {
        final String sequenceName = StringUtils.format("%s_%s_%d", baseSequenceName, interval, i);
        final Pair<String, Integer> maxVersionAndPartitionId = metadataStorageCoordinator
            .findMaxVersionAndAvailablePartitionId(
                dataSource,
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
        final LockResult lockResult = lockbox.tryLock(
            LockGranularity.SEGMENT,
            TaskLockType.EXCLUSIVE,
            task,
            interval,
            maxVersionAndPartitionId.lhs,
            Collections.singleton(maxVersionAndPartitionId.rhs)
        );

        if (lockResult.isRevoked()) {
          // We had acquired a lock but it was preempted by other locks
          throw new ISE("The lock for interval[%s] is preempted and no longer valid", interval);
        }

        if (lockResult.isOk()) {
          final SegmentIdentifier identifier = metadataStorageCoordinator.allocatePendingSegment(
              dataSource,
              sequenceName,
              null,
              interval,
              maxVersionAndPartitionId.lhs,
              (maxPartitions, objectMapper) -> {
                if (spec.numSegmentsToAllocate == 1) {
                  return NoneShardSpec.instance();
                } else {
                  // TODO: support other types
                  return new HashBasedNumberedShardSpec(maxVersionAndPartitionId.rhs, spec.numSegmentsToAllocate, null, objectMapper);
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
  }

  public static class SegmentAllocateSpec
  {
    private final Interval interval;
    private final int numSegmentsToAllocate;

    public SegmentAllocateSpec(Interval interval, int numSegmentsToAllocate)
    {
      this.interval = interval;
      this.numSegmentsToAllocate = numSegmentsToAllocate;
    }
  }
}
