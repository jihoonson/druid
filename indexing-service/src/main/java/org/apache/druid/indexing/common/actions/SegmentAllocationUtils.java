///*
// * Licensed to the Apache Software Foundation (ASF) under one
// * or more contributor license agreements.  See the NOTICE file
// * distributed with this work for additional information
// * regarding copyright ownership.  The ASF licenses this file
// * to you under the Apache License, Version 2.0 (the
// * "License"); you may not use this file except in compliance
// * with the License.  You may obtain a copy of the License at
// *
// *   http://www.apache.org/licenses/LICENSE-2.0
// *
// * Unless required by applicable law or agreed to in writing,
// * software distributed under the License is distributed on an
// * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// * KIND, either express or implied.  See the License for the
// * specific language governing permissions and limitations
// * under the License.
// */
//package org.apache.druid.indexing.common.actions;
//
//import org.apache.druid.indexing.common.TaskLockType;
//import org.apache.druid.indexing.common.task.Task;
//import org.apache.druid.indexing.overlord.IndexerMetadataStorageCoordinator;
//import org.apache.druid.indexing.overlord.IndexerMetadataStorageCoordinator.SegmentAllocationContext;
//import org.apache.druid.indexing.overlord.LockResult;
//import org.apache.druid.indexing.overlord.TaskLockbox;
//import org.apache.druid.indexing.overlord.TaskLockbox.SegmentLockRequest;
//import org.apache.druid.java.util.common.ISE;
//import org.apache.druid.java.util.common.StringUtils;
//import org.apache.druid.java.util.common.logger.Logger;
//import org.apache.druid.segment.realtime.appenderator.SegmentIdentifier;
//import org.apache.druid.timeline.partition.ShardSpec;
//import org.joda.time.Interval;
//
//import java.util.ArrayList;
//import java.util.Collections;
//import java.util.List;
//import java.util.Set;
//import java.util.function.Function;
//import java.util.function.Supplier;
//import java.util.stream.Collectors;
//
//// TODO: maybe move to taskLockbox???
//final class SegmentAllocationUtils
//{
//  private static final Logger log = new Logger(SegmentAllocationUtils.class);
//  private static final long TIMEOUT = 1000; // TODO: probably configurable
//
//  static List<SegmentIdentifier> lockAndAllocateSegments(
//      TaskActionToolbox toolbox,
//      Task task,
//      Supplier<String> sequenceNameSupplier,
//      String previousSegmentId,
//      Interval interval,
//      boolean skipSegmentLineageCheck,
//      Function<SegmentAllocationContext, ShardSpec> shardSpecGenerateFn,
//      int numSegmentsToAllocate,
//      boolean logOnFail
//  ) throws InterruptedException
//  {
//    final TaskLockbox lockbox = toolbox.getTaskLockbox();
//    final IndexerMetadataStorageCoordinator metadataStorageCoordinator = toolbox.getIndexerMetadataStorageCoordinator();
//    final String dataSource = task.getDataSource();
//    final List<SegmentIdentifier> allocatedIds = new ArrayList<>(numSegmentsToAllocate);
//
//    // Get a lock for the entire interval
//    final LockResult timeChunkLockResult = lockbox.getTimeChunkLock(
//        TaskLockType.FULLY_EXCLUSIVE,
//        task,
//        interval,
//        TIMEOUT
//    );
//
//    checkLockIsRovked(timeChunkLockResult, interval);
//
//    if (timeChunkLockResult.isOk()) {
////      final Pair<String, Integer> maxVersionAndPartitionId =  metadataStorageCoordinator
////          .findMaxVersionAndAvailablePartitionId(
////              dataSource,
////              ,
////              previousSegmentId,
////              interval,
////              skipSegmentLineageCheck
////          );
////
////      final int startPartitionId = maxVersionAndPartitionId.rhs;
////      final String version = maxVersionAndPartitionId.lhs;
////      final Set<Integer> partitionIdsToAllocate = IntStream
////          .range(startPartitionId, startPartitionId + numSegmentsToAllocate)
////          .boxed()
////          .collect(Collectors.toSet());
//
//      final String version = timeChunkLockResult.getTaskLock().getVersion();
//
//      for (int i = 0; i < numSegmentsToAllocate; i++) {
//        final String sequenceName = sequenceNameSupplier.get();
//        final SegmentIdentifier identifier = metadataStorageCoordinator.allocatePendingSegment(
//            dataSource,
//            sequenceName,
//            previousSegmentId,
//            interval,
//            version,
//            shardSpecGenerateFn,
//            skipSegmentLineageCheck
//        );
//        if (identifier != null) {
//          allocatedIds.add(identifier);
//        } else {
//          final String msg = StringUtils.format(
//              "Could not allocate a pending segment for task[%s] to dataSource[%s] and interval[%s] with prevSegmentId[%s], version[%s], and skipSegmentLineageCheck[%s]",
//              task.getId(),
//              dataSource,
//              interval,
//              previousSegmentId,
//              version,
//              skipSegmentLineageCheck
//          );
//          if (logOnFail) {
//            log.error(msg);
//          } else {
//            log.debug(msg);
//          }
//          // TODO: cleanup pendingSegment?
//          return Collections.emptyList();
//        }
//      }
//
//      final Set<Integer> allocatedPartitionIds = allocatedIds.stream()
//                                                             .map(id -> id.getShardSpec().getPartitionNum())
//                                                             .collect(Collectors.toSet());
//
//      final LockResult reduceResult = lockbox.reduceLock(
//          task,
//          new SegmentLockRequest(
//              TaskLockType.EXCLUSIVE,
//              task.getGroupId(),
//              task.getDataSource(),
//              interval,
//              allocatedPartitionIds,
//              version,
//              task.getPriority(),
//              false
//          )
//      );
//
//      checkLockIsRovked(reduceResult, interval);
//
//      if (reduceResult.isOk()) {
//        return allocatedIds;
//      } else {
//        final String msg = StringUtils.format(
//            "Could not acquire a proper lock for interval[%s] and partitionIds[%s] for task[%s] with version[%s] and priority[%d]",
//            interval,
//            allocatedPartitionIds,
//            task.getId(),
//            version,
//            task.getPriority()
//        );
//        if (logOnFail) {
//          log.error(msg);
//        } else {
//          log.debug(msg);
//        }
//        return Collections.emptyList();
//      }
//    }
//
//    return allocatedIds;
//  }
//
//  private static void checkLockIsRovked(LockResult lockResult, Interval interval)
//  {
//    if (lockResult.isRevoked()) {
//      // We had acquired a lock but it was preempted by other locks
//      throw new ISE("The lock for interval[%s] is preempted and no longer valid", interval);
//    }
//  }
//
//  private SegmentAllocationUtils() {}
//}
