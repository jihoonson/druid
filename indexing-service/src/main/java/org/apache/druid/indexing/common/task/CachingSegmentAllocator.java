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

import org.apache.druid.data.input.InputRow;
import org.apache.druid.indexing.common.TaskToolbox;
import org.apache.druid.indexing.common.task.IndexTask.ShardSpecs;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.segment.realtime.appenderator.SegmentIdentifier;
import org.apache.druid.timeline.partition.NumberedShardSpec;
import org.apache.druid.timeline.partition.ShardSpec;
import org.joda.time.Interval;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

// TODO: caching??
public abstract class CachingSegmentAllocator implements IndexTaskSegmentAllocator
{
  private final TaskToolbox toolbox;
  private final String taskId;
  private final Map<Interval, Integer> intervalToNumShards;
  @Nullable
  private final List<String> partitionDimensions;
  private final boolean isExtendableShardSpecs;
  private final ShardSpecs shardSpecs;

  // sequenceName -> segmentId
  private final Map<String, SegmentIdentifier> sequenceNameToSegmentId;

  public CachingSegmentAllocator(
      TaskToolbox toolbox,
      String taskId,
      Map<Interval, Integer> intervalToNumShards,
      Map<Interval, Set<Integer>> inputPartitionIds,
      @Nullable List<String> partitionDimensions,
      boolean isExtendableShardSpecs
  ) throws IOException
  {
    this.toolbox = toolbox;
    this.taskId = taskId;
    this.intervalToNumShards = intervalToNumShards;
    this.partitionDimensions = partitionDimensions;
    this.isExtendableShardSpecs = isExtendableShardSpecs;
    this.sequenceNameToSegmentId = new HashMap<>();

    final Map<Interval, List<SegmentIdentifier>> intervalToIds = getIntervalToSegmentIds(inputPartitionIds);
    final Map<Interval, List<ShardSpec>> shardSpecMap = new HashMap<>();

    for (Map.Entry<Interval, List<SegmentIdentifier>> entry : intervalToIds.entrySet()) {
      final Interval interval = entry.getKey();
      final List<SegmentIdentifier> idsPerInterval = intervalToIds.get(interval);
      final int numTotalPartitions = idsPerInterval.size();

      for (SegmentIdentifier segmentIdentifier : idsPerInterval) {
        shardSpecMap.computeIfAbsent(interval, k -> new ArrayList<>()).add(segmentIdentifier.getShardSpec());
        // The shardSpecs for partitinoing and publishing can be different if isExtendableShardSpecs = true.
        sequenceNameToSegmentId.put(
            getSequenceName(interval, segmentIdentifier.getShardSpec()),
            segmentIdentifier.withShardSpec(
                makeExtendableIfNecessary(segmentIdentifier.getShardSpec(), numTotalPartitions)
            )
        );
      }
    }
    shardSpecs = new ShardSpecs(shardSpecMap);
  }

  private ShardSpec makeExtendableIfNecessary(ShardSpec shardSpec, int numTotalPartitions)
  {
    if (isExtendableShardSpecs) {
      return new NumberedShardSpec(shardSpec.getPartitionNum(), numTotalPartitions);
    } else {
      return shardSpec;
    }
  }

  abstract Map<Interval, List<SegmentIdentifier>> getIntervalToSegmentIds(Map<Interval, Set<Integer>> inputPartitionIds)
      throws IOException;

  TaskToolbox getToolbox()
  {
    return toolbox;
  }

  String getTaskId()
  {
    return taskId;
  }

  Map<Interval, Integer> getIntervalToNumShards()
  {
    return intervalToNumShards;
  }

  @Nullable
  List<String> getPartitionDimensions()
  {
    return partitionDimensions;
  }

  @Override
  public SegmentIdentifier allocate(
      InputRow row,
      String sequenceName,
      String previousSegmentId,
      boolean skipSegmentLineageCheck
  )
  {
    return sequenceNameToSegmentId.get(sequenceName);
  }

  @Override
  public String getSequenceName(Interval interval, InputRow inputRow)
  {
    return getSequenceName(interval, shardSpecs.getShardSpec(interval, inputRow));
  }

  /**
   * Create a sequence name from the given shardSpec and interval. The shardSpec must be the original one before calling
   * {@link #makeExtendableIfNecessary(ShardSpec, int)} to apply the proper partitioning.
   *
   * See {@link org.apache.druid.timeline.partition.HashBasedNumberedShardSpec} as an example of partitioning.
   */
  private String getSequenceName(Interval interval, ShardSpec shardSpec)
  {
    return StringUtils.format("%s_%s_%d", taskId, interval, shardSpec.getPartitionNum());
  }
}
