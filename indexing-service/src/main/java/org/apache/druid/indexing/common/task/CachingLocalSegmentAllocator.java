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

import org.apache.druid.indexing.common.TaskLock;
import org.apache.druid.indexing.common.TaskToolbox;
import org.apache.druid.indexing.common.actions.LockListAction;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.segment.realtime.appenderator.SegmentIdentifier;
import org.apache.druid.timeline.partition.HashBasedNumberedShardSpec;
import org.apache.druid.timeline.partition.NoneShardSpec;
import org.apache.druid.timeline.partition.ShardSpec;
import org.joda.time.Interval;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class CachingLocalSegmentAllocator extends CachingSegmentAllocator
{
  private final String dataSource;

  private final Map<Interval, String> intervalToVersion;

  public CachingLocalSegmentAllocator(
      TaskToolbox toolbox,
      String taskId,
      String dataSource,
      Map<Interval, Integer> intervalToNumShards,
      @Nullable List<String> partitionDimensions,
      boolean isExtendableShardSpecs
  ) throws IOException
  {
    // This segment allocator doesn't need inputPartitionIds because the newly created segments don't have to store
    // overshadowingSegments
    super(toolbox, taskId, intervalToNumShards, Collections.emptyMap(), partitionDimensions, isExtendableShardSpecs);
    this.dataSource = dataSource;

    intervalToVersion = toolbox.getTaskActionClient()
                               .submit(new LockListAction())
                               .stream()
                               .collect(Collectors.toMap(TaskLock::getInterval, TaskLock::getVersion));
  }

  @Override
  Map<Interval, List<SegmentIdentifier>> getIntervalToSegmentIds(Map<Interval, Set<Integer>> inputPartitionIds)
  {
    final Map<Interval, List<SegmentIdentifier>> intervalToSegmentIds = new HashMap<>(getIntervalToNumShards().size());
    for (Entry<Interval, Integer> intervalToNumShard : getIntervalToNumShards().entrySet()) {
      final Interval interval = intervalToNumShard.getKey();
      final int numSegments = intervalToNumShard.getValue();
      intervalToSegmentIds.put(
          interval,
          IntStream.range(0, numSegments)
                   .mapToObj(i -> new SegmentIdentifier(
                       dataSource,
                       interval,
                       findVersion(interval), createShardSpec(i, numSegments))
                   )
                   .collect(Collectors.toList())
      );
    }
    return intervalToSegmentIds;
  }

  private ShardSpec createShardSpec(int partitionId, int totalNumPartitions)
  {
    if (totalNumPartitions == 1) {
      return NoneShardSpec.instance();
    } else {
      return new HashBasedNumberedShardSpec(
          partitionId,
          totalNumPartitions,
          getPartitionDimensions(),
          getToolbox().getObjectMapper()
      );
    }
  }

  private String findVersion(Interval interval)
  {
    return intervalToVersion.entrySet().stream()
                            .filter(entry -> entry.getKey().contains(interval))
                            .map(Entry::getValue)
                            .findFirst()
                            .orElseThrow(() -> new ISE("Cannot find a version for interval[%s]", interval));
  }
}
