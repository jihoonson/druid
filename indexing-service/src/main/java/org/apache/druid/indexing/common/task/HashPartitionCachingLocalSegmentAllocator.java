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

import com.google.common.collect.Maps;
import org.apache.druid.indexer.partitions.HashPartitionAnalysis;
import org.apache.druid.indexer.partitions.HashedPartitionsSpec;
import org.apache.druid.indexing.common.TaskToolbox;
import org.apache.druid.segment.realtime.appenderator.SegmentIdWithShardSpec;
import org.apache.druid.timeline.partition.HashBasedNumberedShardSpec;
import org.joda.time.Interval;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * Allocates all necessary hash-partitioned segments locally at the beginning and reuses them.
 */
public class HashPartitionCachingLocalSegmentAllocator extends CachingLocalSegmentAllocator
{
  public HashPartitionCachingLocalSegmentAllocator(
      TaskToolbox toolbox,
      String taskId,
      String supervisorTaskId,
      String dataSource,
      HashedPartitionsSpec partitionsSpec,
      HashPartitionAnalysis partitionAnalysis
  ) throws IOException
  {
    super(
        toolbox,
        taskId,
        supervisorTaskId,
        versionFinder -> getIntervalToSegmentIds(toolbox, dataSource, partitionsSpec, partitionAnalysis, versionFinder)
    );
  }

  private static Map<Interval, List<SegmentIdWithShardSpec>> getIntervalToSegmentIds(
      TaskToolbox toolbox,
      String dataSource,
      HashedPartitionsSpec partitionsSpec,
      HashPartitionAnalysis partitionAnalysis,
      Function<Interval, String> versionFinder
  )
  {
    final Map<Interval, List<SegmentIdWithShardSpec>> intervalToSegmentIds =
        Maps.newHashMapWithExpectedSize(partitionAnalysis.numPrimaryPartitions());

    partitionAnalysis.forEach((interval, numBuckets) -> {
      intervalToSegmentIds.put(
          interval,
          IntStream.range(0, numBuckets)
                   .mapToObj(i -> {
                     final HashBasedNumberedShardSpec shardSpec = new HashBasedNumberedShardSpec(
                         i,
                         numBuckets,
                         partitionsSpec.getPartitionDimensions(),
                         toolbox.getJsonMapper()
                     );
                     return new SegmentIdWithShardSpec(
                         dataSource,
                         interval,
                         versionFinder.apply(interval),
                         shardSpec
                     );
                   })
                   .collect(Collectors.toList())
      );
    });

    return intervalToSegmentIds;
  }
}
