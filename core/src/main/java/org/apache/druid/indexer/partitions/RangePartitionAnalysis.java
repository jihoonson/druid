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

package org.apache.druid.indexer.partitions;

import org.joda.time.Interval;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.function.BiConsumer;

public class RangePartitionAnalysis implements PartitionAnalysis<PartitionBoundaries, SingleDimensionPartitionsSpec>
{
  private final Map<Interval, PartitionBoundaries> intervalToPartitionBoundaries = new HashMap<>();
  private final SingleDimensionPartitionsSpec partitionsSpec;

  public RangePartitionAnalysis(SingleDimensionPartitionsSpec partitionsSpec)
  {
    this.partitionsSpec = partitionsSpec;
  }

  @Override
  public SingleDimensionPartitionsSpec getPartitionsSpec()
  {
    return partitionsSpec;
  }

  @Override
  public void updateBucket(Interval interval, PartitionBoundaries bucketAnalysis)
  {
    intervalToPartitionBoundaries.put(interval, bucketAnalysis);
  }

  @Override
  public PartitionBoundaries getBucketAnalysis(Interval interval)
  {
    return intervalToPartitionBoundaries.get(interval);
  }

  @Override
  public Set<Interval> getAllIntervalsToIndex()
  {
    return Collections.unmodifiableSet(intervalToPartitionBoundaries.keySet());
  }

  public void forEach(BiConsumer<Interval, PartitionBoundaries> consumer)
  {
    intervalToPartitionBoundaries.forEach(consumer);
  }

  public int size()
  {
    return intervalToPartitionBoundaries.size();
  }
}
