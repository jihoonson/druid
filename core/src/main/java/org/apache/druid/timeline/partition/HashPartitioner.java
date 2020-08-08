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

package org.apache.druid.timeline.partition;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import org.apache.druid.data.input.InputRow;
import org.apache.druid.data.input.Rows;

import javax.annotation.Nullable;
import java.util.List;

/**
 * TODO
 */
class HashPartitioner
{
  private final ObjectMapper jsonMapper;
  private final HashPartitionFunction hashPartitionFunction;
  private final List<String> partitionDimensions;
  private final int numBuckets;

  HashPartitioner(
      final ObjectMapper jsonMapper,
      @Nullable final HashPartitionFunction hashPartitionFunction,
      final List<String> partitionDimensions,
      final int numBuckets
  )
  {
    this.jsonMapper = jsonMapper;
    this.hashPartitionFunction = hashPartitionFunction == null
                                 ? HashPartitionFunction.MURMUR3_32_ABS
                                 : hashPartitionFunction;
    this.partitionDimensions = partitionDimensions;
    this.numBuckets = numBuckets;
  }

  ShardSpecLookup createHashLookup(final List<? extends ShardSpec> shardSpecs)
  {
    Preconditions.checkNotNull(hashPartitionFunction, "hashPartitionFunction");
    return (long timestamp, InputRow row) -> {
      int index = hashPartitionFunction.hash(
          getSerializedGroupKey(timestamp, row),
          numBuckets
      );
      return shardSpecs.get(index);
    };
  }

  byte[] getSerializedGroupKey(final long timestamp, final InputRow inputRow)
  {
    return HashBasedNumberedShardSpec.serializeGroupKey(jsonMapper, getGroupKey(timestamp, inputRow));
  }

  /**
   * This method calculates the hash based on whether {@param partitionDimensions} is null or not.
   * If yes, then both {@param timestamp} and dimension columns in {@param inputRow} are used {@link Rows#toGroupKey}
   * Or else, columns in {@param partitionDimensions} are used
   *
   * @param timestamp should be bucketed with query granularity
   * @param inputRow  row from input data
   *
   * @return hash value
   */
  List<Object> getGroupKey(final long timestamp, final InputRow inputRow)
  {
    if (partitionDimensions.isEmpty()) {
      return Rows.toGroupKey(timestamp, inputRow);
    } else {
      return Lists.transform(partitionDimensions, inputRow::getDimension);
    }
  }
}
