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

package org.apache.druid.query.aggregation;

import org.apache.datasketches.memory.WritableMemory;

import javax.annotation.Nullable;

/**
 * {@link org.apache.druid.query.groupby.epinephelinae.FixedSizeHashVectorGrouper} splits
 * a given {@link org.apache.datasketches.memory.Memory} into two arenas, one for used flags
 * and another for hash table. As a result, to use {@link VectorAggregator} in it, the grouper
 * needs to compute the proper offset to read valid values from the backed {@link java.nio.ByteBuffer}.
 * This just makes the implementation complicated, and is why this class is added.
 */
public interface MemoryVectorAggregator
{
  void init(WritableMemory memory, int position);

  void aggregate(WritableMemory memory, int position, int startRow, int endRow);

  void aggregate(WritableMemory memory, int numRows, int[] positions, @Nullable int[] rows, int positionOffset);

  @Nullable
  Object get(WritableMemory memory, int position);

  long getLong(WritableMemory memory, int position);

  double getDouble(WritableMemory memory, int position);

  float getFloat(WritableMemory memory, int position);

  void close();
}
