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

public class CountMemoryVectorAggregator implements MemoryVectorAggregator
{
  @Override
  public void init(WritableMemory memory, int position)
  {
    memory.putLong(position, 0);
  }

  @Override
  public void aggregate(WritableMemory memory, int position, int startRow, int endRow)
  {
    final int delta = endRow - startRow;
    memory.putLong(position, memory.getLong(position) + delta);
  }

  @Override
  public void aggregate(WritableMemory memory, int numRows, int[] positions, @Nullable int[] rows, int positionOffset)
  {
    for (int i = 0; i < numRows; i++) {
      final int position = positions[i] + positionOffset;
      memory.putLong(position, memory.getLong(position) + 1);
    }
  }

  @Nullable
  @Override
  public Object get(WritableMemory memory, int position)
  {
    return getLong(memory, position);
  }

  @Override
  public long getLong(WritableMemory memory, int position)
  {
    return memory.getLong(position);
  }

  @Override
  public double getDouble(WritableMemory memory, int position)
  {
    return getLong(memory, position);
  }

  @Override
  public float getFloat(WritableMemory memory, int position)
  {
    return getLong(memory, position);
  }

  @Override
  public void close()
  {

  }
}
