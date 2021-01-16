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
import org.apache.druid.segment.vector.VectorValueSelector;

import javax.annotation.Nullable;

public class DoubleSumMemoryVectorAggregator implements MemoryVectorAggregator
{
  private final VectorValueSelector selector;

  public DoubleSumMemoryVectorAggregator(VectorValueSelector selector)
  {
    this.selector = selector;
  }

  @Override
  public void init(WritableMemory memory, int position)
  {
    memory.putDouble(position, 0);
  }

  @Override
  public void aggregate(WritableMemory memory, int position, int startRow, int endRow)
  {
    final double[] vector = selector.getDoubleVector();

    double sum = 0;
    for (int i = startRow; i < endRow; i++) {
      sum += vector[i];
    }

    memory.putDouble(position, memory.getDouble(position) + sum);
  }

  @Override
  public void aggregate(WritableMemory memory, int numRows, int[] positions, @Nullable int[] rows, int positionOffset)
  {
    final double[] vector = selector.getDoubleVector();

    for (int i = 0; i < numRows; i++) {
      final int position = positions[i] + positionOffset;
      memory.putDouble(position, memory.getDouble(position) + vector[rows != null ? rows[i] : i]);
    }
  }

  @Nullable
  @Override
  public Object get(WritableMemory memory, int position)
  {
    return getDouble(memory, position);
  }

  @Override
  public long getLong(WritableMemory memory, int position)
  {
    return (long) getDouble(memory, position);
  }

  @Override
  public double getDouble(WritableMemory memory, int position)
  {
    return memory.getDouble(position);
  }

  @Override
  public float getFloat(WritableMemory memory, int position)
  {
    return (float) getDouble(memory, position);
  }

  @Override
  public void close()
  {

  }
}
