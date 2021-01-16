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

import com.google.common.base.Preconditions;
import com.google.common.primitives.Ints;
import org.apache.datasketches.memory.WritableMemory;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.segment.column.ValueType;
import org.apache.druid.segment.vector.VectorColumnSelectorFactory;

import javax.annotation.Nullable;
import java.io.Closeable;
import java.util.Arrays;
import java.util.List;

public class MemoryVectorAggregators implements Closeable
{
  private static final Logger LOG = new Logger(MemoryVectorAggregators.class);

  private final List<MemoryVectorAggregator> aggregators;
  private final List<AggregatorFactory> factories;
  private final int[] aggregatorPositions;
  private final int spaceNeeded;

  public static MemoryVectorAggregators factorizeVector(
      final VectorColumnSelectorFactory columnSelectorFactory,
      final List<AggregatorFactory> aggregatorFactories
  )
  {
    final MemoryVectorAggregator[] aggregators = new MemoryVectorAggregator[aggregatorFactories.size()];
    for (int i = 0; i < aggregatorFactories.size(); i++) {
      final AggregatorFactory aggregatorFactory = aggregatorFactories.get(i);
      aggregators[i] = aggregatorFactory.factorizeMemoryVector(columnSelectorFactory);
    }

    return new MemoryVectorAggregators(aggregatorFactories, Arrays.asList(aggregators));
  }

  private MemoryVectorAggregators(List<AggregatorFactory> factories, List<MemoryVectorAggregator> aggregators)
  {
    Preconditions.checkArgument(factories.size() == aggregators.size());
    this.factories = factories;
    this.aggregators = aggregators;
    this.aggregatorPositions = new int[aggregators.size()];

    long nextPosition = 0;
    for (int i = 0; i < aggregators.size(); i++) {
      final AggregatorFactory aggregatorFactory = factories.get(i);
      aggregatorPositions[i] = Ints.checkedCast(nextPosition);
      nextPosition += aggregatorFactory.getMaxIntermediateSizeWithNulls();
    }

    this.spaceNeeded = Ints.checkedCast(nextPosition);
  }

  public int spaceNeeded()
  {
    return spaceNeeded;
  }

  public List<AggregatorFactory> factories()
  {
    return factories;
  }

  /**
   * Return the individual positions of each aggregator within a hypothetical buffer of size {@link #spaceNeeded()}.
   */
  public int[] aggregatorPositions()
  {
    return aggregatorPositions;
  }

  /**
   * Return the number of aggregators in this object.
   */
  public int size()
  {
    return aggregators.size();
  }

  /**
   * Initialize all aggregators.
   *
   * @param buf      aggregation buffer
   * @param position position in buffer where our block of size {@link #spaceNeeded()} starts
   */
  public void init(final WritableMemory buf, final int position)
  {
    for (int i = 0; i < aggregators.size(); i++) {
      aggregators.get(i).init(buf, position + aggregatorPositions[i]);
    }
  }

  public void aggregateVector(
      final WritableMemory buf,
      final int numRows,
      final int[] positions,
      @Nullable final int[] rows
  )
  {
    for (int i = 0; i < aggregators.size(); i++) {
      final MemoryVectorAggregator adapter = aggregators.get(i);
      adapter.aggregate(buf, numRows, positions, rows, aggregatorPositions[i]);
    }
  }

  @Nullable
  public Object get(final WritableMemory buf, final int position, final int aggregatorNumber)
  {
    return aggregators.get(aggregatorNumber).get(buf, position + aggregatorPositions[aggregatorNumber]);
  }

  public double getDouble(final WritableMemory buf, final int position, final int aggregatorNumber)
  {
    return aggregators.get(aggregatorNumber).getDouble(buf, position + aggregatorPositions[aggregatorNumber]);
  }

  public float getFloat(final WritableMemory buf, final int position, final int aggregatorNumber)
  {
    return aggregators.get(aggregatorNumber).getFloat(buf, position + aggregatorPositions[aggregatorNumber]);
  }

  public long getLong(final WritableMemory buf, final int position, final int aggregatorNumber)
  {
    return aggregators.get(aggregatorNumber).getLong(buf, position + aggregatorPositions[aggregatorNumber]);
  }

  public ValueType getType(int i)
  {
    return factories.get(i).getType();
  }

  @Override
  public void close()
  {
    for (int i = 0; i < aggregators.size(); i++) {
      try {
        aggregators.get(i).close();
      }
      catch (Exception e) {
        LOG.warn(e, "Could not close aggregator [%s], skipping.", factories.get(i).getName());
      }
    }
  }
}
