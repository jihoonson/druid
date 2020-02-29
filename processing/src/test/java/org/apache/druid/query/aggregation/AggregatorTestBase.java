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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import org.apache.druid.data.gen.TestColumnSchema;
import org.apache.druid.data.gen.TestDataGenerator;
import org.apache.druid.data.input.impl.DimensionSchema.ValueType;
import org.apache.druid.java.util.common.granularity.Granularity;
import org.apache.druid.segment.ColumnValueSelector;
import org.apache.druid.segment.DimensionSelector;
import org.apache.druid.segment.incremental.IncrementalIndex;
import org.apache.druid.segment.incremental.IncrementalIndexSchema;
import org.apache.druid.segment.incremental.OnheapIncrementalIndex;
import org.joda.time.Interval;

import java.util.Arrays;
import java.util.List;

public class AggregatorTestBase
{
  // float, double, long, single-valued string, multi-valued string w/o nulls
  // custom column value selector for complex type

  // get as dimension selector
  // get as column value selector

  public static final String FLOAT_COLUMN = "floatColumn";
  public static final String DOUBLE_COLUMN = "doubleColumn";
  public static final String LONG_COLUMN = "longColumn";
  public static final String SINGLE_VALUE_STRING_COLUMN = "singleValueStringColumn";
  public static final String MULTI_VALUE_STRING_COLUMN = "multiValueStringColumn";


  public AggregatorTestBase(Interval interval, Granularity segmentGranularity, int numRows, double nullRatio)
  {
    final List<TestColumnSchema> columnSchemas = ImmutableList.of(
        TestColumnSchema.makeSequential(
            FLOAT_COLUMN,
            ValueType.FLOAT,
            false,
            1,
            nullRatio,
            -(numRows / 2),
            numRows / 2
        ),
        TestColumnSchema.makeSequential(
            DOUBLE_COLUMN,
            ValueType.DOUBLE,
            false,
            1,
            nullRatio,
            -(numRows / 2),
            numRows / 2
        ),
        TestColumnSchema.makeSequential(
            LONG_COLUMN,
            ValueType.LONG,
            false,
            1,
            nullRatio,
            -(numRows / 2),
            numRows / 2
        ),
        TestColumnSchema.makeEnumeratedSequential(
            SINGLE_VALUE_STRING_COLUMN,
            ValueType.STRING,
            false,
            1,
            nullRatio,
            Arrays.asList("Apple", "Orange", "Xylophone", "Corundum", null)
        ),
        TestColumnSchema.makeEnumeratedSequential(
            MULTI_VALUE_STRING_COLUMN,
            ValueType.STRING,
            false,
            4,
            nullRatio,
            Arrays.asList("Apple", "Orange", "Xylophone", "Corundum", null)
        )
    );

    final int numTimePartitions = Iterables.size(segmentGranularity.getIterable(interval));
    final int numRowsPerTimePartition = numRows / numTimePartitions;
    final TestDataGenerator dataGenerator = new TestDataGenerator(columnSchemas, 0, interval, numRowsPerTimePartition);

    IncrementalIndex<Aggregator> incrementalIndex = new IncrementalIndex.Builder()
        .setIndexSchema(
            new IncrementalIndexSchema.Builder()
            .withDimensionsSpec(columnSchemas)
        )
        .buildOnheap();
  }

  public DimensionSelector createDimensionSelector(String columnName)
  {

  }

  public ColumnValueSelector<?> createColumnValueSelector(String columnName)
  {

  }
}
