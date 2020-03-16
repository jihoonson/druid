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

import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.java.util.common.granularity.Granularity;
import org.joda.time.Interval;
import org.junit.Assert;
import org.junit.Test;

import java.util.function.Function;

public class LongSumAggregatorTest2 extends AggregatorTestBase
{
  private static final Interval INTERVAL = Intervals.of("2020-01-01/P1D");
  private static final Granularity SEGMENT_GRANULARITY = Granularities.HOUR;
  private static final int NUM_SEGMENT_PER_TIME_PARTITION = 2;
  private static final int NUM_ROWS_PER_SEGMENT = 10;
  private static final double NULL_RATIO = 0.2;
  private static final boolean ROLL_UP = true; // another class?
  private static final boolean PERSIST = true; // paramterized

  public LongSumAggregatorTest2()
  {
    super(
        INTERVAL,
        SEGMENT_GRANULARITY,
        NUM_SEGMENT_PER_TIME_PARTITION,
        NUM_ROWS_PER_SEGMENT,
        NULL_RATIO,
        null,
        ROLL_UP,
        PERSIST
    );
  }

  @Test
  public void testAggregate()
  {
    final LongSumAggregatorFactory aggregatorFactory = new LongSumAggregatorFactory(
        TestColumn.LONG_COLUMN.getName(),
        TestColumn.LONG_COLUMN.getName()
    );
    Assert.assertEquals(
        compute(
            TestColumn.LONG_COLUMN.getName(),
            INTERVAL,
            Function.identity(),
            Long::sum,
            0L,
            0L
        ),
        aggregate(aggregatorFactory, INTERVAL)
    );
  }

  @Test
  public void testBufferAggregate()
  {
    final LongSumAggregatorFactory aggregatorFactory = new LongSumAggregatorFactory(
        TestColumn.LONG_COLUMN.getName(),
        TestColumn.LONG_COLUMN.getName()
    );
    Assert.assertEquals(
        compute(
            TestColumn.LONG_COLUMN.getName(),
            INTERVAL,
            Function.identity(),
            Long::sum,
            0L,
            0L
        ),
        bufferAggregate(aggregatorFactory, INTERVAL)
    );
  }
}
