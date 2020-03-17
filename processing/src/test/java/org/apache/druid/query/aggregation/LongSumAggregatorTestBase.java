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

public abstract class LongSumAggregatorTestBase extends AggregatorTestBase
{
  static final Interval INTERVAL = Intervals.of("2020-01-01/P1D");
  static final Granularity SEGMENT_GRANULARITY = Granularities.HOUR;
  static final int NUM_SEGMENT_PER_TIME_PARTITION = 2;
  static final int NUM_ROWS_PER_SEGMENT = 10;
  static final double NULL_RATIO = 0.2;
  static final boolean ROLL_UP = true; // another class?
  static final boolean PERSIST = true; // paramterized

  final LongSumAggregatorFactory aggregatorFactory = new LongSumAggregatorFactory(
      TestColumn.LONG_COLUMN.getName(),
      TestColumn.LONG_COLUMN.getName()
  );

  LongSumAggregatorTestBase()
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
}
