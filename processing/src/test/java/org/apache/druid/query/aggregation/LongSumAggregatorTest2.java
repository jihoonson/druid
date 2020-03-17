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

import org.apache.druid.segment.selector.settable.SettableLongColumnValueSelector;
import org.junit.Assert;
import org.junit.Test;

import java.util.function.Function;

public class LongSumAggregatorTest2 extends LongSumAggregatorTestBase
{
  @Test
  public void testAggregate()
  {
    Assert.assertEquals(
        compute(
            TestColumn.LONG_COLUMN,
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
  public void testGet()
  {
    SettableLongColumnValueSelector columnValueSelector = new SettableLongColumnValueSelector();
    SettableColumnSelectorFactory columnSelectorFactory = new SettableColumnSelectorFactory(columnValueSelector);
    Aggregator aggregator = aggregatorFactory.factorize(columnSelectorFactory);
    columnValueSelector.setValue(1);
    aggregator.aggregate();
    Assert.assertEquals(1L, aggregator.get());
    Assert.assertEquals(1L, aggregator.getLong());
    Assert.assertEquals(1., aggregator.getDouble(), 0);
    Assert.assertEquals(1., aggregator.getFloat(), 0);
  }

  @Test
  public void testGetNull()
  {
    SettableLongColumnValueSelector columnValueSelector = new SettableLongColumnValueSelector();
    SettableColumnSelectorFactory columnSelectorFactory = new SettableColumnSelectorFactory(columnValueSelector);
    Aggregator aggregator = aggregatorFactory.factorize(columnSelectorFactory);
    columnValueSelector.setNull(true);
    aggregator.aggregate();
    Assert.assertTrue(aggregator.isNull());
    Assert.assertNull(aggregator.get());

    // TODO: check exceptions
    aggregator.getLong();
    Assert.assertEquals(1., aggregator.getDouble(), 0);
    Assert.assertEquals(1., aggregator.getFloat(), 0);
  }
}
