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

import org.apache.druid.query.aggregation.AggregateTestBase.TestColumn;
import org.apache.druid.segment.ListBasedSingleColumnCursor;
import org.apache.druid.testing.InitializedNullHandlingTest;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import javax.annotation.Nullable;
import java.util.Collections;

public class DirectColumnAccessLongSumAggregatorTest extends InitializedNullHandlingTest
{
  @Rule
  public ExpectedException expectedException = ExpectedException.none();

  private final LongSumAggregatorFactory aggregatorFactory = new LongSumAggregatorFactory(
      TestColumn.LONG_COLUMN.getName(),
      TestColumn.LONG_COLUMN.getName()
  );

  @Test
  public void testGet()
  {
    try (Aggregator aggregator = createAggregatorForValue(1L)) {
      aggregator.aggregate();
      Assert.assertEquals(1L, aggregator.get());
      Assert.assertEquals(1L, aggregator.getLong());
      Assert.assertEquals(1., aggregator.getDouble(), 0);
      Assert.assertEquals(1., aggregator.getFloat(), 0);
    }
  }

  @Test
  public void testGetNull()
  {
    if (!isReplaceNullWithDefault()) {
      try (Aggregator aggregator = createAggregatorForValue(null)) {
        aggregator.aggregate();
        Assert.assertTrue(aggregator.isNull());
        Assert.assertNull(aggregator.get());
      }
    }
  }

  @Test
  public void testGetLongWithNull()
  {
    if (!isReplaceNullWithDefault()) {
      try (Aggregator aggregator = createAggregatorForValue(null)) {
        aggregator.aggregate();
        expectedException.expect(IllegalStateException.class);
        expectedException.expectMessage("Cannot return long for Null Value");
        aggregator.getLong();
      }
    }
  }

  @Test
  public void testGetDoubleWithNull()
  {
    if (!isReplaceNullWithDefault()) {
      try (Aggregator aggregator = createAggregatorForValue(null)) {
        aggregator.aggregate();
        expectedException.expect(IllegalStateException.class);
        expectedException.expectMessage("Cannot return double for Null Value");
        aggregator.getDouble();
      }
    }
  }

  @Test
  public void testGetFloatWithNull()
  {
    if (!isReplaceNullWithDefault()) {
      try (Aggregator aggregator = createAggregatorForValue(null)) {
        aggregator.aggregate();
        expectedException.expect(IllegalStateException.class);
        expectedException.expectMessage("Cannot return float for Null Value");
        aggregator.getFloat();
      }
    }
  }

  private Aggregator createAggregatorForValue(@Nullable Long val)
  {
    ListBasedSingleColumnCursor<Long> cursor = new ListBasedSingleColumnCursor<>(
        Long.class,
        Collections.singletonList(val)
    );
    return aggregatorFactory.factorize(cursor.getColumnSelectorFactory());
  }
}
