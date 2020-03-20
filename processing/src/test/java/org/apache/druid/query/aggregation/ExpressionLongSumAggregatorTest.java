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
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.query.aggregation.AggregateTestBase.TestColumn;
import org.apache.druid.query.expression.TestExprMacroTable;
import org.apache.druid.segment.ListBasedSingleColumnCursor;
import org.apache.druid.testing.InitializedNullHandlingTest;
import org.junit.Assert;
import org.junit.Test;

import java.util.Collections;

public class ExpressionLongSumAggregatorTest extends InitializedNullHandlingTest
{
  private final LongSumAggregatorFactory aggregatorFactory = new LongSumAggregatorFactory(
      TestColumn.LONG_COLUMN.getName(),
      null,
      StringUtils.format("%s + 1", TestColumn.LONG_COLUMN.getName()),
      TestExprMacroTable.INSTANCE
  );

  @Test
  public void testGet()
  {
    ListBasedSingleColumnCursor<Long> cursor = new ListBasedSingleColumnCursor<>(
        Long.class,
        ImmutableList.of(1L)
    );
    try (Aggregator aggregator = aggregatorFactory.factorize(cursor.getColumnSelectorFactory())) {
      aggregator.aggregate();
      Assert.assertEquals(2L, aggregator.get());
      Assert.assertEquals(2L, aggregator.getLong());
      Assert.assertEquals(2., aggregator.getDouble(), 0.000001);
      Assert.assertEquals(2., aggregator.getFloat(), 0.000001);
    }
  }

  @Test
  public void testGetNull()
  {
    if (!isReplaceNullWithDefault()) {
      ListBasedSingleColumnCursor<Long> cursor = new ListBasedSingleColumnCursor<>(
          Long.class,
          Collections.singletonList(null)
      );
      try (Aggregator aggregator = aggregatorFactory.factorize(cursor.getColumnSelectorFactory())) {
        aggregator.aggregate();
        Assert.assertTrue(aggregator.isNull());
        Assert.assertNull(aggregator.get());
      }
    }
  }
}
