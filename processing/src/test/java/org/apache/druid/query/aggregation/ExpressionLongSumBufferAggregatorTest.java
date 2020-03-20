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

import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.query.aggregation.AggregateTestBase.TestColumn;
import org.apache.druid.query.expression.TestExprMacroTable;
import org.apache.druid.segment.ListBasedSingleColumnCursor;
import org.apache.druid.testing.InitializedNullHandlingTest;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import javax.annotation.Nullable;
import java.nio.ByteBuffer;
import java.util.Collections;

public class ExpressionLongSumBufferAggregatorTest extends InitializedNullHandlingTest
{
  private final LongSumAggregatorFactory aggregatorFactory = new LongSumAggregatorFactory(
      TestColumn.LONG_COLUMN.getName(),
      null,
      StringUtils.format("%s + 1", TestColumn.LONG_COLUMN.getName()),
      TestExprMacroTable.INSTANCE
  );

  private ByteBuffer buffer;

  @Before
  public void setup()
  {
    buffer = ByteBuffer.allocate(aggregatorFactory.getMaxIntermediateSizeWithNulls());
  }

  @Test
  public void testGet()
  {
    try (BufferAggregator aggregator = createAggregatorForValue(1L)) {
      aggregator.init(buffer, 0);
      aggregator.aggregate(buffer, 0);
      Assert.assertEquals(2L, aggregator.get(buffer, 0));
      Assert.assertEquals(2L, aggregator.getLong(buffer, 0));
      Assert.assertEquals(2., aggregator.getDouble(buffer, 0), 0.000001);
      Assert.assertEquals(2., aggregator.getFloat(buffer, 0), 0.000001);
    }
  }

  @Test
  public void testGetNull()
  {
    if (!isReplaceNullWithDefault()) {
      try (BufferAggregator aggregator = createAggregatorForValue(null)) {
        aggregator.init(buffer, 0);
        aggregator.aggregate(buffer, 0);
        Assert.assertTrue(aggregator.isNull(buffer, 0));
        Assert.assertNull(aggregator.get(buffer, 0));
      }
    }
  }

  private BufferAggregator createAggregatorForValue(@Nullable Long val)
  {
    ListBasedSingleColumnCursor<Long> cursor = new ListBasedSingleColumnCursor<>(
        Long.class,
        Collections.singletonList(val)
    );
    return aggregatorFactory.factorizeBuffered(cursor.getColumnSelectorFactory());
  }
}
