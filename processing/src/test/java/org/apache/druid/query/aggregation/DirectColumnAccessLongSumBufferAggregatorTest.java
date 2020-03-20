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

import org.apache.druid.java.util.common.Pair;
import org.apache.druid.query.aggregation.AggregateTestBase.TestColumn;
import org.apache.druid.segment.ListBasedSingleColumnCursor;
import org.apache.druid.testing.InitializedNullHandlingTest;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import javax.annotation.Nullable;
import java.nio.ByteBuffer;
import java.util.Collections;

public class DirectColumnAccessLongSumBufferAggregatorTest extends InitializedNullHandlingTest
{
  @Rule
  public ExpectedException expectedException = ExpectedException.none();

  private final LongSumAggregatorFactory aggregatorFactory = new LongSumAggregatorFactory(
      TestColumn.LONG_COLUMN.getName(),
      TestColumn.LONG_COLUMN.getName()
  );

  private ByteBuffer buffer;

  @Before
  public void setup()
  {
    buffer = ByteBuffer.allocate(aggregatorFactory.getMaxIntermediateSizeWithNulls());
  }

  @Test
  public void testInit()
  {
    // write a garbage
    buffer.putLong(0, 10L);
    try (BufferAggregator aggregator = createAggregatorForValue(null)) {
      aggregator.init(buffer, 0);
      Assert.assertEquals(isReplaceNullWithDefault() ? 0L : 72057594037927936L, buffer.getLong(0));
    }
  }

  @Test
  public void testGet()
  {
    try (BufferAggregator aggregator = createAggregatorForValue(1L)) {
      aggregator.init(buffer, 0);
      aggregator.aggregate(buffer, 0);
      Assert.assertEquals(1L, aggregator.get(buffer, 0));
      Assert.assertEquals(1L, aggregator.getLong(buffer, 0));
      Assert.assertEquals(1., aggregator.getDouble(buffer, 0), 0);
      Assert.assertEquals(1., aggregator.getFloat(buffer, 0), 0);
    }
  }

  @Test
  public void testIsNull()
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

  @Test
  public void testGetLongWithNull()
  {
    if (!isReplaceNullWithDefault()) {
      try (BufferAggregator aggregator = createAggregatorForValue(null)) {
        aggregator.init(buffer, 0);
        aggregator.aggregate(buffer, 0);
        expectedException.expect(IllegalStateException.class);
        expectedException.expectMessage("Cannot return long for Null Value");
        aggregator.getLong(buffer, 0);
      }
    }
  }

  @Test
  public void testGetDoubleWithNull()
  {
    if (!isReplaceNullWithDefault()) {
      try (BufferAggregator aggregator = createAggregatorForValue(null)) {
        aggregator.init(buffer, 0);
        aggregator.aggregate(buffer, 0);
        expectedException.expect(IllegalStateException.class);
        expectedException.expectMessage("Cannot return double for Null Value");
        aggregator.getDouble(buffer, 0);
      }
    }
  }

  @Test
  public void testGetFloatWithNull()
  {
    if (!isReplaceNullWithDefault()) {
      try (BufferAggregator aggregator = createAggregatorForValue(null)) {
        aggregator.init(buffer, 0);
        aggregator.aggregate(buffer, 0);
        expectedException.expect(IllegalStateException.class);
        expectedException.expectMessage("Cannot return float for Null Value");
        aggregator.getFloat(buffer, 0);
      }
    }
  }

  @Test
  public void testRelocate()
  {
    // relocate does nothing
    try (BufferAggregator aggregator = createAggregatorForValue(1L)) {
      // write some value
      aggregator.init(buffer, 0);
      aggregator.aggregate(buffer, 0);

      byte[] copy = new byte[buffer.array().length];
      System.arraycopy(buffer.array(), 0, copy, 0, copy.length);
      ByteBuffer originalBuffer = ByteBuffer.wrap(copy);
      ByteBuffer newBuffer = ByteBuffer.allocate(0);
      aggregator.relocate(0, 0, buffer, newBuffer);
      Assert.assertArrayEquals(originalBuffer.array(), buffer.array());
    }
  }

  @Test
  public void testInspectRuntimeShape()
  {
    try (BufferAggregator aggregator = createAggregatorForValue(1L)) {
      RecordingRuntimeShapeInspector runtimeShapeInspector = new RecordingRuntimeShapeInspector();
      aggregator.inspectRuntimeShape(runtimeShapeInspector);
      if (isReplaceNullWithDefault()) {
        Assert.assertEquals(1, runtimeShapeInspector.getVisited().size());
        Pair<String, Object> visited = runtimeShapeInspector.getVisited().get(0);
        Assert.assertEquals("selector", visited.lhs);
      } else {
        Assert.assertEquals(3, runtimeShapeInspector.getVisited().size());
        Pair<String, Object> visited = runtimeShapeInspector.getVisited().get(0);
        Assert.assertEquals("delegate", visited.lhs);
        visited = runtimeShapeInspector.getVisited().get(1);
        Assert.assertEquals("selector", visited.lhs);
        visited = runtimeShapeInspector.getVisited().get(2);
        Assert.assertEquals("nullSelector", visited.lhs);
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
