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
import org.apache.druid.query.monomorphicprocessing.HotLoopCallee;
import org.apache.druid.query.monomorphicprocessing.RuntimeShapeInspector;
import org.apache.druid.segment.selector.settable.SettableLongColumnValueSelector;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import javax.annotation.Nullable;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;

public class LongSumBufferAggregatorTest2 extends LongSumAggregatorTestBase
{
  private ByteBuffer buffer;
  private SettableLongColumnValueSelector columnValueSelector;
  private SimpleLongBufferAggregator aggregator;

  @Before
  public void setup()
  {
    buffer = ByteBuffer.allocate(aggregatorFactory.getMaxIntermediateSizeWithNulls());
    columnValueSelector = new SettableLongColumnValueSelector();
    SettableColumnSelectorFactory columnSelectorFactory = new SettableColumnSelectorFactory(columnValueSelector);
    aggregator = (SimpleLongBufferAggregator) aggregatorFactory.factorizeBuffered(columnSelectorFactory);
  }

  @Test
  public void testInit()
  {
    // write a garbage
    buffer.putLong(0, 10L);
    LongSumBufferAggregator aggregator = new LongSumBufferAggregator(new SettableLongColumnValueSelector());
    aggregator.init(buffer, 0);
    Assert.assertEquals(0, buffer.getLong(0));
  }

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
        bufferAggregate(aggregatorFactory, INTERVAL)
    );
  }

  // TODO: maybe can add util methods for these tests
  @Test
  public void testGet()
  {
    columnValueSelector.setValue(1);
    aggregator.init(buffer, 0);
    aggregator.aggregate(buffer, 0);
    Assert.assertEquals(1L, aggregator.get(buffer, 0));
    Assert.assertEquals(1L, aggregator.getLong(buffer, 0));
    Assert.assertEquals(1., aggregator.getDouble(buffer, 0), 0);
    Assert.assertEquals(1., aggregator.getFloat(buffer, 0), 0);
  }

  @Test
  public void testIsNull()
  {
    columnValueSelector.setNull(true);
    aggregator.init(buffer, 0);
    aggregator.aggregate(buffer, 0);

    if (replaceNullWithDefault) {
      Assert.assertFalse(aggregator.isNull(buffer, 0));
      Assert.assertEquals(0L, aggregator.get(buffer, 0));
    } else {
      Assert.assertTrue(aggregator.isNull(buffer, 0));
      Assert.assertNull(aggregator.get(buffer, 0));
    }

    // TODO: check exceptions
//    aggregator.getLong();
//    Assert.assertEquals(1., aggregator.getDouble(), 0);
//    Assert.assertEquals(1., aggregator.getFloat(), 0);
  }

  @Test
  public void testClose()
  {
    // close does nothing
  }

  @Test
  public void testRelocate()
  {
    // relocate does nothing
  }

  @Test
  public void testInspectRuntimeShape()
  {
    RecordingRuntimeShapeInspector runtimeShapeInspector = new RecordingRuntimeShapeInspector();
    aggregator.inspectRuntimeShape(runtimeShapeInspector);
    Assert.assertEquals(1, runtimeShapeInspector.visited.size());
    Pair<String, Object> visited = runtimeShapeInspector.visited.get(0);
    Assert.assertEquals("selector", visited.lhs);
    Assert.assertEquals(aggregator.selector, visited.rhs);
  }

  private static class RecordingRuntimeShapeInspector implements RuntimeShapeInspector
  {
    private final List<Pair<String, Object>> visited = new ArrayList<>();

    @Override
    public void visit(String fieldName, @Nullable HotLoopCallee value)
    {
      visited.add(Pair.of(fieldName, value));
    }

    @Override
    public void visit(String fieldName, @Nullable Object value)
    {
      visited.add(Pair.of(fieldName, value));
    }

    @Override
    public <T> void visit(String fieldName, T[] values)
    {
      visited.add(Pair.of(fieldName, values));
    }

    @Override
    public void visit(String flagName, boolean flagValue)
    {
      visited.add(Pair.of(flagName, flagName));
    }

    @Override
    public void visit(String key, String runtimeShape)
    {
      visited.add(Pair.of(key, runtimeShape));
    }
  }
}
