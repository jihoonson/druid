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
import org.apache.druid.query.dimension.DimensionSpec;
import org.apache.druid.query.monomorphicprocessing.RuntimeShapeInspector;
import org.apache.druid.segment.ColumnSelectorFactory;
import org.apache.druid.segment.ColumnValueSelector;
import org.apache.druid.segment.DimensionSelector;
import org.apache.druid.segment.column.ColumnCapabilities;
import org.junit.Test;

import javax.annotation.Nullable;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.LongStream;

public class PolyglotJsBufferAggregatorTest
{
  private int cursor = 0;

  @Test
  public void testAggregate()
  {
    final PolyglotJsAggregatorFactory factory = new PolyglotJsAggregatorFactory(
        "name",
        ImmutableList.of("x", "y"),
        "(current,x) => x > 0 ? current + x : current - 10",
        "() => 0"
    );
    final List<Long> values = LongStream.range(0, 10).boxed().collect(Collectors.toList());
    final BufferAggregator aggregator = factory.factorizeBuffered(new TestColumnSelectorFactory(values));
    ByteBuffer buffer = ByteBuffer.allocate(Long.BYTES);

    aggregator.init(buffer, 0);
    for (int i = 0; i < values.size(); i++) {
      aggregator.aggregate(buffer, 0);
      cursor++;
    }
    System.out.println(aggregator.get(buffer, 0));
    aggregator.close();
  }

  private class TestColumnSelectorFactory implements ColumnSelectorFactory
  {
    private final List<Long> values;

    private TestColumnSelectorFactory(List<Long> values)
    {
      this.values = values;
    }

    @Override
    public DimensionSelector makeDimensionSelector(DimensionSpec dimensionSpec)
    {
      return null;
    }

    @Override
    public ColumnValueSelector makeColumnValueSelector(String columnName)
    {
      return new TestColumnValueSelector(values);
    }

    @Nullable
    @Override
    public ColumnCapabilities getColumnCapabilities(String column)
    {
      return null;
    }
  }

  private class TestColumnValueSelector implements ColumnValueSelector<Long>
  {
    private final List<Long> values;

    private TestColumnValueSelector(List<Long> values)
    {
      this.values = values;
    }

    @Override
    public double getDouble()
    {
      return values.get(cursor);
    }

    @Override
    public float getFloat()
    {
      return values.get(cursor);
    }

    @Override
    public long getLong()
    {
      return values.get(cursor);
    }

    @Override
    public void inspectRuntimeShape(RuntimeShapeInspector inspector)
    {

    }

    @Override
    public boolean isNull()
    {
      return false;
    }

    @Nullable
    @Override
    public Long getObject()
    {
      return values.get(cursor);
    }

    @Override
    public Class<? extends Long> classOfObject()
    {
      return Long.class;
    }
  }
}
