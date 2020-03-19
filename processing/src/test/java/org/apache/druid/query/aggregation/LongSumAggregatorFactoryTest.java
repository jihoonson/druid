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
import org.apache.druid.query.aggregation.AggregatorTestBase.SettableColumnSelectorFactory;
import org.apache.druid.query.aggregation.AggregatorTestBase.TestColumn;
import org.apache.druid.query.dimension.DimensionSpec;
import org.apache.druid.query.expression.TestExprMacroTable;
import org.apache.druid.segment.ColumnSelectorFactory;
import org.apache.druid.segment.ColumnValueSelector;
import org.apache.druid.segment.column.ColumnCapabilities;
import org.apache.druid.segment.selector.settable.SettableLongColumnValueSelector;
import org.apache.druid.segment.vector.MultiValueDimensionVectorSelector;
import org.apache.druid.segment.vector.SingleValueDimensionVectorSelector;
import org.apache.druid.segment.vector.VectorColumnSelectorFactory;
import org.apache.druid.segment.vector.VectorObjectSelector;
import org.apache.druid.segment.vector.VectorValueSelector;
import org.apache.druid.testing.InitializedNullHandlingTest;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import javax.annotation.Nullable;
import java.util.Collection;

@RunWith(Parameterized.class)
public class LongSumAggregatorFactoryTest extends InitializedNullHandlingTest
{
  private ColumnValueSelector<Long> columnValueSelector;
  private ColumnSelectorFactory columnSelectorFactory;
  private VectorColumnSelectorFactory vectorColumnSelectorFactory;
  private final LongSumAggregatorFactory aggregatorFactory;

  @Parameterized.Parameters
  public static Collection<Object[]> constructorFeeder()
  {
    return ImmutableList.of(
        new Object[]{new LongSumAggregatorFactory(TestColumn.LONG_COLUMN.getName(), TestColumn.LONG_COLUMN.getName())},
        new Object[]{
            new LongSumAggregatorFactory(
                TestColumn.LONG_COLUMN.getName(),
                null,
                "expression",
                TestExprMacroTable.INSTANCE
            )
        }
    );
  }

  public LongSumAggregatorFactoryTest(LongSumAggregatorFactory aggregatorFactory)
  {
    this.aggregatorFactory = aggregatorFactory;
  }

  @Before
  public void setup()
  {
    columnValueSelector = new SettableLongColumnValueSelector();
    columnSelectorFactory = new SettableColumnSelectorFactory(columnValueSelector);
    vectorColumnSelectorFactory = new NoopVectorColumnSelectorFactory();
  }

  @Test
  public void testFactorize()
  {
    Assert.assertSame(LongSumAggregator.class, aggregatorFactory.factorize(columnSelectorFactory).getClass());
  }

  @Test
  public void testFactorizeBuffered()
  {
    Assert.assertSame(
        LongSumBufferAggregator.class,
        aggregatorFactory.factorizeBuffered(columnSelectorFactory).getClass()
    );
  }

  @Test
  public void testFactorizeBuffered2()
  {
    Assert.assertSame(
        LongSumBufferAggregator.class,
        aggregatorFactory.factorizeBuffered(columnSelectorFactory, columnValueSelector).getClass()
    );
  }

  @Test
  public void testFactorizeVector()
  {
    if (aggregatorFactory.canVectorize()) {
      Assert.assertSame(
          LongSumVectorAggregator.class,
          aggregatorFactory.factorizeVector(vectorColumnSelectorFactory).getClass()
      );
    }
  }

  @Test
  public void testCanVectorize()
  {
    Assert.assertEquals(aggregatorFactory.getExpression() == null, aggregatorFactory.canVectorize());
  }

  @Test
  public void testGetComparator()
  {
    Assert.assertSame(LongSumAggregator.COMPARATOR, aggregatorFactory.getComparator());
  }

  @Test
  public void testCombine()
  {
    Assert.assertEquals(3L, aggregatorFactory.combine(1L, 2L));
    Assert.assertEquals(3L, aggregatorFactory.combine(1., 2.));
    Assert.assertEquals(3L, aggregatorFactory.combine(1.f, 2.f));
    Assert.assertEquals(3L, aggregatorFactory.combine(1.1, 2.1));
  }

  @Test
  public void testCombineWithNull()
  {
    Assert.assertEquals(1L, aggregatorFactory.combine(1L, null));
    Assert.assertEquals(2L, aggregatorFactory.combine(null, 2L));
    Assert.assertNull(aggregatorFactory.combine(null, null));
  }

  private static class NoopVectorColumnSelectorFactory implements VectorColumnSelectorFactory
  {

    @Override
    public int getMaxVectorSize()
    {
      return 0;
    }

    @Override
    public SingleValueDimensionVectorSelector makeSingleValueDimensionSelector(DimensionSpec dimensionSpec)
    {
      return null;
    }

    @Override
    public MultiValueDimensionVectorSelector makeMultiValueDimensionSelector(DimensionSpec dimensionSpec)
    {
      return null;
    }

    @Override
    public VectorValueSelector makeValueSelector(String column)
    {
      return null;
    }

    @Override
    public VectorObjectSelector makeObjectSelector(String column)
    {
      return null;
    }

    @Nullable
    @Override
    public ColumnCapabilities getColumnCapabilities(String column)
    {
      return null;
    }
  }
}
