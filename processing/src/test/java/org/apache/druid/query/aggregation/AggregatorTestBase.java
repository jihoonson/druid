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
import com.google.common.collect.Iterables;
import org.apache.druid.common.ProcessingTestToolbox;
import org.apache.druid.common.utils.UUIDUtils;
import org.apache.druid.data.gen.TestColumnSchema;
import org.apache.druid.data.gen.TestDataGenerator;
import org.apache.druid.data.gen.TestSchemaInfo;
import org.apache.druid.data.input.impl.DimensionSchema.ValueType;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.java.util.common.granularity.Granularity;
import org.apache.druid.java.util.common.guava.Sequence;
import org.apache.druid.java.util.common.guava.Sequences;
import org.apache.druid.java.util.common.io.Closer;
import org.apache.druid.query.dimension.DimensionSpec;
import org.apache.druid.query.monomorphicprocessing.RuntimeShapeInspector;
import org.apache.druid.segment.ColumnSelectorFactory;
import org.apache.druid.segment.ColumnValueSelector;
import org.apache.druid.segment.Cursor;
import org.apache.druid.segment.DimensionSelector;
import org.apache.druid.segment.IndexSpec;
import org.apache.druid.segment.ObjectColumnSelector;
import org.apache.druid.segment.QueryableIndex;
import org.apache.druid.segment.QueryableIndexStorageAdapter;
import org.apache.druid.segment.VirtualColumns;
import org.apache.druid.segment.column.ColumnCapabilities;
import org.apache.druid.segment.incremental.IncrementalIndex;
import org.apache.druid.segment.incremental.IncrementalIndexSchema;
import org.apache.druid.segment.incremental.IncrementalIndexStorageAdapter;
import org.joda.time.Interval;
import org.junit.Rule;
import org.junit.rules.TemporaryFolder;

import javax.annotation.Nullable;
import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.function.Consumer;
import java.util.function.Supplier;

public class AggregatorTestBase
{
  // float, double, long, single-valued string, multi-valued string w/o nulls
  // custom column value selector for complex type

  // get as dimension selector
  // get as column value selector

  public static final String FLOAT_COLUMN = "floatColumn";
  public static final String DOUBLE_COLUMN = "doubleColumn";
  public static final String LONG_COLUMN = "longColumn";
  public static final String SINGLE_VALUE_STRING_COLUMN = "singleValueStringColumn";
  public static final String MULTI_VALUE_STRING_COLUMN = "multiValueStringColumn";

  @Rule
  public TemporaryFolder temporaryFolder = new TemporaryFolder();

  private final IndexStuff indexStuff;

  public AggregatorTestBase(
      Interval interval,
      Granularity segmentGranularity,
      int numRows,
      double nullRatio,
      List<AggregatorFactory> aggregatorFactories,
      boolean rollup,
      boolean persist
  )
  {
    final List<TestColumnSchema> columnSchemas = ImmutableList.of(
        TestColumnSchema.makeSequential(
            FLOAT_COLUMN,
            ValueType.FLOAT,
            false,
            1,
            nullRatio,
            -(numRows / 2),
            numRows / 2
        ),
        TestColumnSchema.makeSequential(
            DOUBLE_COLUMN,
            ValueType.DOUBLE,
            false,
            1,
            nullRatio,
            -(numRows / 2),
            numRows / 2
        ),
        TestColumnSchema.makeSequential(
            LONG_COLUMN,
            ValueType.LONG,
            false,
            1,
            nullRatio,
            -(numRows / 2),
            numRows / 2
        ),
        TestColumnSchema.makeEnumeratedSequential(
            SINGLE_VALUE_STRING_COLUMN,
            ValueType.STRING,
            false,
            1,
            nullRatio,
            Arrays.asList("Apple", "Orange", "Xylophone", "Corundum", null)
        ),
        TestColumnSchema.makeEnumeratedSequential(
            MULTI_VALUE_STRING_COLUMN,
            ValueType.STRING,
            false,
            4,
            nullRatio,
            Arrays.asList("Apple", "Orange", "Xylophone", "Corundum", null)
        )
    );

    final int numTimePartitions = Iterables.size(segmentGranularity.getIterable(interval));
    final int numRowsPerTimePartition = numRows / numTimePartitions;
    final TestSchemaInfo testSchemaInfo = new TestSchemaInfo(
        columnSchemas,
        aggregatorFactories,
        interval,
        rollup
    );
    final TestDataGenerator dataGenerator = new TestDataGenerator(columnSchemas, 0, interval, numRowsPerTimePartition);

    if (persist) {
      indexStuff = new QueryableIndexStuff(
          new ProcessingTestToolbox(),
          dataGenerator,
          testSchemaInfo,
          numTimePartitions,
          numRowsPerTimePartition,
          rollup,
          () -> {
            try {
              return temporaryFolder.newFolder();
            }
            catch (IOException e) {
              throw new RuntimeException(e);
            }
          }
      );
    } else {
      indexStuff = new IncrementalIndexStuff(
          dataGenerator,
          testSchemaInfo,
          numTimePartitions,
          numRowsPerTimePartition,
          rollup
      );
    }
  }

  public <T> T aggregate(String columnName, AggregatorFactory aggregatorFactory, Interval interval)
  {
    SettableColumnValueSelector columnValueSelector = new SettableColumnValueSelector();
    Aggregator combiner = aggregatorFactory.getCombiningFactory().factorize(
        new SettableColumnSelectorFactory(columnValueSelector)
    );
    return (T) indexStuff.createCursorSequence(interval)
                         .map(cursor -> {
                           Aggregator aggregator = aggregatorFactory.factorize(cursor.getColumnSelectorFactory());
                           while (!cursor.isDone()) {
                             aggregator.aggregate();
                             cursor.advance();
                           }
                           aggregator.close();
                           return aggregator.get();
                         })
                         .accumulate(null, (accumulated, val) -> {
                           columnValueSelector.setVal(val);
                           combiner.aggregate();
                           return combiner.get();
                         });
  }

  private interface IndexStuff extends Closeable
  {
    Sequence<Cursor> createCursorSequence(Interval interval);
  }

  private static class IncrementalIndexStuff implements IndexStuff
  {
    private final List<IncrementalIndex<?>> indexList = new ArrayList<>();

    private IncrementalIndexStuff(
        TestDataGenerator dataGenerator,
        TestSchemaInfo testSchemaInfo,
        int numTimePartitions,
        int numRowsPerTimePartition,
        boolean rollup
    )
    {
      createIncrementalIndexes(
          dataGenerator,
          testSchemaInfo,
          numTimePartitions,
          numRowsPerTimePartition,
          rollup,
          indexList::add
      );
    }

    @Override
    public Sequence<Cursor> createCursorSequence(Interval interval)
    {
      return Sequences
          .simple(indexList)
          .flatMap(index -> new IncrementalIndexStorageAdapter(index).makeCursors(
              null,
              interval,
              VirtualColumns.EMPTY,
              Granularities.ALL,
              false,
              null
          ));
    }

    @Override
    public void close() throws IOException
    {
      final Closer closer = Closer.create();
      closer.registerAll(indexList);
      closer.close();
    }
  }

  private static class QueryableIndexStuff implements IndexStuff
  {
    private final List<QueryableIndex> indexList = new ArrayList<>();

    private QueryableIndexStuff(
        ProcessingTestToolbox toolbox,
        TestDataGenerator dataGenerator,
        TestSchemaInfo testSchemaInfo,
        int numTimePartitions,
        int numRowsPerTimePartition,
        boolean rollup,
        Supplier<File> tmpDirSupplier
    )
    {
      final File tmpDir = tmpDirSupplier.get();
      createIncrementalIndexes(
          dataGenerator,
          testSchemaInfo, numTimePartitions,
          numRowsPerTimePartition,
          rollup,
          incrementalIndex -> {
            try {
              final File file = toolbox.getIndexMerger().persist(
                  incrementalIndex,
                  new File(tmpDir, UUIDUtils.generateUuid()),
                  new IndexSpec(),
                  null
              );
              indexList.add(toolbox.getIndexIO().loadIndex(file));
            }
            catch (IOException e) {
              throw new RuntimeException(e);
            }
          }
      );
    }

    @Override
    public Sequence<Cursor> createCursorSequence(Interval interval)
    {
      return Sequences
          .simple(indexList)
          .flatMap(index -> new QueryableIndexStorageAdapter(index).makeCursors(
              null,
              interval,
              VirtualColumns.EMPTY,
              Granularities.ALL,
              false,
              null
          ));
    }

    @Override
    public void close() throws IOException
    {
      final Closer closer = Closer.create();
      closer.registerAll(indexList);
      closer.close();
    }
  }

  private static void createIncrementalIndexes(
      TestDataGenerator dataGenerator,
      TestSchemaInfo testSchemaInfo,
      int numTimePartitions,
      int numRowsPerTimePartition,
      boolean rollup,
      Consumer<IncrementalIndex<?>> consumer
  )
  {
    for (int i = 0; i < numTimePartitions; i++) {
      IncrementalIndex<Aggregator> incrementalIndex = new IncrementalIndex.Builder()
          .setIndexSchema(
              new IncrementalIndexSchema.Builder()
                  .withDimensionsSpec(testSchemaInfo.getDimensionsSpec())
                  .withMetrics(testSchemaInfo.getAggsArray())
                  .withRollup(rollup)
                  .build()
          )
          .setMaxRowCount(numRowsPerTimePartition) // create one segment per time chunk for the sake of convenience
          .buildOnheap();

      try {
        for (int j = 0; j < numRowsPerTimePartition; j++) {
          incrementalIndex.add(dataGenerator.nextRow());
        }
        consumer.accept(incrementalIndex);
      }
      catch (IOException e) {
        throw new RuntimeException(e);
      }
    }
  }

  private static class SettableColumnSelectorFactory implements ColumnSelectorFactory
  {
    private final ColumnValueSelector columnValueSelector;

    private SettableColumnSelectorFactory(SettableColumnValueSelector columnValueSelector)
    {
      this.columnValueSelector = columnValueSelector;
    }

    @Override
    public DimensionSelector makeDimensionSelector(DimensionSpec dimensionSpec)
    {
      return null;
    }

    @Override
    public ColumnValueSelector makeColumnValueSelector(String columnName)
    {
      return columnValueSelector;
    }

    @Nullable
    @Override
    public ColumnCapabilities getColumnCapabilities(String column)
    {
      return null;
    }
  }

  private static class SettableColumnValueSelector extends ObjectColumnSelector<Object>
  {
    private Object val;

    public void setVal(Object val)
    {
      this.val = val;
    }

    @Override
    public void inspectRuntimeShape(RuntimeShapeInspector inspector)
    {
    }

    @Nullable
    @Override
    public Object getObject()
    {
      return val;
    }

    @Override
    public Class<? extends Object> classOfObject()
    {
      return val.getClass();
    }
  }
}
