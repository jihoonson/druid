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

package org.apache.druid.query.groupby;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.InjectableValues;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.smile.SmileFactory;
import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import com.google.common.collect.ImmutableMap;
import org.apache.druid.collections.BlockingPool;
import org.apache.druid.collections.DefaultBlockingPool;
import org.apache.druid.collections.NonBlockingPool;
import org.apache.druid.collections.StupidPool;
import org.apache.druid.data.input.InputRow;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.apache.druid.java.util.common.FileUtils;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.concurrent.Execs;
import org.apache.druid.java.util.common.granularity.Granularity;
import org.apache.druid.java.util.common.guava.Sequence;
import org.apache.druid.java.util.common.guava.Yielder;
import org.apache.druid.java.util.common.guava.Yielders;
import org.apache.druid.math.expr.ExprMacroTable;
import org.apache.druid.offheap.OffheapBufferGenerator;
import org.apache.druid.query.DictionaryConversion;
import org.apache.druid.query.DictionaryMergingQueryRunnerFactory;
import org.apache.druid.query.DruidProcessingConfig;
import org.apache.druid.query.FinalizeResultsQueryRunner;
import org.apache.druid.query.QueryPlus;
import org.apache.druid.query.QueryRunner;
import org.apache.druid.query.QueryRunnerTestHelper;
import org.apache.druid.query.QueryToolChest;
import org.apache.druid.query.SegmentGroupByQueryProcessor;
import org.apache.druid.query.SegmentIdMapper;
import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.query.aggregation.CountAggregatorFactory;
import org.apache.druid.query.aggregation.LongSumAggregatorFactory;
import org.apache.druid.query.aggregation.hyperloglog.HyperUniquesSerde;
import org.apache.druid.query.context.ResponseContext;
import org.apache.druid.query.dimension.DefaultDimensionSpec;
import org.apache.druid.query.expression.TestExprMacroTable;
import org.apache.druid.query.groupby.strategy.GroupByStrategySelector;
import org.apache.druid.query.groupby.strategy.GroupByStrategyV1;
import org.apache.druid.query.groupby.strategy.GroupByStrategyV2;
import org.apache.druid.query.spec.MultipleIntervalSegmentSpec;
import org.apache.druid.query.spec.QuerySegmentSpec;
import org.apache.druid.segment.IndexIO;
import org.apache.druid.segment.IndexMergerV9;
import org.apache.druid.segment.IndexSpec;
import org.apache.druid.segment.QueryableIndex;
import org.apache.druid.segment.QueryableIndexSegment;
import org.apache.druid.segment.column.ColumnConfig;
import org.apache.druid.segment.generator.DataGenerator;
import org.apache.druid.segment.generator.GeneratorBasicSchemas;
import org.apache.druid.segment.generator.GeneratorSchemaInfo;
import org.apache.druid.segment.incremental.IncrementalIndex;
import org.apache.druid.segment.incremental.IncrementalIndexCreator;
import org.apache.druid.segment.incremental.IncrementalIndexSchema;
import org.apache.druid.segment.serde.ComplexMetrics;
import org.apache.druid.segment.writeout.OffHeapMemorySegmentWriteOutMediumFactory;
import org.apache.druid.testing.InitializedNullHandlingTest;
import org.apache.druid.timeline.SegmentId;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;

public class GroupByRunnerMoreSeriousTest extends InitializedNullHandlingTest
{
  private static final Map<String, Map<String, GroupByQuery>> SCHEMA_QUERY_MAP = new LinkedHashMap<>();
  private static final ObjectMapper JSON_MAPPER = new DefaultObjectMapper();
  private static final int RNG_SEED = 9999;
  private static final int ROWS_PER_SEGMENT = 1100;
  private static final int NUM_SEGMENTS = 2;
  private static final int NUM_PROCESSING_THREADS = 16;

  private static IndexMergerV9 INDEX_MERGER;
  private static IndexIO INDEX_IO;

  private static final String QUERY_GRANULARITY = "day";
  private static File TMP_DIR;
  private static List<QueryableIndex> QUERYABLE_INDEXES;
  private static GroupByQueryRunnerFactory FACTORY;

  private final List<GroupByQuery> testQueries = new ArrayList<>();


  private GeneratorSchemaInfo schemaInfo;
  private GroupByQuery query;

  private ExecutorService executorService;

  @BeforeClass
  public static void setupClass() throws IOException
  {
    INDEX_IO = new IndexIO(
        JSON_MAPPER.setInjectableValues(
            new InjectableValues.Std()
                .addValue(ExprMacroTable.class.getName(), TestExprMacroTable.INSTANCE)
                .addValue(ObjectMapper.class.getName(), JSON_MAPPER)
        ),
        new ColumnConfig()
        {
          @Override
          public int columnCacheSizeBytes()
          {
            return 0;
          }
        }
    );
    INDEX_MERGER = new IndexMergerV9(JSON_MAPPER, INDEX_IO, OffHeapMemorySegmentWriteOutMediumFactory.instance());

    ComplexMetrics.registerSerde("hyperUnique", new HyperUniquesSerde());
    String schemaName = "basic";

    GeneratorSchemaInfo schemaInfo = GeneratorBasicSchemas.SCHEMA_MAP.get(schemaName);
    final DataGenerator dataGenerator = new DataGenerator(
        schemaInfo.getColumnSchemas(),
        RNG_SEED + 1,
        schemaInfo.getDataInterval(),
        ROWS_PER_SEGMENT
    );

    TMP_DIR = FileUtils.createTempDir();

    // queryableIndexes   -> numSegments worth of on-disk segments
    // anIncrementalIndex -> the last incremental index
    QUERYABLE_INDEXES = new ArrayList<>(NUM_SEGMENTS);

    for (int i = 0; i < NUM_SEGMENTS; i++) {
      try (final IncrementalIndex index = makeIncIndex(schemaInfo)) {
        for (int j = 0; j < ROWS_PER_SEGMENT; j++) {
          final InputRow row = dataGenerator.nextRow();
          index.add(row);
        }

        final File file = INDEX_MERGER.persist(
            index,
            new File(TMP_DIR, String.valueOf(i)),
            new IndexSpec(),
            null
        );

        QUERYABLE_INDEXES.add(INDEX_IO.loadIndex(file));
      }
    }

    NonBlockingPool<ByteBuffer> bufferPool = new StupidPool<>(
        "GroupByBenchmark-computeBufferPool",
        new OffheapBufferGenerator("compute", 250_000_000),
        0,
        Integer.MAX_VALUE
    );

    // limit of 2 is required since we simulate both historical merge and broker merge in the same process
    BlockingPool<ByteBuffer> mergePool = new DefaultBlockingPool<>(
        new OffheapBufferGenerator("merge", 250_000_000),
        2
    );
    final GroupByQueryConfig config = new GroupByQueryConfig()
    {
      @Override
      public int getBufferGrouperInitialBuckets()
      {
        return -1;
      }

      @Override
      public long getMaxOnDiskStorage()
      {
        return 1_000_000_000L;
      }
    };
    config.setSingleThreaded(false);
    config.setMaxIntermediateRows(Integer.MAX_VALUE);
    config.setMaxResults(Integer.MAX_VALUE);

    DruidProcessingConfig druidProcessingConfig = new DruidProcessingConfig()
    {
      @Override
      public int getNumThreads()
      {
        // Used by "v2" strategy for concurrencyHint
        return NUM_PROCESSING_THREADS;
      }

      @Override
      public String getFormatString()
      {
        return null;
      }
    };

    final Supplier<GroupByQueryConfig> configSupplier = Suppliers.ofInstance(config);
    final GroupByStrategySelector strategySelector = new GroupByStrategySelector(
        configSupplier,
        new GroupByStrategyV1(
            configSupplier,
            new GroupByQueryEngine(configSupplier, bufferPool),
            (query, future) -> {},
            bufferPool
        ),
        new GroupByStrategyV2(
            druidProcessingConfig,
            configSupplier,
            bufferPool,
            mergePool,
            new ObjectMapper(new SmileFactory()),
            (query, future) -> {}
        )
    );

    FACTORY = new GroupByQueryRunnerFactory(
        strategySelector,
        new GroupByQueryQueryToolChest(strategySelector)
    );
  }

  private static IncrementalIndex makeIncIndex(GeneratorSchemaInfo schemaInfo) throws JsonProcessingException
  {
    return IncrementalIndexCreator.parseIndexType("onheap").builder()
        .setIndexSchema(
            new IncrementalIndexSchema.Builder()
                .withDimensionsSpec(schemaInfo.getDimensionsSpec())
                .withMetrics(schemaInfo.getAggsArray())
                .withRollup(schemaInfo.isWithRollup())
                .build()
        )
        .setConcurrentEventAdd(true)
        .setMaxRowCount(ROWS_PER_SEGMENT)
        .build();
  }

  @Before
  public void setup()
  {
    executorService = Execs.multiThreaded(NUM_PROCESSING_THREADS, "GroupByThreadPool[%d]");

    setupQueries();
    String schemaName = "basic";
    String queryName = "A";

    query = SCHEMA_QUERY_MAP.get(schemaName).get(queryName);
  }

  @After
  public void tearDown()
  {
    executorService.shutdownNow();
  }

  public GroupByRunnerMoreSeriousTest()
  {
    setupQueries();
  }

  private void setupQueries()
  {
    GeneratorSchemaInfo basicSchema = GeneratorBasicSchemas.SCHEMA_MAP.get("basic");
    Map<String, GroupByQuery> basicQueries = new LinkedHashMap<>();

    { // basic.A
      QuerySegmentSpec intervalSpec = new MultipleIntervalSegmentSpec(Collections.singletonList(basicSchema.getDataInterval()));
      List<AggregatorFactory> queryAggs = new ArrayList<>();
      queryAggs.add(new CountAggregatorFactory("cnt"));
      queryAggs.add(new LongSumAggregatorFactory("sumLongSequential", "sumLongSequential"));
      GroupByQuery queryA = GroupByQuery
          .builder()
          .setDataSource("blah")
          .setQuerySegmentSpec(intervalSpec)
          .setDimensions(new DefaultDimensionSpec("dimSequential", null), new DefaultDimensionSpec("dimZipf", null))
          .setAggregatorSpecs(queryAggs)
          .setGranularity(Granularity.fromString(QUERY_GRANULARITY))
          .setContext(ImmutableMap.of("vectorize", true, "earlyDictMerge", true))
          .build();
      basicQueries.put("A", queryA);
    }

    { // basic.B
      QuerySegmentSpec intervalSpec = new MultipleIntervalSegmentSpec(Collections.singletonList(basicSchema.getDataInterval()));
      List<AggregatorFactory> queryAggs = new ArrayList<>();
      queryAggs.add(new CountAggregatorFactory("cnt"));
      queryAggs.add(new LongSumAggregatorFactory("sumLongSequential", "sumLongSequential"));
      GroupByQuery queryA = GroupByQuery
          .builder()
          .setDataSource("blah")
          .setQuerySegmentSpec(intervalSpec)
          .setDimensions(new DefaultDimensionSpec("dimHyperUnique", null), new DefaultDimensionSpec("dimUniform", null))
          .setAggregatorSpecs(queryAggs)
          .setGranularity(Granularity.fromString(QUERY_GRANULARITY))
          .setContext(ImmutableMap.of("vectorize", true, "earlyDictMerge", true))
          .build();

      basicQueries.put("B", queryA);
    }

    SCHEMA_QUERY_MAP.put("basic", basicQueries);

    Map<String, GroupByQuery> wideQueries = new LinkedHashMap<>();
    GeneratorSchemaInfo wideSchema = GeneratorBasicSchemas.SCHEMA_MAP.get("wide");

    {
      QuerySegmentSpec intervalSpec = new MultipleIntervalSegmentSpec(Collections.singletonList(wideSchema.getDataInterval()));
      List<AggregatorFactory> queryAggs = new ArrayList<>();
      queryAggs.add(new CountAggregatorFactory("cnt"));
      queryAggs.add(new LongSumAggregatorFactory("sumLong1", "long1"));
      GroupByQuery queryA = GroupByQuery
          .builder()
          .setDataSource("blah")
          .setQuerySegmentSpec(intervalSpec)
          .setDimensions(new DefaultDimensionSpec("string6", null))
          .setAggregatorSpecs(queryAggs)
          .setGranularity(Granularity.fromString(QUERY_GRANULARITY))
          .setContext(
              ImmutableMap.of(
                  "vectorize",
                  true,
                  "earlyDictMerge",
                  true,
                  "bufferGrouperInitialBuckets",
                  10240
              )
          )
          .build();

      wideQueries.put("A", queryA);

    }
    SCHEMA_QUERY_MAP.put("wide", wideQueries);
  }

  @Test
  public void testMultiQueryableIndex()
  {
    QueryToolChest<ResultRow, GroupByQuery> toolChest = FACTORY.getToolchest();
    SegmentIdMapper segmentIdMapper = new SegmentIdMapper();
    SegmentIdMapper segmentIdMapper2 = new SegmentIdMapper();

    QueryRunner<ResultRow> theRunner = new FinalizeResultsQueryRunner<>(
        toolChest.mergeResults(
            FACTORY.mergeRunners(
                executorService,
                makeMultiRunners(segmentIdMapper),
                new DictionaryMergingQueryRunnerFactory().mergeRunners(executorService, makeDictScanRunners(segmentIdMapper))
            )
        ),
        (QueryToolChest) toolChest
    );

    QueryRunner<ResultRow> theRunner2 = new FinalizeResultsQueryRunner<>(
        toolChest.mergeResults(
            FACTORY.mergeRunners2(
                executorService,
                makeMultiRunners2(segmentIdMapper2),
                new DictionaryMergingQueryRunnerFactory().mergeRunners(executorService, makeDictScanRunners(segmentIdMapper2))
            )
        ),
        (QueryToolChest) toolChest
    );

    final GroupByQuery expectedQuery = query.withOverriddenContext(
        ImmutableMap.of(GroupByQueryConfig.CTX_KEY_STRATEGY, "v1")
    );
    Sequence<ResultRow> queryResult = theRunner.run(QueryPlus.wrap(expectedQuery), ResponseContext.createEmpty());
    List<ResultRow> expectedResults = queryResult.toList();

    queryResult = theRunner2.run(QueryPlus.wrap(query), ResponseContext.createEmpty());
    List<ResultRow> results = queryResult.toList();

    Assert.assertEquals(expectedResults.size(), results.size());
    for (int i = 0; i < results.size(); i++) {
      Assert.assertEquals(StringUtils.format("%sth row", i), expectedResults.get(i), results.get(i));
    }
  }

  @Test
  public void queryMultiQueryableIndexTTFB() throws IOException
  {
    QueryToolChest<ResultRow, GroupByQuery> toolChest = FACTORY.getToolchest();
    SegmentIdMapper segmentIdMapper = new SegmentIdMapper();
    QueryRunner<ResultRow> theRunner = new FinalizeResultsQueryRunner<>(
        toolChest.mergeResults(
            FACTORY.mergeRunners2(
                executorService,
                makeMultiRunners2(segmentIdMapper),
                new DictionaryMergingQueryRunnerFactory().mergeRunners(executorService, makeDictScanRunners(segmentIdMapper))
            )
        ),
        (QueryToolChest) toolChest
    );

    Sequence<ResultRow> queryResult = theRunner.run(QueryPlus.wrap(query), ResponseContext.createEmpty());
    Yielder<ResultRow> yielder = Yielders.each(queryResult);
    yielder.close();
  }

  private List<QueryRunner<ResultRow>> makeMultiRunners(SegmentIdMapper segmentIdMapper)
  {
    List<QueryRunner<ResultRow>> runners = new ArrayList<>();
    for (int i = 0; i < NUM_SEGMENTS; i++) {
      String segmentName = "qIndex " + i;
      QueryRunner<ResultRow> runner = QueryRunnerTestHelper.makeQueryRunner(
          FACTORY,
          SegmentId.dummy(segmentName),
          new QueryableIndexSegment(QUERYABLE_INDEXES.get(i), SegmentId.dummy(segmentName)),
          "groupByRunner",
          segmentIdMapper
      );
      runners.add(FACTORY.getToolchest().preMergeQueryDecoration(runner));
    }
    return runners;
  }

  private List<SegmentGroupByQueryProcessor<ResultRow>> makeMultiRunners2(SegmentIdMapper segmentIdMapper)
  {
    List<SegmentGroupByQueryProcessor<ResultRow>> runners = new ArrayList<>();
    for (int i = 0; i < NUM_SEGMENTS; i++) {
      String segmentName = "qIndex " + i;
      SegmentGroupByQueryProcessor<ResultRow> runner = QueryRunnerTestHelper.makeQueryRunner2(
          FACTORY,
          SegmentId.dummy(segmentName),
          new QueryableIndexSegment(QUERYABLE_INDEXES.get(i), SegmentId.dummy(segmentName)),
          segmentIdMapper
      );
      runners.add(((GroupByQueryQueryToolChest) FACTORY.getToolchest()).preMergeQueryDecoration(runner));
    }
    return runners;
  }

  private List<QueryRunner<List<Iterator<DictionaryConversion>>>> makeDictScanRunners(SegmentIdMapper segmentIdMapper)
  {
    List<QueryRunner<List<Iterator<DictionaryConversion>>>> runners = new ArrayList<>();
    for (int i = 0; i < NUM_SEGMENTS; i++) {
      String segmentName = "qIndex " + i;
      QueryRunner<List<Iterator<DictionaryConversion>>> runner = QueryRunnerTestHelper.makeDictionaryScanRunner(
          SegmentId.dummy(segmentName),
          new QueryableIndexSegment(QUERYABLE_INDEXES.get(i), SegmentId.dummy(segmentName)),
          "dictionaryScanRunner",
          segmentIdMapper
      );
      runners.add(runner);
    }
    return runners;
  }
}
