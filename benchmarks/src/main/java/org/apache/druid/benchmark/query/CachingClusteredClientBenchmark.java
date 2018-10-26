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
package org.apache.druid.benchmark.query;

import com.fasterxml.jackson.databind.InjectableValues.Std;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.smile.SmileFactory;
import com.google.common.base.Preconditions;
import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Ordering;
import com.google.common.io.Files;
import org.apache.commons.io.FileUtils;
import org.apache.druid.benchmark.datagen.BenchmarkDataGenerator;
import org.apache.druid.benchmark.datagen.BenchmarkSchemaInfo;
import org.apache.druid.benchmark.datagen.BenchmarkSchemas;
import org.apache.druid.client.CachingClusteredClient;
import org.apache.druid.client.DruidServer;
import org.apache.druid.client.SimpleQueryRunner;
import org.apache.druid.client.TimelineServerView;
import org.apache.druid.client.cache.CacheConfig;
import org.apache.druid.client.cache.CachePopulatorStats;
import org.apache.druid.client.cache.ForegroundCachePopulator;
import org.apache.druid.client.cache.MapCache;
import org.apache.druid.client.selector.HighestPriorityTierSelectorStrategy;
import org.apache.druid.client.selector.QueryableDruidServer;
import org.apache.druid.client.selector.RandomServerSelectorStrategy;
import org.apache.druid.client.selector.ServerSelector;
import org.apache.druid.client.selector.TierSelectorStrategy;
import org.apache.druid.collections.BlockingPool;
import org.apache.druid.collections.DefaultBlockingPool;
import org.apache.druid.collections.NonBlockingPool;
import org.apache.druid.collections.StupidPool;
import org.apache.druid.data.input.InputRow;
import org.apache.druid.data.input.Row;
import org.apache.druid.guice.http.DruidHttpClientConfig;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.apache.druid.java.util.common.concurrent.Execs;
import org.apache.druid.java.util.common.granularity.Granularity;
import org.apache.druid.java.util.common.guava.Sequence;
import org.apache.druid.java.util.common.guava.Yielder;
import org.apache.druid.java.util.common.guava.Yielders;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.math.expr.ExprMacroTable;
import org.apache.druid.offheap.OffheapBufferGenerator;
import org.apache.druid.query.DataSource;
import org.apache.druid.query.DruidProcessingConfig;
import org.apache.druid.query.FluentQueryRunnerBuilder;
import org.apache.druid.query.MapQueryToolChestWarehouse;
import org.apache.druid.query.Query;
import org.apache.druid.query.QueryContexts;
import org.apache.druid.query.QueryPlus;
import org.apache.druid.query.QueryRunner;
import org.apache.druid.query.QueryRunnerFactory;
import org.apache.druid.query.QueryToolChest;
import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.query.aggregation.LongSumAggregatorFactory;
import org.apache.druid.query.dimension.DefaultDimensionSpec;
import org.apache.druid.query.expression.TestExprMacroTable;
import org.apache.druid.query.groupby.GroupByQuery;
import org.apache.druid.query.groupby.GroupByQueryConfig;
import org.apache.druid.query.groupby.GroupByQueryEngine;
import org.apache.druid.query.groupby.GroupByQueryQueryToolChest;
import org.apache.druid.query.groupby.GroupByQueryRunnerFactory;
import org.apache.druid.query.groupby.strategy.GroupByStrategySelector;
import org.apache.druid.query.groupby.strategy.GroupByStrategyV1;
import org.apache.druid.query.groupby.strategy.GroupByStrategyV2;
import org.apache.druid.query.spec.MultipleIntervalSegmentSpec;
import org.apache.druid.query.spec.QuerySegmentSpec;
import org.apache.druid.segment.IndexIO;
import org.apache.druid.segment.IndexMergerV9;
import org.apache.druid.segment.IndexSpec;
import org.apache.druid.segment.QueryableIndex;
import org.apache.druid.segment.column.ColumnConfig;
import org.apache.druid.segment.incremental.IncrementalIndex;
import org.apache.druid.segment.incremental.IncrementalIndexSchema;
import org.apache.druid.segment.writeout.OffHeapMemorySegmentWriteOutMediumFactory;
import org.apache.druid.server.coordination.ServerType;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.timeline.TimelineLookup;
import org.apache.druid.timeline.VersionedIntervalTimeline;
import org.apache.druid.timeline.partition.NumberedShardSpec;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;

import javax.annotation.Nullable;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

@State(Scope.Benchmark)
@Fork(value = 1, jvmArgsAppend = "-XX:+UseG1GC")
@Warmup(iterations = 15)
@Measurement(iterations = 30)
public class CachingClusteredClientBenchmark
{
  @Param({"8"})
  private int numServers;

  @Param({"100000"})
  private int rowsPerSegment;

  @Param({"all"})
  private String queryGranularity;

  @Param({"4", "2", "1"})
  private int brokerParallelMergeDegree;

  @Param({"5120", "10240", "20480"})
  private int brokerParallelMergeQueueSize;

  private static final Logger log = new Logger(CachingClusteredClientBenchmark.class);
  private static final String DATA_SOURCE = "ds";
  private static final int RNG_SEED = 9999;
  private static final IndexMergerV9 INDEX_MERGER_V9;
  private static final IndexIO INDEX_IO;
  public static final ObjectMapper JSON_MAPPER;

  private Map<DataSegment, QueryableIndex> queryableIndexes;

  private SimpleServerView serverView;
  private QueryRunnerFactory<Row, ? extends Query<Row>> factory;
  private CachingClusteredClient cachingClusteredClient;

  private BenchmarkSchemaInfo schemaInfo;
  private File tmpDir;

  private GroupByQuery query;

  static {
    JSON_MAPPER = new DefaultObjectMapper();
    JSON_MAPPER.setInjectableValues(
        new Std()
            .addValue(ExprMacroTable.class.getName(), TestExprMacroTable.INSTANCE)
            .addValue(ObjectMapper.class.getName(), JSON_MAPPER)
            .addValue(DataSegment.PruneLoadSpecHolder.class, DataSegment.PruneLoadSpecHolder.DEFAULT)
    );
    INDEX_IO = new IndexIO(
        JSON_MAPPER,
        OffHeapMemorySegmentWriteOutMediumFactory.instance(),
        new ColumnConfig()
        {
          @Override
          public int columnCacheSizeBytes()
          {
            return 0;
          }
        }
    );
    INDEX_MERGER_V9 = new IndexMergerV9(JSON_MAPPER, INDEX_IO, OffHeapMemorySegmentWriteOutMediumFactory.instance());
  }

  private void setupQuery()
  {
    BenchmarkSchemaInfo basicSchema = BenchmarkSchemas.SCHEMA_MAP.get("basic");

    QuerySegmentSpec intervalSpec = new MultipleIntervalSegmentSpec(Collections.singletonList(basicSchema.getDataInterval()));
    List<AggregatorFactory> queryAggs = new ArrayList<>();
    queryAggs.add(new LongSumAggregatorFactory(
        "sumLongSequential",
        "sumLongSequential"
    ));

    query = GroupByQuery
        .builder()
        .setDataSource(DATA_SOURCE)
        .setQuerySegmentSpec(intervalSpec)
        .setDimensions(
            new DefaultDimensionSpec("dimUniform", null),
            new DefaultDimensionSpec("dimZipf", null)
        )
        .setAggregatorSpecs(queryAggs)
        .setGranularity(Granularity.fromString(queryGranularity))
        .setContext(
            ImmutableMap.of(
                QueryContexts.BROKER_PARALLEL_MERGE_DEGREE,
                brokerParallelMergeDegree,
                QueryContexts.BROKER_PARALLEL_MERGE_QUEUE_SIZE,
                brokerParallelMergeQueueSize
            )
        )
        .build();
  }

  @Setup(Level.Trial)
  public void setup() throws IOException
  {
    final String schemaName = "basic";

    schemaInfo = BenchmarkSchemas.SCHEMA_MAP.get(schemaName);

    final BenchmarkDataGenerator dataGenerator = new BenchmarkDataGenerator(
        schemaInfo.getColumnSchemas(),
        RNG_SEED + 1,
        schemaInfo.getDataInterval(),
        rowsPerSegment
    );

    tmpDir = Files.createTempDir();
    queryableIndexes = new HashMap<>(numServers);

    for (int i = 0; i < numServers; i++) {
      final IncrementalIndex index = makeIncIndex(schemaInfo.isWithRollup());

      for (int j = 0; j < rowsPerSegment; j++) {
        final InputRow row = dataGenerator.nextRow();
        if (j % 20000 == 0) {
          log.info("%,d/%,d rows generated.", i * rowsPerSegment + j, rowsPerSegment * numServers);
        }
        index.add(row);
      }

      log.info(
          "%,d/%,d rows generated, persisting segment %d/%d.",
          (i + 1) * rowsPerSegment,
          rowsPerSegment * numServers,
          i + 1,
          numServers
      );

      final File file = INDEX_MERGER_V9.persist(
          index,
          new File(tmpDir, String.valueOf(i)),
          new IndexSpec(),
          null
      );

      final QueryableIndex queryableIndex = INDEX_IO.loadIndex(file);
      queryableIndexes.put(fromQueryableIndex(queryableIndex, file.length(), i), queryableIndex);

      index.close();
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
    final GroupByQueryConfig groupByQueryConfig = new GroupByQueryConfig();

    DruidProcessingConfig druidProcessingConfig = new DruidProcessingConfig()
    {
      @Override
      public int getNumThreads()
      {
        // Used by "v2" strategy for concurrencyHint
        return 1;
      }

      @Override
      public String getFormatString()
      {
        return null;
      }
    };

    final Supplier<GroupByQueryConfig> configSupplier = Suppliers.ofInstance(groupByQueryConfig);
    final GroupByStrategySelector strategySelector = new GroupByStrategySelector(
        configSupplier,
        new GroupByStrategyV1(
            configSupplier,
            new GroupByQueryEngine(configSupplier, bufferPool),
            QueryBenchmarkUtil.NOOP_QUERYWATCHER,
            bufferPool
        ),
        new GroupByStrategyV2(
            druidProcessingConfig,
            configSupplier,
            bufferPool,
            mergePool,
            new ObjectMapper(new SmileFactory()),
            QueryBenchmarkUtil.NOOP_QUERYWATCHER
        )
    );

    final GroupByQueryQueryToolChest queryQueryToolChest = new GroupByQueryQueryToolChest(
        strategySelector,
        QueryBenchmarkUtil.NoopIntervalChunkingQueryRunnerDecorator()
    );

    factory = new GroupByQueryRunnerFactory(
        strategySelector,
        queryQueryToolChest
    );

    serverView = new SimpleServerView();
    int serverSuffx = 1;
    for (Entry<DataSegment, QueryableIndex> entry : queryableIndexes.entrySet()) {
      serverView.addServer(
          createServer(serverSuffx++),
          entry.getKey(),
          entry.getValue()
      );
    }

    cachingClusteredClient = new CachingClusteredClient(
        new MapQueryToolChestWarehouse(
            ImmutableMap.of(GroupByQuery.class, queryQueryToolChest)
        ),
        serverView,
        MapCache.create(0),
        JSON_MAPPER,
        new ForegroundCachePopulator(JSON_MAPPER, new CachePopulatorStats(), 0),
        new CacheConfig(),
        new DruidHttpClientConfig(),
        Execs.multiThreaded(numServers, "caching-clustered-client-benchmark")
    );
  }

  @TearDown(Level.Trial)
  public void tearDown()
  {
    try {
      if (queryableIndexes != null) {
        for (QueryableIndex index : queryableIndexes.values()) {
          index.close();
        }
      }

      if (tmpDir != null) {
        FileUtils.deleteDirectory(tmpDir);
      }
    }
    catch (IOException e) {
      log.warn(e, "Failed to tear down, temp dir was: %s", tmpDir);
      throw Throwables.propagate(e);
    }
  }

  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.MICROSECONDS)
  public void queryMultiQueryableIndex(Blackhole blackhole) throws IOException
  {
    setupQuery();
    QueryToolChest<Row, Query<Row>> toolChest = (QueryToolChest<Row, Query<Row>>) factory.getToolchest();

    QueryRunner<Row> theRunner = new FluentQueryRunnerBuilder<>(toolChest)
        .create(cachingClusteredClient.getQueryRunnerForIntervals(query, query.getIntervals()))
        .applyPreMergeDecoration()
        .mergeResults()
        .applyPostMergeDecoration();

    Sequence<Row> queryResult = theRunner.run(QueryPlus.wrap(query), Maps.newHashMap());

//    Yielder<Row> yielder = queryResult.each();
//
//    while (!yielder.isDone()) {
//      final Row row = yielder.get();
//      blackhole.consume(row);
//      yielder.next(null);
//    }
//
//    yielder.close();

    List<Row> results = queryResult.toList();

    log.info("# of results: " + results.size());

    for (Row result : results) {
      blackhole.consume(result);
    }
  }

  private IncrementalIndex makeIncIndex(boolean withRollup)
  {
    return new IncrementalIndex.Builder()
        .setIndexSchema(
            new IncrementalIndexSchema.Builder()
                .withMetrics(schemaInfo.getAggsArray())
                .withRollup(withRollup)
                .build()
        )
        .setReportParseExceptions(false)
        .setConcurrentEventAdd(true)
        .setMaxRowCount(rowsPerSegment)
        .buildOnheap();
  }

  private class SimpleServerView implements TimelineServerView
  {
    private final TierSelectorStrategy tierSelectorStrategy = new HighestPriorityTierSelectorStrategy(
        new RandomServerSelectorStrategy()
    );
    // server -> queryRunner
    private final Map<DruidServer, SingleSegmentDruidServer> servers = new HashMap<>();
    // segmentId -> serverSelector
    private final Map<String, ServerSelector> selectors = new HashMap<>();
    // dataSource -> version -> serverSelector
    private final Map<String, VersionedIntervalTimeline<String, ServerSelector>> timelines = new HashMap<>();

    void addServer(DruidServer server, DataSegment dataSegment, QueryableIndex queryableIndex)
    {
      servers.put(server, new SingleSegmentDruidServer(server, new SimpleQueryRunner((QueryRunnerFactory<Row, Query<Row>>) factory, dataSegment.getIdentifier(), queryableIndex)));
      addSegmentToServer(server, dataSegment);
    }

    void addSegmentToServer(DruidServer server, DataSegment segment)
    {
      final ServerSelector selector = selectors.computeIfAbsent(segment.getIdentifier(), k -> new ServerSelector(segment, tierSelectorStrategy));
      selector.addServerAndUpdateSegment(servers.get(server), segment);
      timelines.computeIfAbsent(segment.getDataSource(), k -> new VersionedIntervalTimeline<>(Ordering.natural()))
               .add(segment.getInterval(), segment.getVersion(), segment.getShardSpec().createChunk(selector));
    }

    @Nullable
    @Override
    public TimelineLookup<String, ServerSelector> getTimeline(DataSource dataSource)
    {
      final String table = Iterables.getOnlyElement(dataSource.getNames());
      return timelines.get(table);
    }

    @Override
    public <T> QueryRunner<T> getQueryRunner(DruidServer server)
    {
      final SingleSegmentDruidServer queryableDruidServer = Preconditions.checkNotNull(servers.get(server), "server");
      return (QueryRunner<T>) queryableDruidServer.getQueryRunner();
    }

    @Override
    public void registerTimelineCallback(
        Executor exec, TimelineCallback callback
    )
    {

    }

    @Override
    public void registerServerRemovedCallback(
        Executor exec, ServerRemovedCallback callback
    )
    {

    }

    @Override
    public void registerSegmentCallback(Executor exec, SegmentCallback callback)
    {

    }
  }

  private class SingleSegmentDruidServer implements QueryableDruidServer<SimpleQueryRunner>
  {
    private final DruidServer server;
    private final SimpleQueryRunner runner;

    SingleSegmentDruidServer(DruidServer server, SimpleQueryRunner runner)
    {
      this.server = server;
      this.runner = runner;
    }

    @Override
    public DruidServer getServer()
    {
      return server;
    }

    @Override
    public SimpleQueryRunner getQueryRunner()
    {
      return runner;
    }
  }

  private static DruidServer createServer(int nameSuiffix)
  {
    return new DruidServer(
        "server_" + nameSuiffix,
        "127.0.0." + nameSuiffix,
        null,
        10240L,
        ServerType.HISTORICAL,
        "default",
        0
    );
  }

  private static DataSegment fromQueryableIndex(QueryableIndex queryableIndex, long size, int seq)
  {
    return new DataSegment(
        DATA_SOURCE,
        queryableIndex.getDataInterval(),
        "version",
        Collections.emptyMap(),
        Lists.newArrayList(queryableIndex.getAvailableDimensions().iterator()),
        Arrays.stream(queryableIndex.getMetadata().getAggregators()).map(AggregatorFactory::getName).collect(Collectors.toList()),
        new NumberedShardSpec(seq, 0),
        0,
        size
    );
  }
}
