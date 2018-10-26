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
package org.apache.druid.client;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.apache.druid.client.cache.CacheConfig;
import org.apache.druid.client.cache.CachePopulatorStats;
import org.apache.druid.client.cache.ForegroundCachePopulator;
import org.apache.druid.client.cache.MapCache;
import org.apache.druid.collections.CloseableStupidPool;
import org.apache.druid.data.input.MapBasedRow;
import org.apache.druid.data.input.Row;
import org.apache.druid.guice.http.DruidHttpClientConfig;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.concurrent.Execs;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.java.util.common.guava.Sequence;
import org.apache.druid.query.DefaultGenericQueryMetricsFactory;
import org.apache.druid.query.DefaultQueryRunnerFactoryConglomerate;
import org.apache.druid.query.DruidProcessingConfig;
import org.apache.druid.query.FluentQueryRunnerBuilder;
import org.apache.druid.query.Query;
import org.apache.druid.query.QueryPlus;
import org.apache.druid.query.QueryRunner;
import org.apache.druid.query.QueryRunnerFactory;
import org.apache.druid.query.QueryRunnerFactoryConglomerate;
import org.apache.druid.query.QueryRunnerTestHelper;
import org.apache.druid.query.QueryToolChest;
import org.apache.druid.query.QueryToolChestWarehouse;
import org.apache.druid.query.TableDataSource;
import org.apache.druid.query.aggregation.DoubleMaxAggregatorFactory;
import org.apache.druid.query.aggregation.LongSumAggregatorFactory;
import org.apache.druid.query.dimension.DefaultDimensionSpec;
import org.apache.druid.query.groupby.GroupByQuery;
import org.apache.druid.query.groupby.GroupByQueryConfig;
import org.apache.druid.query.groupby.GroupByQueryRunnerTest;
import org.apache.druid.query.groupby.strategy.GroupByStrategySelector;
import org.apache.druid.query.metadata.SegmentMetadataQueryConfig;
import org.apache.druid.query.metadata.SegmentMetadataQueryQueryToolChest;
import org.apache.druid.query.metadata.SegmentMetadataQueryRunnerFactory;
import org.apache.druid.query.metadata.metadata.SegmentMetadataQuery;
import org.apache.druid.query.scan.ScanQuery;
import org.apache.druid.query.scan.ScanQueryConfig;
import org.apache.druid.query.scan.ScanQueryEngine;
import org.apache.druid.query.scan.ScanQueryQueryToolChest;
import org.apache.druid.query.scan.ScanQueryRunnerFactory;
import org.apache.druid.query.select.SelectQuery;
import org.apache.druid.query.select.SelectQueryConfig;
import org.apache.druid.query.select.SelectQueryEngine;
import org.apache.druid.query.select.SelectQueryQueryToolChest;
import org.apache.druid.query.select.SelectQueryRunnerFactory;
import org.apache.druid.query.spec.MultipleIntervalSegmentSpec;
import org.apache.druid.query.timeseries.TimeseriesQuery;
import org.apache.druid.query.timeseries.TimeseriesQueryEngine;
import org.apache.druid.query.timeseries.TimeseriesQueryQueryToolChest;
import org.apache.druid.query.timeseries.TimeseriesQueryRunnerFactory;
import org.apache.druid.query.topn.TopNQuery;
import org.apache.druid.query.topn.TopNQueryConfig;
import org.apache.druid.query.topn.TopNQueryQueryToolChest;
import org.apache.druid.query.topn.TopNQueryRunnerFactory;
import org.apache.druid.segment.TestHelper;
import org.apache.druid.segment.VirtualColumns;
import org.apache.druid.server.coordination.ServerType;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.timeline.partition.NumberedShardSpec;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;

public class CachingClusteredClientParallelMergeTest
{
  private static final String DATA_SOURCE = "test";
  private static final String VERSION = "version";
  private static final int NUM_SERVERS = 3;

  private final Random random = new Random(9999);

  private CachingClusteredClient client;
  private QueryToolChestWarehouse toolChestWarehouse;

  private static final int DIM1_CARD = 10;
  private static final int DIM2_CARD = 1;

  @Before
  public void setup()
  {
    final QueryRunnerFactoryConglomerate conglomerate = new DefaultQueryRunnerFactoryConglomerate(
        ImmutableMap.<Class<? extends Query>, QueryRunnerFactory>builder()
            .put(
                SegmentMetadataQuery.class,
                new SegmentMetadataQueryRunnerFactory(
                    new SegmentMetadataQueryQueryToolChest(
                        new SegmentMetadataQueryConfig("P1W")
                    ),
                    QueryRunnerTestHelper.NOOP_QUERYWATCHER
                )
            )
            .put(
                ScanQuery.class,
                new ScanQueryRunnerFactory(
                    new ScanQueryQueryToolChest(
                        new ScanQueryConfig(),
                        new DefaultGenericQueryMetricsFactory(TestHelper.makeJsonMapper())
                    ),
                    new ScanQueryEngine()
                )
            )
            .put(
                SelectQuery.class,
                new SelectQueryRunnerFactory(
                    new SelectQueryQueryToolChest(
                        TestHelper.makeJsonMapper(),
                        QueryRunnerTestHelper.NoopIntervalChunkingQueryRunnerDecorator(),
                        Suppliers.ofInstance(
                            new SelectQueryConfig(true)
                        )
                    ),
                    new SelectQueryEngine(),
                    QueryRunnerTestHelper.NOOP_QUERYWATCHER
                )
            )
            .put(
                TimeseriesQuery.class,
                new TimeseriesQueryRunnerFactory(
                    new TimeseriesQueryQueryToolChest(
                        QueryRunnerTestHelper.NoopIntervalChunkingQueryRunnerDecorator()
                    ),
                    new TimeseriesQueryEngine(),
                    QueryRunnerTestHelper.NOOP_QUERYWATCHER
                )
            )
            .put(
                TopNQuery.class,
                new TopNQueryRunnerFactory(
                    new CloseableStupidPool<>(
                        "TopNQueryRunnerFactory-bufferPool",
                        new Supplier<ByteBuffer>()
                        {
                          @Override
                          public ByteBuffer get()
                          {
                            return ByteBuffer.allocate(10 * 1024 * 1024);
                          }
                        }
                    ),
                    new TopNQueryQueryToolChest(
                        new TopNQueryConfig(),
                        QueryRunnerTestHelper.NoopIntervalChunkingQueryRunnerDecorator()
                    ),
                    QueryRunnerTestHelper.NOOP_QUERYWATCHER
                )
            )
            .put(
                GroupByQuery.class,
                GroupByQueryRunnerTest
                    .makeQueryRunnerFactory(
                        GroupByQueryRunnerTest.DEFAULT_MAPPER,
                        new GroupByQueryConfig()
                        {
                          @Override
                          public String getDefaultStrategy()
                          {
                            return GroupByStrategySelector.STRATEGY_V2;
                          }
                        },
                        new DruidProcessingConfig()
                        {
                          @Override
                          public String getFormatString()
                          {
                            return null;
                          }

                          @Override
                          public int intermediateComputeSizeBytes()
                          {
                            return 10 * 1024 * 1024;
                          }

                          @Override
                          public int getNumMergeBuffers()
                          {
                            // Need 3 buffers for CalciteQueryTest.testDoubleNestedGroupby.
                            // Two buffers for the broker and one for the queryable
                            return 3;
                          }
                        }
                    ).lhs
            )
            .build()
    );

    toolChestWarehouse = new QueryToolChestWarehouse()
    {
      @Override
      public <T, QueryType extends Query<T>> QueryToolChest<T, QueryType> getToolChest(final QueryType query)
      {
        return conglomerate.findFactory(query).getToolchest();
      }
    };

    final TestTimelineServerView serverView = new TestTimelineServerView();

    for (int i = 0; i < NUM_SERVERS; i++) {
      final int numRows = random.nextInt(10000) + 10000;
//      final int numRows = 2;
      final List<Row> rows = new ArrayList<>(numRows);
      for (int j = 0; j < numRows; j++) {
        final Row row = createRow(
            "2018-01-01",
            StringUtils.format("dim1_%d", random.nextInt(DIM1_CARD)),
            StringUtils.format("dim2_%d", random.nextInt(DIM2_CARD)),
            random.nextInt(100),
            random.nextDouble()
        );
        rows.add(row);
      }
      serverView.addServer(createServer(i + 1), new TestQueryRunner(rows));
    }

//    serverView.addServer(
//        createServer(1),
//        new TestQueryRunner(
//            ImmutableList.of(
//                createRow("2018-01-01", "dim1_1", "dim2_1", 2, 0.1),
//                createRow("2018-01-02", "dim1_1", "dim2_1", 5, 0.3)
//            )
//        )
//    );
//    serverView.addServer(
//        createServer(2),
//        new TestQueryRunner(
//            ImmutableList.of(
//                createRow("2018-01-01", "dim1_1", "dim2_1", 1, 0.9),
//                createRow("2018-01-03", "dim1_1", "dim2_1", 4, 0.3),
//                createRow("2018-01-01", "dim1_2", "dim2_1", 4, 1.2)
//            )
//        )
//    );
//    serverView.addServer(
//        createServer(3),
//        new TestQueryRunner(
//            ImmutableList.of(
//                createRow("2018-01-01", "dim1_1", "dim2_1", 2, 10.0),
//                createRow("2018-01-02", "dim1_1", "dim2_1", 1, 0.1),
//                createRow("2018-01-03", "dim1_1", "dim2_1", 6, 0.3),
//                createRow("2018-01-01", "dim1_2", "dim2_1", 1, 2.2)
//            )
//        )
//    );

    serverView.addSegmentToServer(
        createServer(1),
        createSegment("2018-01-01/2018-01-07", 0)
    );
    serverView.addSegmentToServer(
        createServer(2),
        createSegment("2018-01-01/2018-01-07", 1)
    );
    serverView.addSegmentToServer(
        createServer(3),
        createSegment("2018-01-01/2018-01-07", 2)
    );

    final ObjectMapper objectMapper = new DefaultObjectMapper();
    client = new CachingClusteredClient(
        toolChestWarehouse,
        serverView,
        MapCache.create(1024),
        objectMapper,
        new ForegroundCachePopulator(objectMapper, new CachePopulatorStats(), 1024),
        new CacheConfig(),
        new DruidHttpClientConfig(),
        Execs.multiThreaded(2, "caching-clustered-client-parallel-merge-test")
    );
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

  private static Row createRow(String timestamp, String dim1, String dim2, long cnt, double doubleMet)
  {
    return new MapBasedRow(
        DateTimes.of(timestamp),
        ImmutableMap.of("dim1", dim1, "dim2", dim2, "cnt", cnt, "double_max", doubleMet)
    );
  }

  private static DataSegment createSegment(String interval, int partitionNum)
  {
    return new DataSegment(
        DATA_SOURCE,
        Intervals.of(interval),
        VERSION,
        Collections.emptyMap(),
        ImmutableList.of("dim1", "dim2"),
        ImmutableList.of("cnt", "double_met"),
        new NumberedShardSpec(partitionNum, 0),
        0,
        1024L
    );
  }

  private boolean sameDim(Row row1, Row row2)
  {
    for (int i = 0; i < DIM1_CARD; i++) {
      final String dim = StringUtils.format("dim1_%d", i);
      if (!row1.getDimension(dim).equals(row2.getDimension(dim))) {
        return false;
      }
    }

    for (int i = 0; i < DIM2_CARD; i++) {
      final String dim = StringUtils.format("dim2_%d", i);
      if (!row1.getDimension(dim).equals(row2.getDimension(dim))) {
        return false;
      }
    }

    return true;
  }

  private Set<String> getDims(Row row)
  {
    final Set<String> dims = new HashSet<>();
    dims.add(row.getDimension("dim1").get(0));
    dims.add(row.getDimension("dim2").get(0));

    return dims;
  }

  @Test
  public void test()
  {
    final GroupByQuery query = new GroupByQuery(
        new TableDataSource(DATA_SOURCE),
        new MultipleIntervalSegmentSpec(Collections.singletonList(Intervals.of("2018-01-01/2018-01-07"))),
        VirtualColumns.EMPTY,
        null,
        Granularities.ALL,
        ImmutableList.of(new DefaultDimensionSpec("dim1", "dim1"), new DefaultDimensionSpec("dim2", "dim2")),
        ImmutableList.of(
            new LongSumAggregatorFactory("cnt", "cnt"),
            new DoubleMaxAggregatorFactory("double_max", "double_met")
        ),
        null,
        null,
        null,
        null,
        ImmutableMap.of("intermediateMergeBatchThreshold", 2)
//        Collections.emptyMap()
    );
    final QueryRunner<Row> queryRunner = client.getQueryRunnerForIntervals(
        query,
        Collections.singletonList(Intervals.of("2018-01-01/2018-01-07"))
    );

    final Sequence<Row> result = new FluentQueryRunnerBuilder<>(toolChestWarehouse.getToolChest(query))
        .create(queryRunner)
        .applyPreMergeDecoration()
        .mergeResults()
        .applyPostMergeDecoration()
        .run(QueryPlus.wrap(query), new HashMap<>());

    final GroupByQuery expectedQuery = new GroupByQuery(
        new TableDataSource(DATA_SOURCE),
        new MultipleIntervalSegmentSpec(Collections.singletonList(Intervals.of("2018-01-01/2018-01-07"))),
        VirtualColumns.EMPTY,
        null,
        Granularities.ALL,
        ImmutableList.of(new DefaultDimensionSpec("dim1", "dim1"), new DefaultDimensionSpec("dim2", "dim2")),
        ImmutableList.of(
            new LongSumAggregatorFactory("cnt", "cnt"),
            new DoubleMaxAggregatorFactory("double_max", "double_met")
        ),
        null,
        null,
        null,
        null,
        Collections.emptyMap()
    );

    final Sequence<Row> expected = new FluentQueryRunnerBuilder<>(toolChestWarehouse.getToolChest(expectedQuery))
        .create(queryRunner)
        .applyPreMergeDecoration()
        .mergeResults()
        .applyPostMergeDecoration()
        .run(QueryPlus.wrap(query), new HashMap<>());

//    final List<Row> rows = result.toList();

    Assert.assertEquals(expected.toList(), result.toList());

//    final List<Row> expected = new ArrayList<>();
//    expected.add(
//        new MapBasedRow(
//            DateTimes.of("2018-01-01T00:00:00.000Z"),
//            ImmutableMap.of(
//                "dim1",
//                "dim1_1",
//                "dim2",
//                "dim2_1",
//                "cnt",
//                21L,
//                "double_max",
//                10.0
//            )
//        )
//    );
//
//    expected.add(
//        new MapBasedRow(
//            DateTimes.of("2018-01-01T00:00:00.000Z"),
//            ImmutableMap.of(
//                "dim1",
//                "dim1_2",
//                "dim2",
//                "dim2_1",
//                "cnt",
//                5L,
//                "double_max",
//                2.2
//            )
//        )
//    );
//
//      Assert.assertEquals(expected, result.toList());
  }
}
