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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.inject.Inject;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.guava.Sequence;
import org.apache.druid.java.util.common.parsers.CloseableIterator;
import org.apache.druid.query.DictionaryMergingQueryRunner;
import org.apache.druid.query.Query;
import org.apache.druid.query.QueryPlus;
import org.apache.druid.query.QueryRunner;
import org.apache.druid.query.QueryRunnerFactory;
import org.apache.druid.query.QueryRunnerFactory2;
import org.apache.druid.query.QueryToolChest;
import org.apache.druid.query.SegmentGroupByQueryProcessor;
import org.apache.druid.query.SegmentIdMapper;
import org.apache.druid.query.context.ResponseContext;
import org.apache.druid.query.groupby.epinephelinae.GroupByShuffleMergingQueryRunner.TimestampedIterators;
import org.apache.druid.query.groupby.strategy.GroupByStrategySelector;
import org.apache.druid.segment.IdentifiableStorageAdapter;
import org.apache.druid.segment.Segment;

import javax.annotation.Nullable;
import java.util.concurrent.ExecutorService;

/**
 *
 */
public class GroupByQueryRunnerFactory implements QueryRunnerFactory<ResultRow, GroupByQuery>, QueryRunnerFactory2<ResultRow, GroupByQuery>
{
  private final GroupByStrategySelector strategySelector;
  private final GroupByQueryQueryToolChest toolChest;

  @Inject
  public GroupByQueryRunnerFactory(
      GroupByStrategySelector strategySelector,
      GroupByQueryQueryToolChest toolChest
  )
  {
    this.strategySelector = strategySelector;
    this.toolChest = toolChest;
  }

  @Override
  public QueryRunner<ResultRow> createRunner(SegmentIdMapper segmentIdMapper, final Segment segment)
  {
    return new GroupByQueryRunner(segmentIdMapper, segment, strategySelector);
  }

  @Override
  public QueryRunner<ResultRow> createRunner(Segment segment)
  {
    throw new UnsupportedOperationException();
  }

  @Override
  public SegmentGroupByQueryProcessor<ResultRow> createRunner2(SegmentIdMapper segmentIdMapper, Segment segment)
  {
    return new GroupByQueryRunner2(segmentIdMapper, segment, strategySelector);
  }

  @Override
  public QueryRunner<ResultRow> mergeRunners2(
      ExecutorService exec,
      Iterable<SegmentGroupByQueryProcessor<ResultRow>> queryRunners,
      @Nullable DictionaryMergingQueryRunner dictionaryMergingRunner
  )
  {
    // mergeRunners should take ListeningExecutorService at some point
    final ListeningExecutorService queryExecutor = MoreExecutors.listeningDecorator(exec);

    return new QueryRunner<ResultRow>()
    {
      @Override
      public Sequence<ResultRow> run(QueryPlus<ResultRow> queryPlus, ResponseContext responseContext)
      {
        QueryRunner<ResultRow> rowQueryRunner = strategySelector
            .strategize((GroupByQuery) queryPlus.getQuery())
            .mergeRunners2(queryExecutor, queryRunners, dictionaryMergingRunner);
        return rowQueryRunner.run(queryPlus, responseContext);
      }
    };
  }


  @Override
  public QueryRunner<ResultRow> mergeRunners(
      ExecutorService queryExecutor, Iterable<QueryRunner<ResultRow>> queryRunners
  )
  {
    throw new UnsupportedOperationException();
  }

  @Override
  public QueryRunner<ResultRow> mergeRunners(
      final ExecutorService exec,
      final Iterable<QueryRunner<ResultRow>> queryRunners,
      @Nullable final DictionaryMergingQueryRunner dictionaryMergingRunner
  )
  {
    // mergeRunners should take ListeningExecutorService at some point
    final ListeningExecutorService queryExecutor = MoreExecutors.listeningDecorator(exec);

    return new QueryRunner<ResultRow>()
    {
      @Override
      public Sequence<ResultRow> run(QueryPlus<ResultRow> queryPlus, ResponseContext responseContext)
      {
        QueryRunner<ResultRow> rowQueryRunner = strategySelector
            .strategize((GroupByQuery) queryPlus.getQuery())
            .mergeRunners(queryExecutor, queryRunners, dictionaryMergingRunner);
        return rowQueryRunner.run(queryPlus, responseContext);
      }
    };
  }

  @Override
  public QueryToolChest<ResultRow, GroupByQuery> getToolchest()
  {
    return toolChest;
  }

  private static class GroupByQueryRunner implements QueryRunner<ResultRow>
  {
    private final IdentifiableStorageAdapter adapter;
    private final GroupByStrategySelector strategySelector;

    public GroupByQueryRunner(
        SegmentIdMapper segmentIdMapper,
        Segment segment,
        final GroupByStrategySelector strategySelector
    )
    {
      this.adapter = new IdentifiableStorageAdapter(
          segmentIdMapper.applyAsInt(segment.getId()),
          segment.asStorageAdapter()
      );
      this.strategySelector = strategySelector;
    }

    @Override
    public Sequence<ResultRow> run(QueryPlus<ResultRow> queryPlus, ResponseContext responseContext)
    {
      Query<ResultRow> query = queryPlus.getQuery();
      if (!(query instanceof GroupByQuery)) {
        throw new ISE("Got a [%s] which isn't a %s", query.getClass(), GroupByQuery.class);
      }

      return strategySelector.strategize((GroupByQuery) query).process((GroupByQuery) query, adapter);
    }
  }

  @VisibleForTesting
  public GroupByStrategySelector getStrategySelector()
  {
    return strategySelector;
  }

  private static class GroupByQueryRunner2 implements SegmentGroupByQueryProcessor<ResultRow>
  {
    private final IdentifiableStorageAdapter adapter;
    private final GroupByStrategySelector strategySelector;

    public GroupByQueryRunner2(
        SegmentIdMapper segmentIdMapper,
        Segment segment,
        final GroupByStrategySelector strategySelector
    )
    {
      this.adapter = new IdentifiableStorageAdapter(
          segmentIdMapper.applyAsInt(segment.getId()),
          segment.asStorageAdapter()
      );
      this.strategySelector = strategySelector;
    }

    @Override
    public CloseableIterator<TimestampedIterators> process(
        QueryPlus<ResultRow> queryPlus,
        ResponseContext responseContext
    )
    {
      Query<ResultRow> query = queryPlus.getQuery();
      if (!(query instanceof GroupByQuery)) {
        throw new ISE("Got a [%s] which isn't a %s", query.getClass(), GroupByQuery.class);
      }

      return strategySelector.strategize((GroupByQuery) query).process2((GroupByQuery) query, adapter);
    }
  }
}
