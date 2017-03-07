/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Metamarkets licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.druid.query.join;

import com.google.common.base.Supplier;
import com.google.inject.Inject;
import io.druid.collections.StupidPool;
import io.druid.common.guava.SettableSupplier;
import io.druid.data.input.Row;
import io.druid.java.util.common.guava.Sequence;
import io.druid.query.Query;
import io.druid.query.QueryRunner;
import io.druid.query.QueryRunnerFactory;
import io.druid.query.QueryToolChest;
import io.druid.query.QueryWatcher;
import io.druid.segment.Segment;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;

public class JoinQueryRunnerFactory implements QueryRunnerFactory<Row, JoinQuery>
{
  private final JoinQueryQueryToolChest toolChest;
  private final JoinStrategyFactory factory;
  private final QueryWatcher queryWatcher;
  private final StupidPool<ByteBuffer> pool;
  private final SettableSupplier<Sequence<Row>> supplier = new SettableSupplier<>();

  @Inject
  public JoinQueryRunnerFactory(
      JoinQueryQueryToolChest toolChest,
      JoinStrategyFactory factory,
      QueryWatcher queryWatcher,
      StupidPool<ByteBuffer> pool
  )
  {
    this.toolChest = toolChest;
    this.factory = factory;
    this.queryWatcher = queryWatcher;
    this.pool = pool;
  }

  @Override
  public QueryRunner<Row> createRunner(Segment segment)
  {
    return new JoinQueryRunner(factory, segment, supplier, pool);
  }

  @Override
  public QueryRunner<Row> mergeRunners(
      ExecutorService queryExecutor, Iterable<QueryRunner<Row>> queryRunners
  )
  {
    // TODO: join broadcasted tables
    // TODO: cache join results in the future
    // supplier.set();

    // merge the results of query runners
    return null;
  }

  @Override
  public QueryToolChest<Row, JoinQuery> getToolchest()
  {
    return toolChest;
  }

  private static class JoinQueryRunner implements QueryRunner<Row>
  {
    private final Segment segment;
    private final Supplier<Sequence<Row>> joinedBroadcastedSources;
    private final JoinStrategyFactory factory;
    private final StupidPool<ByteBuffer> pool;

    private JoinQueryRunner(
        JoinStrategyFactory factory,
        Segment nonBroadcastSegment,
        Supplier<Sequence<Row>> joinedBroadcastedSources,
        StupidPool<ByteBuffer> pool
    )
    {
      this.factory = factory;
      this.segment = nonBroadcastSegment;
      this.joinedBroadcastedSources = joinedBroadcastedSources;
      this.pool = pool;
    }

    @Override
    public Sequence<Row> run(Query<Row> query, Map<String, Object> responseContext)
    {
      return factory.strategize(query).createEngine().process((JoinQuery)query, segment, joinedBroadcastedSources, pool);
    }
  }
}
