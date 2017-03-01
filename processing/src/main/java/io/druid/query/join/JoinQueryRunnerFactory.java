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

import com.google.inject.Inject;
import io.druid.data.input.Row;
import io.druid.java.util.common.guava.Sequence;
import io.druid.query.Query;
import io.druid.query.QueryRunner;
import io.druid.query.QueryRunnerFactory;
import io.druid.query.QueryToolChest;
import io.druid.query.QueryWatcher;
import io.druid.segment.Segment;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;

public class JoinQueryRunnerFactory implements QueryRunnerFactory<Row, JoinQuery>
{
  private final JoinQueryQueryToolChest toolChest;
  private final JoinQueryEngine engine;
  private final QueryWatcher queryWatcher;
  private Map<Object, List<Row>> hashTable;

  @Inject
  public JoinQueryRunnerFactory(
      JoinQueryQueryToolChest toolChest,
      JoinQueryEngine engine,
      QueryWatcher queryWatcher
  )
  {
    this.toolChest = toolChest;
    this.engine = engine;
    this.queryWatcher = queryWatcher;
  }

  public Object prepareSharedResources(Query<Row> query)
  {
    return null;
  }

  @Override
  public QueryRunner<Row> createRunner(Segment segment)
  {
    return new JoinQueryRunner(engine, hashTable, segment);
  }

  @Override
  public QueryRunner<Row> mergeRunners(
      ExecutorService queryExecutor, Iterable<QueryRunner<Row>> queryRunners
  )
  {
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
    private final Map<Object, List<Row>> hashTable;
    private final JoinQueryEngine engine;

    private JoinQueryRunner(JoinQueryEngine engine, Map<Object, List<Row>> hashTable, Segment segment)
    {
      this.engine = engine;
      this.hashTable = hashTable;
      this.segment = segment;
    }

    @Override
    public Sequence<Row> run(Query<Row> query, Map<String, Object> responseContext)
    {
      return engine.process((JoinQuery)query, hashTable, segment);
    }
  }
}
