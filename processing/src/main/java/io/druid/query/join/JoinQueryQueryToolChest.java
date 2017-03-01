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

import com.fasterxml.jackson.core.type.TypeReference;
import com.google.common.base.Function;
import com.metamx.emitter.service.ServiceMetricEvent.Builder;
import io.druid.data.input.Row;
import io.druid.query.QueryRunner;
import io.druid.query.QueryToolChest;
import io.druid.query.aggregation.MetricManipulationFn;

public class JoinQueryQueryToolChest extends QueryToolChest<Row, JoinQuery>
{
  @Override
  public QueryRunner<Row> mergeResults(QueryRunner<Row> runner)
  {
    return null;
  }

  @Override
  public Builder makeMetricBuilder(JoinQuery query)
  {
    return null;
  }

  @Override
  public Function<Row, Row> makePreComputeManipulatorFn(
      JoinQuery query, MetricManipulationFn fn
  )
  {
    return null;
  }

  @Override
  public TypeReference<Row> getResultTypeReference()
  {
    return null;
  }
}
