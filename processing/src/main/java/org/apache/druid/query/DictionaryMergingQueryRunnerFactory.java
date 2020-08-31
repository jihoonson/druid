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

package org.apache.druid.query;

import com.fasterxml.jackson.core.type.TypeReference;
import com.google.common.base.Function;
import com.google.common.base.Functions;
import com.google.common.collect.Iterables;
import org.apache.druid.query.aggregation.MetricManipulationFn;
import org.apache.druid.segment.Segment;

import java.util.concurrent.ExecutorService;

public class DictionaryMergingQueryRunnerFactory
    implements QueryRunnerFactory<DictionaryConversion[], Query<DictionaryConversion[]>>
{
  static final int UNKNOWN_DICTIONARY_ID = -1;

  @Override
  public QueryRunner<DictionaryConversion[]> createRunner(SegmentIdMapper segmentIdMapper, Segment segment)
  {
    return new DictionaryScanRunner(
        segmentIdMapper.applyAsInt(segment.getId()),
        segment
    );
  }

  @Override
  public QueryRunner<DictionaryConversion[]> createRunner(Segment segment)
  {
    throw new UnsupportedOperationException();
  }

  @Override
  public DictionaryMergingQueryRunner mergeRunners(
      ExecutorService queryExecutor,
      Iterable<QueryRunner<DictionaryConversion[]>> queryRunners
  )
  {
    // TODO: is this good? should the size be passed as a param?
    return new DictionaryMergingQueryRunner(queryExecutor, queryRunners, Iterables.size(queryRunners));
  }

  @Override
  public QueryToolChest<DictionaryConversion[], Query<DictionaryConversion[]>> getToolchest()
  {
    return new QueryToolChest<DictionaryConversion[], Query<DictionaryConversion[]>>()
    {
      @Override
      public QueryMetrics<? super Query<DictionaryConversion[]>> makeMetrics(Query<DictionaryConversion[]> query)
      {
        return new DefaultQueryMetrics<>();
      }

      @Override
      public Function<DictionaryConversion[], DictionaryConversion[]> makePreComputeManipulatorFn(
          Query<DictionaryConversion[]> query, MetricManipulationFn fn
      )
      {
        return Functions.identity();
      }

      @Override
      public TypeReference<DictionaryConversion[]> getResultTypeReference()
      {
        return new TypeReference<DictionaryConversion[]>()
        {
        };
      }
    };
  }
}
