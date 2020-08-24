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
import it.unimi.dsi.fastutil.objects.Object2IntArrayMap;
import it.unimi.dsi.fastutil.objects.Object2IntMap;
import org.apache.druid.query.aggregation.MetricManipulationFn;
import org.apache.druid.segment.Segment;
import org.apache.druid.timeline.SegmentId;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicInteger;

public class DictionaryMergingQueryRunnerFactory
    implements QueryRunnerFactory<DictionaryConversion[], Query<DictionaryConversion[]>>
{
  static final int UNKNOWN_DICTIONARY_ID = -1;
  // TODO: this factory is singleton.. what to do with this map?
  private final Object2IntMap<SegmentId> segmentIds = new Object2IntArrayMap<>();
  private final AtomicInteger nextId = new AtomicInteger(0);

  @Override
  public QueryRunner<DictionaryConversion[]> createRunner(Segment segment)
  {
    return new DictionaryScanRunner(
        segmentIds.computeIntIfAbsent(segment.getId(), k -> nextId.getAndIncrement()),
        segment
    );
  }

  @Override
  public QueryRunner<DictionaryConversion[]> mergeRunners(
      ExecutorService queryExecutor,
      Iterable<QueryRunner<DictionaryConversion[]>> queryRunners
  )
  {
    return new DictionaryMergingQueryRunner(queryExecutor, queryRunners);
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
