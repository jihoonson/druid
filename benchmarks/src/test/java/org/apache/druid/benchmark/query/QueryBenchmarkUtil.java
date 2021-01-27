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

import org.apache.druid.common.config.NullHandling;
import org.apache.druid.java.util.common.guava.Sequence;
import org.apache.druid.query.BySegmentQueryRunner;
import org.apache.druid.query.DictionaryConversion;
import org.apache.druid.query.DictionaryMergingQueryRunnerFactory;
import org.apache.druid.query.FinalizeResultsQueryRunner;
import org.apache.druid.query.GroupByQuerySegmentProcessor;
import org.apache.druid.query.IdentifiableQueryRunner;
import org.apache.druid.query.Query;
import org.apache.druid.query.QueryPlus;
import org.apache.druid.query.QueryRunner;
import org.apache.druid.query.QueryRunnerFactory;
import org.apache.druid.query.QueryRunnerFactory2;
import org.apache.druid.query.QueryToolChest;
import org.apache.druid.query.QueryWatcher;
import org.apache.druid.query.SegmentIdMapper;
import org.apache.druid.query.context.ResponseContext;
import org.apache.druid.query.groupby.epinephelinae.GroupByShuffleMergingQueryRunner.TimestampedIterators;
import org.apache.druid.query.groupby.epinephelinae.vector.TimeGranulizerIterator;
import org.apache.druid.segment.Segment;
import org.apache.druid.timeline.SegmentId;

import javax.annotation.Nullable;
import java.util.Iterator;
import java.util.List;

public class QueryBenchmarkUtil
{
  static {
    NullHandling.initializeForTests();
  }

  public static <T, QueryType extends Query<T>> QueryRunner<T> makeQueryRunner(
      QueryRunnerFactory<T, QueryType> factory,
      SegmentId segmentId,
      Segment adapter
  )
  {
    return makeQueryRunner(factory, segmentId, adapter, null);
  }

  public static <T, QueryType extends Query<T>> QueryRunner<T> makeQueryRunner(
      QueryRunnerFactory<T, QueryType> factory,
      SegmentId segmentId,
      Segment adapter,
      @Nullable SegmentIdMapper segmentIdMapper // TODO: should not be null
  )
  {
    return new FinalizeResultsQueryRunner<>(
        new BySegmentQueryRunner<>(segmentId, adapter.getDataInterval().getStart(), factory.createRunner(segmentIdMapper, adapter)),
        (QueryToolChest<T, Query<T>>) factory.getToolchest()
    );
  }

  public static <T, QueryType extends Query<T>> GroupByQuerySegmentProcessor<T> makeQueryRunner2(
      QueryRunnerFactory2<T, QueryType> factory,
      SegmentId segmentId,
      Segment adapter,
      @Nullable SegmentIdMapper segmentIdMapper // TODO: is this nullable? maybe null for topN
  )
  {
    return new GroupByQuerySegmentProcessor<T>()
    {
      @Override
      public TimeGranulizerIterator<TimestampedIterators> process(
          QueryPlus<T> queryPlus, ResponseContext responseContext
      )
      {
        return factory.createRunner2(segmentIdMapper, adapter).process(queryPlus, responseContext);
      }
    };
  }

  public static QueryRunner<List<Iterator<DictionaryConversion>>> makeDictionaryScanRunner(
      SegmentId segmentId,
      Segment adapter,
      SegmentIdMapper segmentIdMapper
  )
  {
    final DictionaryMergingQueryRunnerFactory factory = new DictionaryMergingQueryRunnerFactory();
    return new IdentifiableQueryRunner<List<Iterator<DictionaryConversion>>>()
    {
      @Override
      public int getSegmentId()
      {
        return segmentIdMapper.applyAsInt(segmentId);
      }

      @Override
      public Sequence<List<Iterator<DictionaryConversion>>> run(
          QueryPlus<List<Iterator<DictionaryConversion>>> queryPlus,
          ResponseContext responseContext
      )
      {
        return factory.createRunner(segmentIdMapper, adapter).run(queryPlus, responseContext);
      }
    };
  }


  public static final QueryWatcher NOOP_QUERYWATCHER = (query, future) -> {};
}
