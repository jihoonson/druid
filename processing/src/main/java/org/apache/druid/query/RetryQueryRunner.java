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
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.druid.java.util.common.guava.MergeSequence;
import org.apache.druid.java.util.common.guava.MergingYielderIterator;
import org.apache.druid.java.util.common.guava.Sequence;
import org.apache.druid.java.util.common.guava.Yielder;
import org.apache.druid.java.util.common.guava.Yielders;
import org.apache.druid.java.util.common.guava.YieldingAccumulator;
import org.apache.druid.java.util.common.guava.YieldingSequenceBase;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.query.context.ResponseContext;
import org.apache.druid.segment.SegmentMissingException;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class RetryQueryRunner<T> implements QueryRunner<T>
{
  private static final Logger LOG = new Logger(RetryQueryRunner.class);

  private final QueryRunner<T> baseRunner;
  private final RetryQueryRunnerConfig config;
  private final ObjectMapper jsonMapper;

  public RetryQueryRunner(
      QueryRunner<T> baseRunner,
      RetryQueryRunnerConfig config,
      ObjectMapper jsonMapper
  )
  {
    this.baseRunner = baseRunner;
    this.config = config;
    this.jsonMapper = jsonMapper;
  }

  @Override
  public Sequence<T> run(final QueryPlus<T> queryPlus, final ResponseContext context)
  {
    final Sequence<T> baseSequence = baseRunner.run(queryPlus, context);

    return new YieldingSequenceBase<T>()
    {
      @Override
      public <OutType> Yielder<OutType> toYielder(OutType initValue, YieldingAccumulator<OutType, T> accumulator)
      {
        final List<Yielder<T>> yielders = new ArrayList<>();
        yielders.add(Yielders.each(baseSequence));

        List<SegmentDescriptor> missingSegments = getMissingSegments(context);

        if (!missingSegments.isEmpty()) {
          for (int i = 0; i < config.getNumTries() && !missingSegments.isEmpty(); i++) {
            LOG.info("[%,d] missing segments found. Retry attempt [%,d]", missingSegments.size(), i);

            context.put(ResponseContext.Key.MISSING_SEGMENTS, new ArrayList<>());
            final QueryPlus<T> retryQueryPlus = queryPlus.withQuery(
                Queries.withSpecificSegments(queryPlus.getQuery(), missingSegments)
            );
            Sequence<T> retrySequence = baseRunner.run(retryQueryPlus, context);
            yielders.add(Yielders.each(retrySequence));
            missingSegments = getMissingSegments(context);
          }

          final List<SegmentDescriptor> finalMissingSegs = getMissingSegments(context);
          if (!config.isReturnPartialResults() && !finalMissingSegs.isEmpty()) {
            throw new SegmentMissingException("No results found for segments[%s]", finalMissingSegs);
          }
        }
        return MergeSequence.makeYielder(
            new MergingYielderIterator<>(queryPlus.getQuery().getResultOrdering(), yielders),
            initValue,
            accumulator
        );
      }
    };
  }

  private List<SegmentDescriptor> getMissingSegments(final ResponseContext context)
  {
    final Object maybeMissingSegments = context.get(ResponseContext.Key.MISSING_SEGMENTS);
    if (maybeMissingSegments == null) {
      return Collections.emptyList();
    }

    return jsonMapper.convertValue(
        maybeMissingSegments,
        new TypeReference<List<SegmentDescriptor>>()
        {
        }
    );
  }
}
