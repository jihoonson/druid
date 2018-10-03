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

package org.apache.druid.query.spec;

import com.google.common.collect.Lists;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.guava.Accumulator;
import org.apache.druid.java.util.common.guava.Sequence;
import org.apache.druid.java.util.common.guava.SequenceWrapper;
import org.apache.druid.java.util.common.guava.Sequences;
import org.apache.druid.java.util.common.guava.Yielder;
import org.apache.druid.java.util.common.guava.Yielders;
import org.apache.druid.java.util.common.guava.YieldingAccumulator;
import org.apache.druid.query.Query;
import org.apache.druid.query.QueryPlus;
import org.apache.druid.query.QueryRunner;
import org.apache.druid.query.Result;
import org.apache.druid.query.SegmentDescriptor;
import org.apache.druid.segment.SegmentMissingException;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

/**
 */
public class SpecificSegmentQueryRunner<T> implements QueryRunner<T>
{
  private final QueryRunner<T> base;
  private final SpecificSegmentSpec specificSpec;

  public SpecificSegmentQueryRunner(
      QueryRunner<T> base,
      SpecificSegmentSpec specificSpec
  )
  {
    this.base = base;
    this.specificSpec = specificSpec;
  }

  @Override
  public Sequence<T> run(final QueryPlus<T> input, final Map<String, Object> responseContext)
  {
    final QueryPlus<T> queryPlus = input.withQuerySegmentSpec(specificSpec);
    final Query<T> query = queryPlus.getQuery();

    final Thread currThread = Thread.currentThread();
    final String currThreadName = currThread.getName();
    final String newName = StringUtils.format("%s_%s_%s", query.getType(), query.getDataSource(), query.getIntervals());

    final Sequence<T> baseSequence = doNamed(
        currThread, currThreadName, newName, () -> base.run(queryPlus, responseContext)
    );

    Sequence<T> segmentMissingCatchingSequence = new Sequence<T>()
    {
      @Override
      public <OutType> OutType accumulate(final Supplier<OutType> initValue, final Accumulator<OutType, T> accumulator, Supplier<Accumulator<OutType, T>> accumulatorFactory)
      {
        try {
          return baseSequence.accumulate(initValue, accumulator, accumulatorFactory);
        }
        catch (SegmentMissingException e) {
          appendMissingSegment(responseContext);
          return initValue.get();
        }
      }

      @Override
      public <OutType> Yielder<OutType> toYielder(
          final Supplier<OutType> initValue,
          YieldingAccumulator<OutType, T> statefulAccumulator,
          final Supplier<YieldingAccumulator<OutType, T>> accumulator
      )
      {
        try {
          return makeYielder(baseSequence.toYielder(initValue, statefulAccumulator, accumulator));
        }
        catch (SegmentMissingException e) {
          appendMissingSegment(responseContext);
          return Yielders.done(initValue.get(), null);
        }
      }

      private <OutType> Yielder<OutType> makeYielder(final Yielder<OutType> yielder)
      {
        return new Yielder<OutType>()
        {
          @Override
          public OutType get()
          {
            return yielder.get();
          }

          @Override
          public Yielder<OutType> next(final OutType initValue)
          {
            try {
              return yielder.next(initValue);
            }
            catch (SegmentMissingException e) {
              appendMissingSegment(responseContext);
              return Yielders.done(initValue, null);
            }
          }

          @Override
          public boolean isDone()
          {
            return yielder.isDone();
          }

          @Override
          public void close() throws IOException
          {
            yielder.close();
          }
        };
      }
    };
    return Sequences.wrap(
        segmentMissingCatchingSequence,
        new SequenceWrapper()
        {
          @Override
          public <RetType> RetType wrap(com.google.common.base.Supplier<RetType> sequenceProcessing)
          {
            return doNamed(currThread, currThreadName, newName, sequenceProcessing);
          }
        }
    );
  }

  private void appendMissingSegment(Map<String, Object> responseContext)
  {
    List<SegmentDescriptor> missingSegments = (List<SegmentDescriptor>) responseContext.get(Result.MISSING_SEGMENTS_KEY);
    if (missingSegments == null) {
      missingSegments = Lists.newArrayList();
      responseContext.put(Result.MISSING_SEGMENTS_KEY, missingSegments);
    }
    missingSegments.add(specificSpec.getDescriptor());
  }

  private <RetType> RetType doNamed(Thread currThread, String currName, String newName, com.google.common.base.Supplier<RetType> toRun)
  {
    try {
      currThread.setName(newName);
      return toRun.get();
    }
    finally {
      currThread.setName(currName);
    }
  }
}
