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

package org.apache.druid.java.util.common.guava;

import com.google.common.collect.Ordering;

import java.io.IOException;

/**
 * Used to perform an n-way merge on n ordered sequences
 */
public class MergeSequence<T> extends YieldingSequenceBase<T>
{
  private final Ordering<? super T> ordering;
  private final Sequence<? extends Sequence<T>> baseSequences;

  public MergeSequence(
      Ordering<? super T> ordering,
      Sequence<? extends Sequence<? extends T>> baseSequences
  )
  {
    this.ordering = ordering;
    this.baseSequences = (Sequence<? extends Sequence<T>>) baseSequences;
  }

  @Override
  public <OutType> Yielder<OutType> toYielder(OutType initValue, YieldingAccumulator<OutType, T> accumulator)
  {
    return makeYielder(
        new MergingYielderIterator<>(ordering, baseSequences.map(Yielders::each).toList()),
        initValue,
        accumulator
    );
  }

  public static <T, OutType> Yielder<OutType> makeYielder(
      final MergingYielderIterator<T> mergingYielderIterator,
      final OutType initVal,
      final YieldingAccumulator<OutType, T> accumulator
  )
  {
    try {
      OutType retVal = initVal;
      while (!accumulator.yielded() && mergingYielderIterator.hasNext()) {
        Yielder<T> yielder = mergingYielderIterator.next();
        retVal = accumulator.accumulate(retVal, yielder.get());

        if (yielder.isDone()) {
          try {
            yielder.close();
          }
          catch (IOException e) {
            throw new RuntimeException(e);
          }
        }
      }

      if (!mergingYielderIterator.hasNext() && !accumulator.yielded()) {
        return Yielders.done(retVal, null);
      }

      final OutType yieldVal = retVal;
      return new Yielder<OutType>()
      {
        @Override
        public OutType get()
        {
          return yieldVal;
        }

        @Override
        public Yielder<OutType> next(OutType initValue)
        {
          accumulator.reset();
          return makeYielder(mergingYielderIterator, initValue, accumulator);
        }

        @Override
        public boolean isDone()
        {
          return false;
        }

        @Override
        public void close() throws IOException
        {
          mergingYielderIterator.close();
        }
      };
    }
    catch (Throwable t) {
      try {
        mergingYielderIterator.close();
      }
      catch (IOException e) {
        t.addSuppressed(e);
      }
      throw t;
    }
  }
}
