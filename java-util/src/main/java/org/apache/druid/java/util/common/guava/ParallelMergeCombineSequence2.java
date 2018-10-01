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
import org.apache.druid.java.util.common.guava.BaseSequence.IteratorMaker;
import org.apache.druid.java.util.common.guava.nary.BinaryFn;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.function.Supplier;

public class ParallelMergeCombineSequence2<T> implements Sequence<T>
{
  private final ExecutorService exec;
  private final List<? extends Sequence<T>> baseSequences;
  private final Ordering<T> ordering;
  private final BinaryFn<T, T, T> mergeFn;
  private final int batchSize;

  public ParallelMergeCombineSequence2(
      ExecutorService exec,
      List<? extends Sequence<? extends T>> baseSequences,
      Ordering<T> ordering,
      BinaryFn<T, T, T> mergeFn,
      int batchSize
  )
  {
    this.exec = exec;
    this.baseSequences = (List<? extends Sequence<T>>) baseSequences;
    this.ordering = ordering;
    this.mergeFn = mergeFn;
    this.batchSize = batchSize;
  }

  @Override
  public <OutType> OutType accumulate(OutType initValue, Accumulator<OutType, T> accumulator)
  {
    return accumulate(() -> initValue, () -> accumulator);
  }

  @Override
  public <OutType> OutType accumulate(
      Supplier<OutType> initValueSupplier, Supplier<Accumulator<OutType, T>> accumulatorSupplier
  )
  {
    // TODO: validate this
    final List<Sequence<OutType>> finalSequences = new ArrayList<>();

    for (int i = 0; i < baseSequences.size(); i += batchSize) {
      final Sequence<? extends Sequence<T>> subSequences = Sequences.simple(
          baseSequences.subList(i, Math.min(i + batchSize, baseSequences.size()))
      );
      final CombiningSequence<T> combiningSequence = CombiningSequence.create(new MergeSequence<>(ordering, subSequences), ordering, mergeFn, null);
      final Future<OutType> future = exec.submit(() -> combiningSequence.accumulate(initValueSupplier, accumulatorSupplier));

      finalSequences.add(
          new LazySequence<>(
              () -> {
                try {
                  final OutType result = future.get();

                  return new BaseSequence<>(
                      new IteratorMaker<OutType, Iterator<OutType>>()
                      {
                        @Override
                        public Iterator<OutType> make()
                        {
                          return Collections.singletonList(result).iterator();
                        }

                        @Override
                        public void cleanup(Iterator<OutType> iterFromMake)
                        {

                        }
                      }
                  );
                }
                catch (InterruptedException | ExecutionException e) {
                  throw new RuntimeException(e);
                }
              }
          )
      );
    }

    return CombiningSequence.create(
        new MergeSequence<>(ordering, Sequences.fromStream(finalSequences.stream().map(seq -> (Sequence<T>) seq))),
        ordering,
        mergeFn
    ).accumulate(null, accumulatorSupplier.get());
  }

  @Override
  public <OutType> Yielder<OutType> toYielder(OutType initValue, YieldingAccumulator<OutType, T> accumulator)
  {
    return toYielder(() -> initValue, () -> accumulator);
  }

  @Override
  public <OutType> Yielder<OutType> toYielder(
      Supplier<OutType> initValueSupplier, Supplier<YieldingAccumulator<OutType, T>> yieldingAccumulatorSupplier
  )
  {
    final List<Sequence<OutType>> finalSequences = new ArrayList<>();

    for (int i = 0; i < baseSequences.size(); i += batchSize) {
      final Sequence<? extends Sequence<T>> subSequences = Sequences.simple(
          baseSequences.subList(i, Math.min(i + batchSize, baseSequences.size()))
      );
      final CombiningSequence<T> combiningSequence = CombiningSequence.create(new MergeSequence<>(ordering, subSequences), ordering, mergeFn);

      final BlockingQueue<OutType> queue = new ArrayBlockingQueue<>(10240);
      final Object sentinel = new Object();

      exec.submit(() -> {
        try {
          final OutType initVal = initValueSupplier.get();
          final YieldingAccumulator<OutType, T> yieldingAccumulator = yieldingAccumulatorSupplier.get();
          Yielder<OutType> yielder = combiningSequence.toYielder(initVal, yieldingAccumulator);
          OutType prevVal = null;
          OutType val = yielder.get();

          queue.put(val);

          while (!yielder.isDone()) {
            yielder = yielder.next(val);
            prevVal = val;
            val = yielder.get();
            if (val != prevVal) { // TODO: why are there same values?
              queue.put(val);
            }
          }
          queue.put((OutType) sentinel);
          yielder.close();
        }
        catch (InterruptedException | IOException e) {
          throw new RuntimeException(e);
        }
      });

      finalSequences.add(
          new BaseSequence<>(
              new IteratorMaker<OutType, Iterator<OutType>>()
              {
                @Override
                public Iterator<OutType> make()
                {
                  return new Iterator<OutType>()
                  {
                    private OutType nextVal;

                    @Override
                    public boolean hasNext()
                    {
                      try {
                        nextVal = queue.take();
                        return nextVal != sentinel;
                      }
                      catch (InterruptedException e) {
                        throw new RuntimeException(e);
                      }
                    }

                    @Override
                    public OutType next()
                    {
                      return nextVal;
                    }
                  };
                }

                @Override
                public void cleanup(Iterator<OutType> iterFromMake)
                {

                }
              }
          )
      );
    }

    return CombiningSequence.create(
        new MergeSequence<>(ordering, Sequences.fromStream(finalSequences.stream().map(seq -> (Sequence<T>) seq))),
        ordering,
        mergeFn
    ).toYielder(null, yieldingAccumulatorSupplier.get());
  }
}
