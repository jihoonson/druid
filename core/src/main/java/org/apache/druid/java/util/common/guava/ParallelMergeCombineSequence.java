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
import org.apache.druid.java.util.common.Pair;
import org.apache.druid.java.util.common.guava.BaseSequence.IteratorMaker;
import org.apache.druid.java.util.common.guava.nary.BinaryFn;
import org.apache.druid.query.ParallelCombines;

import javax.annotation.Nullable;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class ParallelMergeCombineSequence<T> extends YieldingSequenceBase<T>
{
  private final ExecutorService exec;
  private final List<Sequence<T>> baseSequences;
  private final Ordering<T> ordering;
  private final BinaryFn<T, T, T> combineFn;
  private final int combineDegree;
  private final int queueSize;
  private final boolean hasTimeout;
  private final long timeoutAt;

  public ParallelMergeCombineSequence(
      ExecutorService exec,
      List<? extends Sequence<? extends T>> baseSequences,
      Ordering<T> ordering,
      BinaryFn<T, T, T> combineFn,
      int combineDegree,
      int queueSize,
      boolean hasTimeout,
      long timeout
  )
  {
    this.exec = exec;
    this.baseSequences = (List<Sequence<T>>) baseSequences;
    this.ordering = ordering;
    this.combineFn = combineFn;
    this.combineDegree = combineDegree;
    this.queueSize = queueSize;
    this.hasTimeout = hasTimeout;
    this.timeoutAt = System.currentTimeMillis() + timeout;
  }

  private void addToQueue(BlockingQueue<ValueHolder> queue, ValueHolder holder)
      throws InterruptedException
  {
    if (!hasTimeout) {
      queue.put(holder);
    } else {
      final long timeout = timeoutAt - System.currentTimeMillis();
      if (!queue.offer(holder, timeout, TimeUnit.MILLISECONDS)) {
        throw new RuntimeException(new TimeoutException());
      }
    }
  }

  @Override
  public <OutType> Yielder<OutType> toYielder(OutType initValue, YieldingAccumulator<OutType, T> accumulator)
  {
    final Pair<Sequence<T>, List<Future>> rootAndFutures = ParallelCombines.buildCombineTree(
        baseSequences,
        combineDegree,
        combineDegree,
        this::runCombine
    );

    // Ignore futures since they are handled in the generated BaseSequence.IteratorMaker.cleanup().
    return rootAndFutures.lhs.toYielder(initValue, accumulator);
  }

  private Pair<Sequence<T>, Future> runCombine(List<Sequence<T>> sequenceList)
  {
    final Sequence<? extends Sequence<T>> sequences = Sequences.simple(sequenceList);
    final CombiningSequence<T> combiningSequence = CombiningSequence.create(
        new MergeSequence<>(ordering, sequences),
        ordering,
        combineFn
    );

    final BlockingQueue<ValueHolder> queue = new ArrayBlockingQueue<>(queueSize);

    // AbstractPrioritizedCallable
    final Future future = exec.submit(() -> {
      combiningSequence.accumulate(
          queue,
          (theQueue, v) -> {
            try {
              addToQueue(theQueue, new ValueHolder(v));
            }
            catch (InterruptedException e) {
              throw new RuntimeException(e);
            }
            return theQueue;
          }
      );
      try {
        addToQueue(queue, new ValueHolder(null));
      }
      catch (InterruptedException e) {
        throw new RuntimeException(e);
      }
    });

    final Sequence<T> backgroundCombineSequence = new BaseSequence<>(
        new IteratorMaker<T, Iterator<T>>()
        {
          @Override
          public Iterator<T> make()
          {
            return new Iterator<T>()
            {
              private T nextVal;

              @Override
              public boolean hasNext()
              {
                try {
                  final ValueHolder holder;
                  if (!hasTimeout) {
                    holder = queue.take();
                  } else {
                    final long timeout = timeoutAt - System.currentTimeMillis();
                    holder = queue.poll(timeout, TimeUnit.MILLISECONDS);
                  }

                  if (holder == null) {
                    throw new RuntimeException(new TimeoutException());
                  }
                  nextVal = holder.val;
                  return nextVal != null;
                }
                catch (InterruptedException e) {
                  throw new RuntimeException(e);
                }
              }

              @Override
              public T next()
              {
                return nextVal;
              }
            };
          }

          @Override
          public void cleanup(Iterator<T> iterFromMake)
          {
            try {
              if (future.isDone()) {
                future.get();
              } else {
                future.cancel(true);
              }
            }
            catch (InterruptedException | ExecutionException e) {
              throw new RuntimeException(e);
            }
          }
        }
    );

    return Pair.of(backgroundCombineSequence, future);
  }

  private class ValueHolder
  {
    @Nullable
    private final T val;

    private ValueHolder(@Nullable T val)
    {
      this.val = val;
    }
  }
}
