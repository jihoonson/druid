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

package io.druid.java.util.common.guava;

import com.google.common.base.Throwables;
import com.google.common.collect.Ordering;

import java.io.IOException;
import java.util.List;
import java.util.PriorityQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingDeque;

public class ParallelMergeSequence<T> extends YieldingSequenceBase<T>
{
  private final ExecutorService exec;
  private final Ordering<T> ordering;
//  private final List<Sequence<T>> baseSequences;
  private final Sequence<? extends Sequence<T>> baseSequences;

  public ParallelMergeSequence(
      ExecutorService exec,
      Ordering<T> ordering,
//      List<Sequence<T>> baseSequences
      Sequence<? extends Sequence<? extends T>> baseSequences
  )
  {
    this.exec = exec;
    this.ordering = ordering;
//    this.baseSequences = baseSequences;
    this.baseSequences = (Sequence<? extends Sequence<T>>) baseSequences;
  }

  @Override
  public <OutType> Yielder<OutType> toYielder(
      OutType initValue, YieldingAccumulator<OutType, T> accumulator
  )
  {
    final PriorityQueue<Yielder<T>> pQueue = new PriorityQueue<>(
        32,
        ordering.onResultOf(Yielder::get)
    );

//    baseSequences.forEach(
//        sequence -> {
//          final Yielder<T> yielder = new AsyncYielder<>(
//              exec,
//              sequence.toYielder(
//                  null,
//                  new YieldingAccumulator<T, T>()
//                  {
//                    @Override
//                    public T accumulate(T accumulated, T in)
//                    {
//                      yield();
//                      return in;
//                    }
//                  }
//              )
//          );
//
//          if (!yielder.isDone()) {
//            pQueue.add(yielder);
//          } else {
//            try {
//              yielder.close();
//            }
//            catch (IOException e) {
//              throw new RuntimeException(e);
//            }
//          }
//        }
//    );

    baseSequences.accumulate(
        pQueue,
        (queue, in) -> {
          final Yielder<T> yielder = new AsyncYielder<T>(
            exec,
            in.toYielder(
                null,
                new YieldingAccumulator<T, T>()
                {
                  @Override
                  public T accumulate(T accumulated, T in)
                  {
                    yield();
                    return in;
                  }
                }
            )
          );

          if (!yielder.isDone()) {
            queue.add(yielder);
          } else {
            try {
              yielder.close();
            }
            catch (IOException e) {
              throw new RuntimeException(e);
            }
          }

          return queue;
        }
    );

    return MergeSequence.makeYielder(pQueue, initValue, accumulator);
  }

  private static class AsyncYielder<T> implements Yielder<T>
  {
    private final LinkedBlockingDeque<Yielder<T>> buffer;
    private final ExecutorService exec;
    private final Yielder<T> value;

    AsyncYielder(ExecutorService exec, Yielder<T> baseYielder)
    {
      this.buffer = new LinkedBlockingDeque<>();
      this.exec = exec;
      fillBuffer(baseYielder);
      try {
        this.value = buffer.take();
      }
      catch (InterruptedException e) {
        throw new RuntimeException(e);
      }
    }

    AsyncYielder(LinkedBlockingDeque<Yielder<T>> buffer, ExecutorService exec)
    {
      this.buffer = buffer;
      this.exec = exec;
      try {
        this.value = this.buffer.take();
      }
      catch (InterruptedException e) {
        throw new RuntimeException(e);
      }
    }

    private void fillBuffer(Yielder<T> yielder)
    {
      exec.submit(() -> {
        Yielder<T> baseYielder = yielder;
        while (!baseYielder.isDone()) {
          try {
            buffer.put(baseYielder);
            baseYielder = baseYielder.next(null);
          }
          catch (InterruptedException e) {
            throw new RuntimeException(e);
          }
          finally {
            try {
              baseYielder.close();
            }
            catch (IOException e) {
              throw Throwables.propagate(e);
            }
          }
        }

        try {
          buffer.put(Yielders.done(null, baseYielder));
        }
        catch (InterruptedException e) {
          throw new RuntimeException(e);
        }
      });
    }

    @Override
    public T get()
    {
      return value.get();
    }

    @Override
    public Yielder<T> next(T initValue)
    {
      return new AsyncYielder<>(buffer, exec);
    }

    @Override
    public boolean isDone()
    {
      return value.isDone();
    }

    @Override
    public void close() throws IOException
    {
    }
  }
}
