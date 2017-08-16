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

import com.google.common.collect.Ordering;
import io.druid.java.util.common.io.Closer;

import java.io.IOException;
import java.util.PriorityQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingDeque;

public class ParallelMergeSequence<T> extends YieldingSequenceBase<T>
{
  private final ExecutorService exec;
  private final Ordering<T> ordering;
  private final Sequence<? extends Sequence<T>> baseSequences;

  public ParallelMergeSequence(
      ExecutorService exec,
      Ordering<T> ordering,
      Sequence<? extends Sequence<? extends T>> baseSequences
  )
  {
    this.exec = exec;
    this.ordering = ordering;
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

    baseSequences.accumulate(
        pQueue,
        (queue, in) -> {
          final Yielder<T> yielder = new AsyncYielder<>(exec, in);

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
    private static final int MAX_BUFFER_SIZE = 1024;
    private final LinkedBlockingDeque<Yielder<T>> buffer;
    private final ExecutorService exec;
    private final Yielder<T> value;

    private final Future<Yielder<T>> fillFuture;

    AsyncYielder(ExecutorService exec, Sequence<T> baseSequence)
    {
      this.buffer = new LinkedBlockingDeque<>(MAX_BUFFER_SIZE);
      this.exec = exec;
      this.fillFuture = fillBuffer(baseSequence);
      try {
        this.value = buffer.take();
      }
      catch (InterruptedException e) {
        throw new RuntimeException(e);
      }
    }

    AsyncYielder(LinkedBlockingDeque<Yielder<T>> buffer, ExecutorService exec, Future<Yielder<T>> fillFuture)
    {
      this.buffer = buffer;
      this.exec = exec;
      if (fillFuture.isDone() && buffer.size() < MAX_BUFFER_SIZE / 2) {
        try {
          this.fillFuture = fillBuffer(fillFuture.get());
        }
        catch (InterruptedException | ExecutionException e) {
          throw new RuntimeException(e);
        }
      } else {
        this.fillFuture = fillFuture;
      }
      try {
        this.value = this.buffer.take();
      }
      catch (InterruptedException e) {
        throw new RuntimeException(e);
      }
    }

    private Future<Yielder<T>> fillBuffer(Sequence<T> baseSequence)
    {
      return exec.submit(() -> {
        Yielder<T> baseYielder = baseSequence.toYielder(
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
        );
        final int availableSpace = MAX_BUFFER_SIZE - buffer.size();
        for (int i = 0; i < availableSpace; i++) {
          try {
            if (baseYielder.isDone()) {
              buffer.put(Yielders.done(null, baseYielder));
              break;
            } else {
              buffer.put(baseYielder);
              baseYielder = baseYielder.next(null);
            }
          }
          catch (InterruptedException e) {
            throw new RuntimeException(e);
          }
        }
        return baseYielder;
      });
    }

    private Future<Yielder<T>> fillBuffer(Yielder<T> yielder)
    {
      return exec.submit(() -> {
        Yielder<T> baseYielder = yielder;
        final int availableSpace = MAX_BUFFER_SIZE - buffer.size();
        for (int i = 0; i < availableSpace; i++) {
          try {
            if (baseYielder.isDone()) {
              buffer.put(Yielders.done(null, baseYielder));
              break;
            } else {
              buffer.put(baseYielder);
              baseYielder = baseYielder.next(null);
            }
          }
          catch (InterruptedException e) {
            throw new RuntimeException(e);
          }
        }
        return baseYielder;
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
      return new AsyncYielder<>(buffer, exec, fillFuture);
    }

    @Override
    public boolean isDone()
    {
      return value.isDone();
    }

    @Override
    public void close() throws IOException
    {
      // TODO: check?
      final Closer closer = Closer.create();
      closer.register(value);
      while (!buffer.isEmpty()) {
        closer.register(buffer.poll());
      }
      closer.close();
    }
  }
}
