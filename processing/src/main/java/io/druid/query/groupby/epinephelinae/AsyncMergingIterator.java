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

package io.druid.query.groupby.epinephelinae;

import com.google.common.collect.Iterators;
import io.druid.query.AbstractPrioritizedCallable;

import java.util.ArrayDeque;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

public class AsyncMergingIterator<T> implements Iterator<T>
{
  private static final int MAX_BUFFER_SIZE = 1024;
  private static final int POLL_TIMEOUT_MS = 500;

  private final Iterator<T> baseIterator;
//  private final BlockingDeque<T> buffer = new LinkedBlockingDeque<>(MAX_BUFFER_SIZE);
  private final ArrayDeque<T> buffer = new ArrayDeque<>(MAX_BUFFER_SIZE);
  private final ReentrantLock lock;
  private final Condition notFull;
  private final Condition notEmpty;

  AsyncMergingIterator(
      ExecutorService exec,
      int priority,
      List<Iterator<T>> sourceIterators,
      Comparator<T> comparator
  )
  {
    this.baseIterator = Iterators.mergeSorted(sourceIterators, comparator);
    this.lock = new ReentrantLock();
    this.notFull = lock.newCondition();
    this.notEmpty = lock.newCondition();

    exec.submit(
        new AbstractPrioritizedCallable<Void>(priority)
        {

          @Override
          public Void call() throws Exception
          {
            while (baseIterator.hasNext()) {
              putInterruptibly();
            }

            return null;
          }
        }
    );
  }

  private void putInterruptibly() throws InterruptedException, TimeoutException
  {
    long nanos = TimeUnit.MILLISECONDS.toNanos(POLL_TIMEOUT_MS);
    final ReentrantLock lock = this.lock;

    lock.lockInterruptibly();
    try {
      final T next = baseIterator.next();
      while (!buffer.offer(next)) {
        if (nanos <= 0) {
          throw new TimeoutException("Cannot add more elements to buffer");
        }
        nanos = notFull.awaitNanos(nanos);
      }
      notEmpty.signal();
    }
    finally {
      lock.unlock();
    }
  }

  @Override
  public boolean hasNext()
  {
    try {
      return hasNextInterruptibly();
    }
    catch (InterruptedException e) {
      throw new RuntimeException(e);
    }
  }

  private boolean hasNextInterruptibly() throws InterruptedException
  {
    final ReentrantLock lock = this.lock;
    lock.lockInterruptibly();
    try {
      return baseIterator.hasNext() || !buffer.isEmpty();
    }
    finally {
      lock.unlock();
    }
  }

  @Override
  public T next()
  {
    if (!hasNext()) {
      throw new NoSuchElementException();
    }

    try {
     return nextInterruptibly();
    }
    catch (InterruptedException | TimeoutException e) {
      throw new RuntimeException(e);
    }
  }

  private T nextInterruptibly() throws InterruptedException, TimeoutException
  {
    long nanos = TimeUnit.MILLISECONDS.toNanos(POLL_TIMEOUT_MS);
    final ReentrantLock lock = this.lock;

    lock.lockInterruptibly();
    try {
      while (buffer.isEmpty()) {
        if (nanos <= 0) {
          throw new TimeoutException();
        }
        nanos = notEmpty.awaitNanos(nanos);
      }

      final T next = buffer.poll();
      notFull.signal();
      return next;
    }
    finally {
      lock.unlock();
    }
  }
}
