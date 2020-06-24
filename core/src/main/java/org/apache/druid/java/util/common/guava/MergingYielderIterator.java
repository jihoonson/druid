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
import org.apache.druid.java.util.common.io.Closer;
import org.apache.druid.java.util.common.parsers.CloseableIterator;

import java.io.IOException;
import java.util.Collection;
import java.util.NoSuchElementException;
import java.util.PriorityQueue;


/**
 * Note: the yielder for MergeSequence returns elements from the priority queue in order of increasing priority.
 * This is due to the fact that PriorityQueue#remove() polls from the head of the queue which is, according to
 * the PriorityQueue javadoc, "the least element with respect to the specified ordering"
 */
public class MergingYielderIterator<T> implements CloseableIterator<Yielder<T>>
{
  private final PriorityQueue<Yielder<T>> queue;

  public MergingYielderIterator(Ordering<? super T> ordering, Collection<Yielder<T>> yielders)
  {
    try {
      queue = new PriorityQueue<>(32, ordering.onResultOf(Yielder::get));
      yielders.forEach(this::addOrCloseYielder);
    }
    catch (Throwable t) {
      final Closer closer = Closer.create();
      closer.registerAll(yielders);
      try {
        closer.close();
      }
      catch (IOException e) {
        t.addSuppressed(e);
      }
      throw t;
    }
  }

  private void addOrCloseYielder(Yielder<T> yielder)
  {
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
  }

  @Override
  public boolean hasNext()
  {
    return !queue.isEmpty();
  }

  @Override
  public Yielder<T> next()
  {
    if (!hasNext()) {
      throw new NoSuchElementException();
    }
    final Yielder<T> retVal = queue.remove();
    if (!retVal.isDone()) {
      final Yielder<T> next = retVal.next(null);  // TODO: init value?
      addOrCloseYielder(next);
    }
    return retVal;
  }

  @Override
  public void close() throws IOException
  {
    final Closer closer = Closer.create();
    closer.registerAll(queue);
    closer.close();
  }
}
