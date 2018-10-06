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

package org.apache.druid.timeline.partition;

import com.google.common.collect.Iterables;
import org.apache.druid.timeline.Overshadowable;

import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.Spliterator;

/**
 * An object that clumps together multiple other objects which each represent a shard of some space.
 */
public class PartitionHolder<T extends Overshadowable<T>> implements Iterable<PartitionChunk<T>>
{
//  private final TreeSet<PartitionChunk<T>> holderSet;
  private final OvershadowChecker<T> overshadowChecker;

  public PartitionHolder(OvershadowChecker<T> overshadowChecker, PartitionChunk<T> initialChunk)
  {
    this.overshadowChecker = overshadowChecker;
    add(initialChunk);
  }

  public PartitionHolder(OvershadowChecker<T> overshadowChecker, List<PartitionChunk<T>> initialChunks)
  {
    this.overshadowChecker = overshadowChecker;
    for (PartitionChunk<T> chunk : initialChunks) {
      add(chunk);
    }
  }

  public PartitionHolder(PartitionHolder<T> partitionHolder)
  {
    this.overshadowChecker = partitionHolder.overshadowChecker.copy();
  }

  public void add(PartitionChunk<T> chunk)
  {
    overshadowChecker.add(chunk);
  }

  public PartitionChunk<T> remove(PartitionChunk<T> chunk)
  {
//    if (!holderSet.isEmpty()) {
//      // Somewhat funky implementation in order to return the removed object as it exists in the set
//      SortedSet<PartitionChunk<T>> tailSet = holderSet.tailSet(chunk, true);
//      if (!tailSet.isEmpty()) {
//        PartitionChunk<T> element = tailSet.first();
//        if (chunk.equals(element)) {
//          holderSet.remove(element);
//          return element;
//        }
//      }
//    }
//    return null;
    return overshadowChecker.remove(chunk);
  }

  public boolean isEmpty()
  {
    return overshadowChecker.isEmpty();
  }

  public boolean isComplete()
  {
    if (overshadowChecker.isEmpty()) {
      return false;
    }

    Iterator<PartitionChunk<T>> iter = iterator();

    PartitionChunk<T> curr = iter.next();

    if (!curr.isStart()) {
      return false;
    }

    if (curr.isEnd()) {
      return overshadowChecker.isComplete();
    }

    while (iter.hasNext()) {
      PartitionChunk<T> next = iter.next();
      if (!curr.abuts(next)) {
        return false;
      }

      if (next.isEnd()) {
        return overshadowChecker.isComplete();
      }
      curr = next;
    }

    return false;
  }

  public PartitionChunk<T> getChunk(final int partitionNum)
  {
    return overshadowChecker.getChunk(partitionNum);
  }

  @Override
  public Iterator<PartitionChunk<T>> iterator()
  {
    return overshadowChecker.findVisibles().iterator();
  }

  @Override
  public Spliterator<PartitionChunk<T>> spliterator()
  {
    return overshadowChecker.findVisibles().spliterator();
  }

  public Iterable<T> payloads()
  {
    return Iterables.transform(this, PartitionChunk::getObject);
  }

  @Override
  public boolean equals(Object o)
  {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    PartitionHolder<?> that = (PartitionHolder<?>) o;
    return Objects.equals(overshadowChecker, that.overshadowChecker);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(overshadowChecker);
  }

  @Override
  public String toString()
  {
    return "PartitionHolder{" +
           "overshadowChecker=" + overshadowChecker +
           '}';
  }
}
