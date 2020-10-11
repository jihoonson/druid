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

import com.google.common.collect.FluentIterable;
import com.google.common.collect.Iterators;
import it.unimi.dsi.fastutil.objects.Object2IntMap;
import it.unimi.dsi.fastutil.objects.Object2IntOpenHashMap;
import org.apache.druid.java.util.common.guava.Sequence;
import org.apache.druid.java.util.common.guava.Sequences;
import org.apache.druid.query.context.ResponseContext;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicInteger;

// TODO: maybe these runners don't have to be QueryRunner
public class DictionaryMergingQueryRunner implements QueryRunner<List<Iterator<DictionaryConversion>>>
{
  private final ExecutorService exec;
  private final Iterable<QueryRunner<List<Iterator<DictionaryConversion>>>> queryRunners;
  private final int numQueryRunners;

  public DictionaryMergingQueryRunner(
      ExecutorService exec,
      Iterable<QueryRunner<List<Iterator<DictionaryConversion>>>> queryRunners,
      int numQueryRunners
  )
  {
    this.exec = exec;
//    this.exec = Execs.multiThreaded(2, "test-%d");
//    this.exec = Execs.singleThreaded("dict-merge");
    this.queryRunners = queryRunners;
    this.numQueryRunners = numQueryRunners;
  }

  public int getNumQueryRunners()
  {
    return numQueryRunners;
  }

  @Override
  public Sequence<List<Iterator<DictionaryConversion>>> run(
      QueryPlus<List<Iterator<DictionaryConversion>>> queryPlus,
      ResponseContext responseContext
  )
  {
    final DictionaryMergeQuery query = (DictionaryMergeQuery) queryPlus.getQuery();

    // TODO: maybe it should be able to support string metrics too

    final List<QueryRunner<List<Iterator<DictionaryConversion>>>> runnerList = FluentIterable.from(queryRunners)
                                                                                             .toList();
    // queryRunner -> dimension -> iterator
    final List<List<Iterator<DictionaryConversion>>> iteratorsFromRunners = new ArrayList<>(runnerList.size());
    for (QueryRunner<List<Iterator<DictionaryConversion>>> runner : runnerList) {
      // TODO: do not use get(0)
      iteratorsFromRunners.add(runner.run(queryPlus, responseContext).toList().get(0));
    }

    // TODO Maybe parallelize..?
    final List<Iterator<DictionaryConversion>> mergeIterators = new ArrayList<>(query.getDimensions().size());
    for (int i = 0; i < query.getDimensions().size(); i++) {
      final int dimIndex = i;
      mergeIterators.add(
          Iterators.mergeSorted(
              () -> new Iterator<Iterator<? extends DictionaryConversion>>()
              {
                Iterator<List<Iterator<DictionaryConversion>>> innerIterator = iteratorsFromRunners.iterator();

                @Override
                public boolean hasNext()
                {
                  return innerIterator.hasNext();
                }

                @Override
                public Iterator<? extends DictionaryConversion> next()
                {
                  if (!hasNext()) {
                    throw new NoSuchElementException();
                  }
                  return innerIterator.next().get(dimIndex);
                }
              },
              Comparator.comparing(DictionaryConversion::getVal)
          )
      );
    }

    return Sequences.simple(
        () -> new Iterator<List<Iterator<DictionaryConversion>>>()
        {
          boolean returned = false;

          @Override
          public boolean hasNext()
          {
            return !returned;
          }

          @Override
          public List<Iterator<DictionaryConversion>> next()
          {
            if (!hasNext()) {
              throw new NoSuchElementException();
            }
            returned = true;
            return mergeIterators;
          }
        }
    );
  }

  private static class MergingDictionary
  {
    private final Object2IntMap<String> reverseDictionary = new Object2IntOpenHashMap<>();
    private final Object2IntMap<EntryKey> entries = new Object2IntOpenHashMap<>();
    private int nextId;

    private void add(String val, int segmentId, int oldDictId)
    {
      if (val != null) {
        final int newDictId = reverseDictionary.computeIfAbsent(val, k -> nextId++);
        entries.putIfAbsent(new EntryKey(val, segmentId, oldDictId), newDictId);
      }
    }

    private Iterator<Object2IntMap.Entry<EntryKey>> entries()
    {
      return entries.object2IntEntrySet().iterator();
    }
  }

  private static class ThreadSafeMergingDictionary
  {
    private final ConcurrentHashMap<String, Integer> reverseDictionary;
    private final Object2IntMap<EntryKey> entries = new Object2IntOpenHashMap<>();
    private final AtomicInteger nextId;

    private ThreadSafeMergingDictionary(ConcurrentHashMap<String, Integer> reverseDictionary, AtomicInteger nextId)
    {
      this.reverseDictionary = reverseDictionary;
      this.nextId = nextId;
    }

    /**
     * Return new dictionary ID.
     */
    private void add(String val, int segmentId, int oldDictId)
    {
      if (val != null) {
        final int newDictId = reverseDictionary.computeIfAbsent(val, k -> nextId.getAndIncrement());
        entries.putIfAbsent(new EntryKey(val, segmentId, oldDictId), newDictId);
      }
    }

    private Iterator<Object2IntMap.Entry<EntryKey>> entries()
    {
      return entries.object2IntEntrySet().iterator();
    }
  }

  private static class EntryKey
  {
    private final String val;
    private final int segmentId;
    private final int oldDictId;

    private EntryKey(String val, int segmentId, int oldDictId)
    {
      this.val = val;
      this.segmentId = segmentId;
      this.oldDictId = oldDictId;
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
      EntryKey entryKey = (EntryKey) o;
      return segmentId == entryKey.segmentId &&
             oldDictId == entryKey.oldDictId &&
             Objects.equals(val, entryKey.val);
    }

    @Override
    public int hashCode()
    {
      return Objects.hash(val, segmentId, oldDictId);
    }
  }
}
