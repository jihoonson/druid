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

import it.unimi.dsi.fastutil.objects.Object2IntMap;
import it.unimi.dsi.fastutil.objects.Object2IntOpenHashMap;
import org.apache.druid.java.util.common.guava.LazySequence;
import org.apache.druid.java.util.common.guava.Sequence;
import org.apache.druid.java.util.common.guava.Sequences;
import org.apache.druid.query.context.ResponseContext;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;

public class DictionaryMergingQueryRunner implements QueryRunner<DictionaryConversion[]>
{
  private final ExecutorService exec;
  private final Iterable<QueryRunner<DictionaryConversion[]>> queryRunners;
  private final int numQueryRunners;

  public DictionaryMergingQueryRunner(
      ExecutorService exec,
      Iterable<QueryRunner<DictionaryConversion[]>> queryRunners,
      int numQueryRunners
  )
  {
    this.exec = exec;
    this.queryRunners = queryRunners;
    this.numQueryRunners = numQueryRunners;
  }

  public int getNumQueryRunners()
  {
    return numQueryRunners;
  }

  @Override
  public Sequence<DictionaryConversion[]> run(
      QueryPlus<DictionaryConversion[]> queryPlus,
      ResponseContext responseContext
  )
  {
    final DictionaryMergeQuery query = (DictionaryMergeQuery) queryPlus.getQuery();

    // TODO: i'm reading same data twice.. probably i don't need that since the dictionaries are all sorted
    // TODO: maybe it should be able to support string metrics too
    final ThreadSafeMergingDictionary[] dictionaries = new ThreadSafeMergingDictionary[query.getDimensions().size()];
    for (int i = 0; i < dictionaries.length; i++) {
      dictionaries[i] = new ThreadSafeMergingDictionary();
    }
//    final Future<ThreadSafeMergingDictionary[]> future = exec.submit(
//        () -> Sequences
//            .simple(queryRunners)
//            .flatMap(runner -> runner.run(queryPlus, responseContext))
//            .accumulate(dictionaries, (mergingDictionaries, conversions) -> {
//              for (int i = 0; i < conversions.length; i++) {
//                if (conversions[i].getOldDictionaryId() != DictionaryMergingQueryRunnerFactory.UNKNOWN_DICTIONARY_ID) {
//                  mergingDictionaries[i].add(conversions[i].getVal());
//                }
//              }
//              return mergingDictionaries;
//            })
//    );
//    try {
//      future.get();
//    }
//    catch (InterruptedException e) {
//      throw new QueryInterruptedException(e);
//    }
//    catch (ExecutionException e) {
//      throw new QueryInterruptedException(e);
//    }

    final List<Future> futures = new ArrayList<>();
    for (QueryRunner<DictionaryConversion[]> runner : queryRunners) {
      futures.add(
          exec.submit(() -> {
            runner.run(queryPlus, responseContext)
                  .accumulate(dictionaries, (mergingDictionaries, conversions) -> {
                    for (int i = 0; i < conversions.length; i++) {
                      if (conversions[i].getOldDictionaryId()
                          != DictionaryMergingQueryRunnerFactory.UNKNOWN_DICTIONARY_ID) {
                        mergingDictionaries[i].add(
                            conversions[i].getVal(),
                            conversions[i].getSegmentId(),
                            conversions[i].getOldDictionaryId()
                        );
                      }
                    }
                    return mergingDictionaries;
                  });
          })
      );
    }

    return new LazySequence<>(
        () -> {
          for (Future future : futures) {
            try {
              future.get();
            }
            catch (InterruptedException e) {
              throw new QueryInterruptedException(e);
            }
            catch (ExecutionException e) {
              throw new QueryInterruptedException(e);
            }
          }

          final Iterator<Entry<EntryKey, Integer>>[] iterators = new Iterator[dictionaries.length];
          for (int i = 0; i < iterators.length; i++) {
            iterators[i] = dictionaries[i].entries();
          }
          final IteratorsIterator iterator = new IteratorsIterator(iterators);
          return Sequences.simple(() -> iterator);

//          return Sequences.simple(queryRunners)
//                          .flatMap(runner -> runner.run(queryPlus, responseContext))
//                          .map(conversions -> {
//                            DictionaryConversion[] newConversions = new DictionaryConversion[conversions.length];
//                            for (int i = 0; i < conversions.length; i++) {
//                              newConversions[i] = new DictionaryConversion(
//                                  conversions[i].getVal(),
//                                  conversions[i].getSegmentId(),
//                                  conversions[i].getOldDictionaryId(),
//                                  dictionaries[i].get(conversions[i].getVal())
//                              );
//                            }
//                            return newConversions;
//                          });
        }
    );
  }

  private static class MergingDictionary
  {
    // TODO: size limit on dictionary?

    private final Object2IntMap<String> reverseDictionary = new Object2IntOpenHashMap<>();
    private int nextId;

    private MergingDictionary()
    {
      reverseDictionary.defaultReturnValue(DictionaryMergingQueryRunnerFactory.UNKNOWN_DICTIONARY_ID);
    }

    /**
     * Return new dictionary ID.
     */
    private int add(String val)
    {
      return reverseDictionary.computeIntIfAbsent(val, k -> nextId++);
    }

    private int get(String val)
    {
      return reverseDictionary.getInt(val);
    }
  }

  private static class ThreadSafeMergingDictionary
  {
    private final ConcurrentHashMap<String, Integer> reverseDictionary = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<EntryKey, Integer> entries = new ConcurrentHashMap<>();
    private final AtomicInteger nextId = new AtomicInteger();

    /**
     * Return new dictionary ID.
     */
    private int add(String val, int segmentId, int oldDictId)
    {
      final int newDictId = reverseDictionary.computeIfAbsent(val, k -> nextId.getAndIncrement());
      entries.putIfAbsent(new EntryKey(val, segmentId, oldDictId), newDictId);
      return newDictId;
    }

    private int get(String val)
    {
      return reverseDictionary.getOrDefault(val, DictionaryMergingQueryRunnerFactory.UNKNOWN_DICTIONARY_ID);
    }

    private Iterator<Entry<EntryKey, Integer>> entries()
    {
      return entries.entrySet().iterator();
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

  private static class IteratorsIterator implements Iterator<DictionaryConversion[]>
  {
    private final Iterator<Entry<EntryKey, Integer>>[] iterators;

    private IteratorsIterator(Iterator<Entry<EntryKey, Integer>>[] iterators)
    {
      this.iterators = iterators;
    }

    @Override
    public boolean hasNext()
    {
      for (Iterator<Entry<EntryKey, Integer>> iterator : iterators) {
        if (iterator.hasNext()) {
          return true;
        }
      }
      return false;
    }

    @Override
    public DictionaryConversion[] next()
    {
      if (!hasNext()) {
        throw new NoSuchElementException();
      }

      final DictionaryConversion[] conversions = new DictionaryConversion[iterators.length];
      for (int i = 0; i < conversions.length; i++) {
        if (iterators[i].hasNext()) {
          final Entry<EntryKey, Integer> entry = iterators[i].next();
          conversions[i] = new DictionaryConversion(
              entry.getKey().val,
              entry.getKey().segmentId,
              entry.getKey().oldDictId,
              entry.getValue()
          );
        }
      }
      return conversions;
    }
  }
}
