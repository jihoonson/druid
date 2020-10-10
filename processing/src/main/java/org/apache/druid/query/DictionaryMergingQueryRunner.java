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
import it.unimi.dsi.fastutil.objects.Object2IntMap;
import it.unimi.dsi.fastutil.objects.Object2IntOpenHashMap;
import org.apache.commons.lang3.mutable.MutableInt;
import org.apache.druid.java.util.common.concurrent.Execs;
import org.apache.druid.java.util.common.guava.LazySequence;
import org.apache.druid.java.util.common.guava.Sequence;
import org.apache.druid.java.util.common.guava.Sequences;
import org.apache.druid.query.context.ResponseContext;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;

// TODO: maybe these runners don't have to be QueryRunner
public class DictionaryMergingQueryRunner implements QueryRunner<Iterator<DictionaryConversion>>
{
  private final ExecutorService exec;
  private final Iterable<QueryRunner<Iterator<DictionaryConversion>>> queryRunners;
  private final int numQueryRunners;

  public DictionaryMergingQueryRunner(
      ExecutorService exec,
      Iterable<QueryRunner<Iterator<DictionaryConversion>>> queryRunners,
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
  public Sequence<Iterator<DictionaryConversion>> run(
      QueryPlus<Iterator<DictionaryConversion>> queryPlus,
      ResponseContext responseContext
  )
  {
    final DictionaryMergeQuery query = (DictionaryMergeQuery) queryPlus.getQuery();

    // TODO: maybe it should be able to support string metrics too

    final AtomicInteger[] nextIds = new AtomicInteger[query.getDimensions().size()];
    final ConcurrentHashMap<String, Integer>[] reverseDictionaries = new ConcurrentHashMap[query.getDimensions().size()];
    for (int i = 0; i < nextIds.length; i++) {
      nextIds[i] = new AtomicInteger();
      reverseDictionaries[i] = new ConcurrentHashMap<>();
    }

    final List<Future<ThreadSafeMergingDictionary[]>> futures = new ArrayList<>();
    for (QueryRunner<Iterator<DictionaryConversion>> runner : queryRunners) {
      final int runnerId = ((IdentifiableQueryRunner<Iterator<DictionaryConversion>>) runner).getSegmentId();
      futures.add(
          exec.submit(() -> {
            // TODO: this must preserve the ordering, so that the new dict id is ordered
            final ThreadSafeMergingDictionary[] dictionaries = new ThreadSafeMergingDictionary[query.getDimensions().size()];
            for (int i = 0; i < dictionaries.length; i++) {
              dictionaries[i] = new ThreadSafeMergingDictionary(reverseDictionaries[i], nextIds[i]);
            }
            final MutableInt dimensionPointer = new MutableInt(runnerId % query.getDimensions().size());
            runner.run(queryPlus, responseContext)
                  .accumulate(
                      dictionaries,
                      (mergingDictionaries, conversions) -> {
                        final int dimensionIndex = dimensionPointer.getAndIncrement();
                        if (dimensionPointer.getValue() == query.getDimensions().size()) {
                          dimensionPointer.setValue(0);
                        }
                        while (conversions.hasNext()) {
                          final DictionaryConversion conversion = conversions.next();
                          if (conversion.getOldDictionaryId() != DictionaryMergingQueryRunnerFactory.UNKNOWN_DICTIONARY_ID) {
                            mergingDictionaries[dimensionIndex].add(
                                conversion.getVal(),
                                conversion.getSegmentId(),
                                conversion.getOldDictionaryId()
                            );
                          }
                        }
                        return mergingDictionaries;
                      }
                  );
            return dictionaries;
          })
      );
    }

    return new LazySequence<>(
        () -> {
          final List<ThreadSafeMergingDictionary[]> dictionariesList = new ArrayList<>(futures.size());
          for (Future<ThreadSafeMergingDictionary[]> future : futures) {
            try {
              // TODO: handle timeout
              dictionariesList.add(future.get());
            }
            catch (InterruptedException e) {
              throw new QueryInterruptedException(e);
            }
            catch (ExecutionException e) {
              throw new QueryInterruptedException(e);
            }
          }

          return Sequences.simple(
              () -> new Iterator<Iterator<DictionaryConversion>>()
              {
                int dimensionIndex = 0;

                @Override
                public boolean hasNext()
                {
                  return dimensionIndex < query.getDimensions().size();
                }

                @Override
                public Iterator<DictionaryConversion> next()
                {
                  if (!hasNext()) {
                    throw new NoSuchElementException();
                  }
                  final List<Iterator<Object2IntMap.Entry<EntryKey>>> entryIteratorList = new ArrayList<>(
                      dictionariesList.size()
                  );
                  dictionariesList.forEach(
                      mergingDictionaries -> entryIteratorList.add(mergingDictionaries[dimensionIndex].entries())
                  );
                  dimensionIndex++;

                  return FluentIterable
                      .from(entryIteratorList)
                      .transformAndConcat(entryIterator -> () -> entryIterator)
                      .transform(entry -> {
                        final EntryKey key = entry.getKey();
                        final int newDictId = entry.getIntValue();
                        return new DictionaryConversion(key.val, key.segmentId, key.oldDictId, newDictId);
                      })
                      .iterator();
                }
              }
          );
        }
    );
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
