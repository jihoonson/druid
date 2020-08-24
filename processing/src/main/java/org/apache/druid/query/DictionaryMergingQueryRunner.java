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
import org.apache.druid.java.util.common.guava.Sequence;
import org.apache.druid.java.util.common.guava.Sequences;
import org.apache.druid.query.context.ResponseContext;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

public class DictionaryMergingQueryRunner implements QueryRunner<DictionaryConversion[]>
{
  private final ExecutorService exec;
  private final Iterable<QueryRunner<DictionaryConversion[]>> queryRunners;

  public DictionaryMergingQueryRunner(ExecutorService exec, Iterable<QueryRunner<DictionaryConversion[]>> queryRunners)
  {
    this.exec = exec;
    this.queryRunners = queryRunners;
  }

  @Override
  public Sequence<DictionaryConversion[]> run(
      QueryPlus<DictionaryConversion[]> queryPlus,
      ResponseContext responseContext
  )
  {
    final DictionaryMergeQuery query = (DictionaryMergeQuery) queryPlus.getQuery();

    // TODO: i'm reading same data twice.. probably i don't need that since the dictionaries are all sorted
    final MergingDictionary[] dictionaries = new MergingDictionary[query.getDimensions().size()];
    for (int i = 0; i < dictionaries.length; i++) {
      dictionaries[i] = new MergingDictionary();
    }
    final Future<MergingDictionary[]> future = exec.submit(
        () -> Sequences
            .simple(queryRunners)
            .flatMap(runner -> runner.run(queryPlus, responseContext))
            .accumulate(dictionaries, (mergingDictionaries, conversions) -> {
              for (int i = 0; i < conversions.length; i++) {
                if (conversions[i].getOldDictionaryId() != DictionaryMergingQueryRunnerFactory.UNKNOWN_DICTIONARY_ID) {
                  mergingDictionaries[i].add(conversions[i].getVal());
                }
              }
              return mergingDictionaries;
            })
    );
    try {
      future.get();
    }
    catch (InterruptedException e) {
      throw new QueryInterruptedException(e);
    }
    catch (ExecutionException e) {
      throw new QueryInterruptedException(e);
    }
    return Sequences.simple(queryRunners)
                    .flatMap(runner -> runner.run(queryPlus, responseContext))
                    .map(conversions -> {
                      DictionaryConversion[] newConversions = new DictionaryConversion[conversions.length];
                      for (int i = 0; i < conversions.length; i++) {
                        newConversions[i] = new DictionaryConversion(
                            conversions[i].getVal(),
                            conversions[i].getSegmentId(),
                            conversions[i].getOldDictionaryId(),
                            dictionaries[i].reverseDictionary.getInt(conversions[i].getVal())
                        );
                      }
                      return newConversions;
                    });
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
      int id = reverseDictionary.getInt(val);
      if (id == DictionaryMergingQueryRunnerFactory.UNKNOWN_DICTIONARY_ID) {
        id = nextId++;
        reverseDictionary.put(val, id);
      }
      return id;
    }
  }
}
