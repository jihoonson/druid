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

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;

public class DictionaryMergingQueryRunner implements QueryRunner<DictionaryConversion>
{
  private static final int UNKNOWN_DICTIONARY_ID = -1;

  private final ExecutorService exec;
  private final Iterable<QueryRunner<DictionaryConversion>> queryRunners;

  public DictionaryMergingQueryRunner(ExecutorService exec, Iterable<QueryRunner<DictionaryConversion>> queryRunners)
  {
    this.exec = exec;
    this.queryRunners = queryRunners;
  }

  @Override
  public Sequence<DictionaryConversion> run(QueryPlus<DictionaryConversion> queryPlus, ResponseContext responseContext)
  {
    final MergingDictionary dictionary = Sequences
        .simple(queryRunners)
        .flatMap(runner -> runner.run(queryPlus, responseContext))
        .accumulate(new MergingDictionary(), (mergingDictionary, dictionaryConversion) -> {
          if (dictionaryConversion.getOldDictionaryId() != UNKNOWN_DICTIONARY_ID) {
            mergingDictionary.add(dictionaryConversion.getVal());
          }
          return mergingDictionary;
        });
    return Sequences.simple(queryRunners)
                    .flatMap(runner -> runner.run(queryPlus, responseContext))
                    .map(dictionaryConversion -> new DictionaryConversion(
                        dictionaryConversion.getVal(),
                        dictionaryConversion.getSegmentId(),
                        dictionaryConversion.getOldDictionaryId(),
                        dictionary.reverseDictionary.getInt(dictionaryConversion.getVal())
                    ));
  }

  private static class MergingDictionary
  {
    // TODO: size limit on dictionary?

    private final List<String> dictionary = new ArrayList<>();
    private final Object2IntMap<String> reverseDictionary = new Object2IntOpenHashMap<>();

    private MergingDictionary()
    {
      reverseDictionary.defaultReturnValue(UNKNOWN_DICTIONARY_ID);
    }

    /**
     * Return new dictionary ID.
     */
    private int add(String val)
    {
      int id = reverseDictionary.getInt(val);
      if (id == UNKNOWN_DICTIONARY_ID) {
        id = dictionary.size();
        reverseDictionary.put(val, id);
        dictionary.add(val);
      }
      return id;
    }
  }
}
