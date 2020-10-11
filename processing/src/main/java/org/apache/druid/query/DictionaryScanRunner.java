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
import org.apache.druid.java.util.common.guava.Sequence;
import org.apache.druid.java.util.common.guava.Sequences;
import org.apache.druid.query.context.ResponseContext;
import org.apache.druid.query.dimension.DimensionSpec;
import org.apache.druid.segment.Segment;
import org.apache.druid.segment.StorageAdapter;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

// TODO: how to extend to columns which don't have dictionary?
public class DictionaryScanRunner implements IdentifiableQueryRunner<List<Iterator<DictionaryConversion>>>
{
  private final int segmentId;
  private final StorageAdapter storageAdapter;

  public DictionaryScanRunner(int segmentId, Segment segment)
  {
    this.segmentId = segmentId;
    this.storageAdapter = segment.asStorageAdapter();
  }

  // TODO: make a type for List<Iterator<DictionaryConversion>>
  @Override
  public Sequence<List<Iterator<DictionaryConversion>>> run(QueryPlus<List<Iterator<DictionaryConversion>>> queryPlus, ResponseContext responseContext)
  {
    final DictionaryMergeQuery query = (DictionaryMergeQuery) queryPlus.getQuery();

    return Sequences.simple(
        () -> new Iterator<List<Iterator<DictionaryConversion>>>()
        {
          boolean returned;

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
            final List<Iterator<DictionaryConversion>> dictionaryIterators = new ArrayList<>(
                query.getDimensions().size()
            );
            final int[] dictIds = new int[query.getDimensions().size()];
            Arrays.fill(dictIds, 0);
            for (int i = 0; i < query.getDimensions().size(); i++) {
              final int dimIndex = i;
              final DimensionSpec dimensionSpec = query.getDimensions().get(dimIndex);
              dictionaryIterators.add(
                  FluentIterable.from(() -> storageAdapter.getDictionaryIterator(dimensionSpec.getDimension()))
                                .transform(val -> new DictionaryConversion(
                                    val,
                                    segmentId,
                                    dictIds[dimIndex]++,
                                    DictionaryMergingQueryRunnerFactory.UNKNOWN_DICTIONARY_ID // fills with unknown temporarily. will be updated when merging
                                ))
                                .iterator()
              );
            }
            return dictionaryIterators;
          }
        }
    );
  }

  @Override
  public int getSegmentId()
  {
    return segmentId;
  }
}
