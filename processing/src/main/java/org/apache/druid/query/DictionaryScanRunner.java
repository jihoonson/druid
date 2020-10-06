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

import org.apache.druid.java.util.common.guava.Sequence;
import org.apache.druid.java.util.common.guava.Sequences;
import org.apache.druid.query.context.ResponseContext;
import org.apache.druid.segment.Segment;
import org.apache.druid.segment.StorageAdapter;

import java.util.Arrays;
import java.util.Iterator;
import java.util.NoSuchElementException;

public class DictionaryScanRunner implements IdentifiableQueryRunner<Iterator<DictionaryConversion>>
{
  private final int segmentId;
  private final StorageAdapter storageAdapter;

  public DictionaryScanRunner(int segmentId, Segment segment)
  {
    this.segmentId = segmentId;
    this.storageAdapter = segment.asStorageAdapter();
  }

  // TODO: This can return Sequence<Iterator<DictionaryConversion>>. Each iterator is the conversions for one column.
  // or Sequence<Sequence<DictionaryConversion>>.. but probably iterator will be better since there is no reason
  // to flat out the outer sequence.
  @Override
  public Sequence<Iterator<DictionaryConversion>> run(QueryPlus<Iterator<DictionaryConversion>> queryPlus, ResponseContext responseContext)
  {
    final DictionaryMergeQuery query = (DictionaryMergeQuery) queryPlus.getQuery();

    return Sequences.simple(
        () -> new Iterator<Iterator<DictionaryConversion>>()
        {
          int remainingDimensions = query.getDimensions().size();
          int nextDimension = segmentId % remainingDimensions;

          @Override
          public boolean hasNext()
          {
            return remainingDimensions > 0;
          }

          @Override
          public Iterator<DictionaryConversion> next()
          {
            final Iterator<String> dictionaryIterator = storageAdapter.getDictionaryIterator(
                query.getDimensions().get(nextDimension++).getDimension()
            );
            if (nextDimension == query.getDimensions().size()) {
              nextDimension = 0;
            }
            remainingDimensions--;
            return new Iterator<DictionaryConversion>()
            {
              int dictId;

              @Override
              public boolean hasNext()
              {
                return dictionaryIterator.hasNext();
              }

              @Override
              public DictionaryConversion next()
              {
                if (!hasNext()) {
                  throw new NoSuchElementException();
                }
                final String val = dictionaryIterator.next();
                return new DictionaryConversion(
                    val,
                    segmentId,
                    dictId++,
                    DictionaryMergingQueryRunnerFactory.UNKNOWN_DICTIONARY_ID // fills with unknown temporarily. will be updated when merging
                );
              }
            };
          }
        }
    );

//    final Iterator<String>[] dictionaryIterators = new Iterator[query.getDimensions().size()];
//    for (int i = 0; i < dictionaryIterators.length; i++) {
//      // TODO: add a good interface in StorageAdapter
//      dictionaryIterators[i] = storageAdapter.getDictionaryIterator(
//          query.getDimensions().get(i).getDimension()
//      );
//    }
//
//    final IteratorsIterator iterator = new IteratorsIterator(segmentId, dictionaryIterators);
//    return Sequences.simple(() -> iterator);
  }

  @Override
  public int getSegmentId()
  {
    return segmentId;
  }

  private static class IteratorsIterator implements Iterator<DictionaryConversion[]>
  {
    private final int segmentId;
    private final Iterator<String>[] iterators;
    private int dictId;

    private IteratorsIterator(int segmentId, Iterator<String>[] dictionaryIterators)
    {
      this.segmentId = segmentId;
      this.iterators = dictionaryIterators;
    }

    @Override
    public boolean hasNext()
    {
      return Arrays.stream(iterators).anyMatch(Iterator::hasNext);
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
          final String value = iterators[i].next();
          // TODO: we can filter out value based on the query filter
          conversions[i] = new DictionaryConversion(
              value,
              segmentId,
              dictId,
              DictionaryMergingQueryRunnerFactory.UNKNOWN_DICTIONARY_ID // fills with unknown temporarily. will be updated when merging
          );
        }
      }
      dictId++;
      return conversions;
    }
  }
}