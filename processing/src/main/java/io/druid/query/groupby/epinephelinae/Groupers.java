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

import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ExecutorService;

public class Groupers
{
  private Groupers()
  {
    // No instantiation
  }

  static final AggregateResult DICTIONARY_FULL = AggregateResult.failure(
      "Not enough dictionary space to execute this query. Try increasing "
      + "druid.query.groupBy.maxMergingDictionarySize or enable disk spilling by setting "
      + "druid.query.groupBy.maxOnDiskStorage to a positive number."
  );
  static final AggregateResult HASH_TABLE_FULL = AggregateResult.failure(
      "Not enough aggregation buffer space to execute this query. Try increasing "
      + "druid.processing.buffer.sizeBytes or enable disk spilling by setting "
      + "druid.query.groupBy.maxOnDiskStorage to a positive number."
  );

  private static final int C1 = 0xcc9e2d51;
  private static final int C2 = 0x1b873593;

  /**
   * This method was rewritten in Java from an intermediate step of the Murmur hash function in
   * https://github.com/aappleby/smhasher/blob/master/src/MurmurHash3.cpp, which contained the
   * following header:
   *
   * MurmurHash3 was written by Austin Appleby, and is placed in the public domain. The author
   * hereby disclaims copyright to this source code.
   */
  static int smear(int hashCode)
  {
    return C2 * Integer.rotateLeft(hashCode * C1, 15);
  }

  public static int hash(final Object obj)
  {
    // Mask off the high bit so we can use that to determine if a bucket is used or not.
    // Also apply the smear function, to improve distribution.
    final int code = obj.hashCode();
    return smear(code) & 0x7fffffff;

  }

  static int getUsedFlag(int keyHash)
  {
    return keyHash | 0x80000000;
  }

  public static <KeyType> Iterator<Grouper.Entry<KeyType>> mergeIterators(
      final Iterable<Iterator<Grouper.Entry<KeyType>>> iterators,
      final Comparator<Grouper.Entry<KeyType>> keyTypeComparator
  )
  {
    if (keyTypeComparator != null) {
      return Iterators.mergeSorted(
          iterators,
          keyTypeComparator
      );
    } else {
      return Iterators.concat(iterators.iterator());
    }
  }

  public static <T> AsyncMergingIterator<T> asyncMergeSorted(
      ExecutorService exec,
      int priority,
      List<Iterator<T>> sortedIterators,
      Comparator<T> comparator
  )
  {
    return new AsyncMergingIterator<>(exec, priority, sortedIterators, comparator);
  }

  private static final int MERGE_TREE_FANOUT = 2;

  public static <T> Iterator<T> parallalMergeIterators(
      ExecutorService exec,
      int concurrencyHint, // TODO
      int priority,
      List<Iterator<T>> sortedIterators,
      Comparator<T> comparator
  )
  {
    if (sortedIterators.size() == 1) {
      return sortedIterators.get(0);
    } else if (sortedIterators.size() <= MERGE_TREE_FANOUT) {
      return asyncMergeSorted(exec, priority, sortedIterators, comparator);
    } else {
      final int numIteratorsPerGroup = (sortedIterators.size() + MERGE_TREE_FANOUT - 1) / MERGE_TREE_FANOUT; // ceil

      final List<Iterator<T>> childIterators = new ArrayList<>(MERGE_TREE_FANOUT);
      for (int i = 0; i < sortedIterators.size(); i += numIteratorsPerGroup) {
        childIterators.add(
            parallalMergeIterators(
                exec,
                concurrencyHint,
                priority,
                sortedIterators.subList(i, Math.min(sortedIterators.size(), i + numIteratorsPerGroup)),
                comparator
            )
        );
      }
      return asyncMergeSorted(exec, priority, childIterators, comparator);
    }
  }
}
