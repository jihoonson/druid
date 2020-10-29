///*
// * Licensed to the Apache Software Foundation (ASF) under one
// * or more contributor license agreements.  See the NOTICE file
// * distributed with this work for additional information
// * regarding copyright ownership.  The ASF licenses this file
// * to you under the Apache License, Version 2.0 (the
// * "License"); you may not use this file except in compliance
// * with the License.  You may obtain a copy of the License at
// *
// *   http://www.apache.org/licenses/LICENSE-2.0
// *
// * Unless required by applicable law or agreed to in writing,
// * software distributed under the License is distributed on an
// * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// * KIND, either express or implied.  See the License for the
// * specific language governing permissions and limitations
// * under the License.
// */
//
//package org.apache.druid.query.groupby.epinephelinae;
//
//import com.google.common.base.Supplier;
//import com.google.common.base.Suppliers;
//import com.google.common.collect.Lists;
//import org.apache.druid.collections.ReferenceCountingResourceHolder;
//import org.apache.druid.collections.Releaser;
//import org.apache.druid.java.util.common.CloseableIterators;
//import org.apache.druid.java.util.common.guava.Sequence;
//import org.apache.druid.java.util.common.parsers.CloseableIterator;
//import org.apache.druid.query.AbstractPrioritizedCallable;
//import org.apache.druid.query.aggregation.AggregatorFactory;
//import org.apache.druid.query.groupby.ResultRow;
//import org.apache.druid.query.groupby.epinephelinae.Grouper.Entry;
//import org.apache.druid.query.groupby.epinephelinae.Grouper.KeySerde;
//import org.apache.druid.query.groupby.epinephelinae.Grouper.KeySerdeFactory;
//import org.apache.druid.query.groupby.epinephelinae.ParallelCombiner.SettableColumnSelectorFactory;
//import org.apache.druid.query.groupby.epinephelinae.RowBasedGrouperHelper.RowBasedKey;
//
//import java.io.IOException;
//import java.nio.ByteBuffer;
//import java.util.ArrayList;
//import java.util.Comparator;
//import java.util.List;
//import java.util.concurrent.ExecutorService;
//import java.util.concurrent.Future;
//
//public class SequenceCombiner
//{
//  // list of sequences to merge and combine
//  // merge buffer slice
//  // return a combined sequence reading from merge buffer slice
//  // reading and writing should be able to be done in parallel
//
//  private final List<CloseableIterator<Entry<RowBasedKey>>> iterators;
//  private final ReferenceCountingResourceHolder<ByteBuffer> combineBufferHolder;
//  private final Supplier<ByteBuffer> bufferSupplier;
//  private final ExecutorService executor;
//  private final int priority;
//
//  public Sequence<ResultRow> combine(
//      KeySerdeFactory<RowBasedKey> keySerdeFactory,
//      Comparator<Entry<RowBasedKey>> keyObjComparator,
//      AggregatorFactory[] combiningFactories,
//      int concurrencyHint,
//      long queryTimeoutAtMs
//  )
//  {
//    final KeySerde<RowBasedKey> keySerde = keySerdeFactory.factorize();
//    final int partitionSize = Math.round((float) iterators.size() / concurrencyHint);
//    final List<CloseableIterator<Entry<RowBasedKey>>> grouperIterators = new ArrayList<>(concurrencyHint);
//    final List<Future> grouperFutures = new ArrayList<>(concurrencyHint);
//    final ByteBuffer mergeBuffer = bufferSupplier.get();
//    final int sliceSize = mergeBuffer.capacity() / concurrencyHint;
//    int i = 0;
//    for (List<CloseableIterator<Entry<RowBasedKey>>> partition : Lists.partition(iterators, partitionSize)) {
//      final ByteBuffer slice = Groupers.getSlice(mergeBuffer, sliceSize, i++);
//      final SettableColumnSelectorFactory settableColumnSelectorFactory =
//          new SettableColumnSelectorFactory(combiningFactories);
//      final StreamingMergeSortedGrouper<RowBasedKey> grouper = new StreamingMergeSortedGrouper<>(
//          Suppliers.ofInstance(slice),
//          keySerde,
//          settableColumnSelectorFactory,
//          combiningFactories,
//          queryTimeoutAtMs
//      );
//      grouper.init();
//      grouperIterators.add(grouper.iterator());
//
//      grouperFutures.add(
//          executor.submit(
//              new AbstractPrioritizedCallable<Void>(priority)
//              {
//                @Override
//                public Void call()
//                {
//                  try (
//                      CloseableIterator<Entry<RowBasedKey>> mergedIterator = CloseableIterators.mergeSorted(
//                          partition,
//                          keyObjComparator
//                      );
//                      // This variable is used to close releaser automatically.
//                      @SuppressWarnings("unused")
//                      final Releaser releaser = combineBufferHolder.increment()
//                  ) {
//                    while (mergedIterator.hasNext()) {
//                      final Entry<RowBasedKey> next = mergedIterator.next();
//
//                      settableColumnSelectorFactory.set(next.values);
//                      grouper.aggregate(next.key); // grouper always returns ok or throws an exception
//                      settableColumnSelectorFactory.set(null);
//                    }
//                  }
//                  catch (IOException e) {
//                    throw new RuntimeException(e);
//                  }
//
//                  grouper.finish();
//                  return null;
//                }
//              }
//          )
//      );
//    }
//  }
//}
