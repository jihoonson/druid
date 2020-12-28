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

package org.apache.druid.query.groupby.epinephelinae;

import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import com.google.common.collect.Iterables;
import com.google.common.collect.Ordering;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import org.apache.druid.collections.ReferenceCountingResourceHolder;
import org.apache.druid.common.guava.SettableSupplier;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.NonnullPair;
import org.apache.druid.java.util.common.Pair;
import org.apache.druid.java.util.common.guava.Accumulator;
import org.apache.druid.java.util.common.guava.BaseSequence;
import org.apache.druid.java.util.common.guava.BaseSequence.IteratorMaker;
import org.apache.druid.java.util.common.guava.CloseQuietly;
import org.apache.druid.java.util.common.guava.MergeSequence;
import org.apache.druid.java.util.common.guava.Sequence;
import org.apache.druid.java.util.common.guava.Sequences;
import org.apache.druid.java.util.common.io.Closer;
import org.apache.druid.java.util.common.parsers.CloseableIterator;
import org.apache.druid.query.AbstractPrioritizedCallable;
import org.apache.druid.query.BaseQuery;
import org.apache.druid.query.QueryInterruptedException;
import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.query.dimension.DimensionSpec;
import org.apache.druid.query.groupby.GroupByQuery;
import org.apache.druid.query.groupby.ResultRow;
import org.apache.druid.query.groupby.epinephelinae.Grouper.Entry;
import org.apache.druid.query.groupby.epinephelinae.Grouper.KeySerdeFactory;
import org.apache.druid.query.groupby.epinephelinae.RowBasedGrouperHelper.RowBasedKey;
import org.apache.druid.query.groupby.epinephelinae.RowBasedGrouperHelper.ValueExtractFunction;
import org.apache.druid.segment.ColumnSelectorFactory;
import org.apache.druid.segment.DictionaryEncodedRowBasedColumnSelectorFactory;
import org.apache.druid.segment.DimensionHandlerUtils;
import org.apache.druid.segment.RowAdapter;
import org.apache.druid.segment.column.ValueType;

import javax.annotation.Nullable;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.function.Function;
import java.util.function.ToLongFunction;

public class ParallelSortedSequenceCombiner
{
  private static final int MINIMUM_LEAF_COMBINE_DEGREE = 1;

  private final GroupByQuery query;
  private final Ordering<ResultRow> rowOrdering;
  private final ReferenceCountingResourceHolder<ByteBuffer> combineBufferHolder;
  private final AggregatorFactory[] combiningFactories;
  private final KeySerdeFactory<RowBasedKey> combineKeySerdeFactory;
  private final ListeningExecutorService executor;
  private final int concurrencyHint;
  private final int priority;
  private final long queryTimeoutAt;

  // The default value is 8 which comes from an experiment. A non-leaf node will combine up to intermediateCombineDegree
  // rows for the same grouping key.
  private final int intermediateCombineDegree;

  public ParallelSortedSequenceCombiner(
      GroupByQuery query,
      Ordering<ResultRow> rowOrdering,
      ReferenceCountingResourceHolder<ByteBuffer> combineBufferHolder,
      AggregatorFactory[] combiningFactories,
      KeySerdeFactory<RowBasedKey> combineKeySerdeFactory, // TODO: make sure that this serdeFactory has the merged dictionary fed
      ListeningExecutorService executor,
      int concurrencyHint,
      int priority,
      long queryTimeoutAt,
      int intermediateCombineDegree
  )
  {
    this.query = query;
    this.rowOrdering = rowOrdering;
    this.combineBufferHolder = combineBufferHolder;
    this.combiningFactories = combiningFactories;
    this.combineKeySerdeFactory = combineKeySerdeFactory;
    this.executor = executor;
    this.concurrencyHint = concurrencyHint;
    this.priority = priority;
    this.intermediateCombineDegree = intermediateCombineDegree;
    this.queryTimeoutAt = queryTimeoutAt;
  }

  public Sequence<ResultRow> combine(
      List<Sequence<ResultRow>> sortedSequences,
      Supplier<MergedDictionary[]> mergedDictionariesSupplier
  )
  {
    // CombineBuffer is initialized when this method is called and closed after the result iterator is done
    final Closer closer = Closer.create();
    try {
      final ByteBuffer combineBuffer = combineBufferHolder.get();
      final int minimumRequiredBufferCapacity = StreamingMergeSortedGrouper.requiredBufferCapacity(
          combineKeySerdeFactory.factorize(),
          combiningFactories
      );
      // We want to maximize the parallelism while the size of buffer slice is greater than the minimum buffer size
      // required by StreamingMergeSortedGrouper. Here, we find the leafCombineDegree of the cominbing tree and the
      // required number of buffers maximizing the parallelism.
      final Pair<Integer, Integer> degreeAndNumBuffers = findLeafCombineDegreeAndNumBuffers(
          combineBuffer,
          minimumRequiredBufferCapacity,
          concurrencyHint,
          sortedSequences.size()
      );

      final int leafCombineDegree = degreeAndNumBuffers.lhs;
      final int numBuffers = degreeAndNumBuffers.rhs;
      final int sliceSize = combineBuffer.capacity() / numBuffers;

      final Supplier<ByteBuffer> bufferSupplier = createCombineBufferSupplier(combineBuffer, numBuffers, sliceSize);

      final NonnullPair<List<CloseableIterator<Entry<RowBasedKey>>>, List<Future<AggregateResult>>> combineIteratorAndFutures = buildCombineTree(
          sortedSequences,
          bufferSupplier,
          combiningFactories,
          leafCombineDegree,
          mergedDictionariesSupplier
      );

      final CloseableIterator<Entry<RowBasedKey>> combineIterator = Iterables.getOnlyElement(combineIteratorAndFutures.lhs);
      final List<Future<AggregateResult>> combineFutures = combineIteratorAndFutures.rhs;

      closer.register(() -> checkCombineFutures(combineFutures));

      return new BaseSequence<>(
          new IteratorMaker<ResultRow, Iterator<ResultRow>>()
          {
            @Override
            public Iterator<ResultRow> make()
            {
              return RowBasedGrouperHelper.makeGrouperIterator(
                  combineIterator,
                  query,
                  null,
                  closer,
                  mergedDictionariesSupplier
              );
            }

            @Override
            public void cleanup(Iterator<ResultRow> iterFromMake)
            {
              CloseQuietly.close(closer);
            }
          }
      );
    }
    catch (Throwable t) {
      try {
        closer.close();
      }
      catch (Throwable t2) {
        t.addSuppressed(t2);
      }
      throw t;
    }
  }

  private static void checkCombineFutures(List<Future<AggregateResult>> combineFutures)
  {
    for (Future<AggregateResult> future : combineFutures) {
      try {
        if (!future.isDone()) {
          // Cancel futures if close() for the iterator is called early due to some reason (e.g., test failure)
          future.cancel(true);
        } else {
          future.get();
        }
      }
      catch (InterruptedException | CancellationException e) {
        throw new QueryInterruptedException(e);
      }
      catch (ExecutionException e) {
        throw new RuntimeException(e);
      }
    }
  }

  private static Supplier<ByteBuffer> createCombineBufferSupplier(
      ByteBuffer combineBuffer,
      int numBuffers,
      int sliceSize
  )
  {
    return new Supplier<ByteBuffer>()
    {
      private int i = 0;

      @Override
      public ByteBuffer get()
      {
        if (i < numBuffers) {
          return Groupers.getSlice(combineBuffer, sliceSize, i++);
        } else {
          throw new ISE("Requested number[%d] of buffer slices exceeds the planned one[%d]", i++, numBuffers);
        }
      }
    };
  }

  private Pair<Integer, Integer> findLeafCombineDegreeAndNumBuffers(
      ByteBuffer combineBuffer,
      int requiredMinimumBufferCapacity,
      int numAvailableThreads,
      int numLeafNodes
  )
  {
    for (int leafCombineDegree = MINIMUM_LEAF_COMBINE_DEGREE; leafCombineDegree <= numLeafNodes; leafCombineDegree++) {
      final int requiredBufferNum = computeRequiredBufferNum(numLeafNodes, leafCombineDegree);
      if (requiredBufferNum <= numAvailableThreads) {
        final int expectedSliceSize = combineBuffer.capacity() / requiredBufferNum;
        if (expectedSliceSize >= requiredMinimumBufferCapacity) {
          return Pair.of(leafCombineDegree, requiredBufferNum);
        }
      }
    }

    throw new ISE(
        "Cannot find a proper leaf combine degree for the combining tree. "
        + "Each node of the combining tree requires a buffer of [%d] bytes. "
        + "Try increasing druid.processing.buffer.sizeBytes (currently [%d] bytes) for larger buffer or "
        + "druid.query.groupBy.intermediateCombineDegree for a smaller tree",
        requiredMinimumBufferCapacity,
        combineBuffer.capacity()
    );
  }

  private int computeRequiredBufferNum(int numChildNodes, int combineDegree)
  {
    // numChildrenForLastNode used to determine that the last node is needed for the current level.
    // Please see buildCombineTree() for more details.
    final int numChildrenForLastNode = numChildNodes % combineDegree;
    final int numCurLevelNodes = numChildNodes / combineDegree + (numChildrenForLastNode > 1 ? 1 : 0);
    final int numChildOfParentNodes = numCurLevelNodes + (numChildrenForLastNode == 1 ? 1 : 0);

    if (numChildOfParentNodes == 1) {
      return numCurLevelNodes;
    } else {
      return numCurLevelNodes +
             computeRequiredBufferNum(numChildOfParentNodes, intermediateCombineDegree);
    }
  }

  private NonnullPair<List<CloseableIterator<Entry<RowBasedKey>>>, List<Future<AggregateResult>>> buildCombineTree(
      List<Sequence<ResultRow>> childSequences,
      Supplier<ByteBuffer> bufferSupplier,
      AggregatorFactory[] combiningFactories,
      int combineDegree,
      Supplier<MergedDictionary[]> mergedDictionariesSupplier
  )
  {
    final int numChildLevelIterators = childSequences.size();
    final List<CloseableIterator<Entry<RowBasedKey>>> childIteratorsOfNextLevel = new ArrayList<>();
    final List<Future<AggregateResult>> combineFutures = new ArrayList<>();

    // The below algorithm creates the combining nodes of the current level. It first checks that the number of children
    // to be combined together is 1. If it is, the intermediate combining node for that child is not needed. Instead, it
    // can be directly connected to a node of the parent level. Here is an example of generated tree when
    // numLeafNodes = 6 and leafCombineDegree = intermediateCombineDegree = 2. See the description of
    // MINIMUM_LEAF_COMBINE_DEGREE for more details about leafCombineDegree and intermediateCombineDegree.
    //
    //      o
    //     / \
    //    o   \
    //   / \   \
    //  o   o   o
    // / \ / \ / \
    // o o o o o o
    //
    // We can expect that the aggregates can be combined as early as possible because the tree is built in a bottom-up
    // manner.

    for (int i = 0; i < numChildLevelIterators; i += combineDegree) {
      final List<Sequence<ResultRow>> subSequences = childSequences.subList(
          i,
          Math.min(i + combineDegree, numChildLevelIterators)
      );
      final Pair<CloseableIterator<Entry<RowBasedKey>>, Future<AggregateResult>> iteratorAndFuture = runCombiner(
          query,
          subSequences,
          rowOrdering,
          bufferSupplier.get(),
          combiningFactories,
          mergedDictionariesSupplier
      );

      childIteratorsOfNextLevel.add(iteratorAndFuture.lhs);
      combineFutures.add(iteratorAndFuture.rhs);
    }

    if (childIteratorsOfNextLevel.size() == 1) {
      // This is the root
      return new NonnullPair<>(childIteratorsOfNextLevel, combineFutures);
    } else {
//      // Build the parent level iterators
//      final Pair<List<CloseableIterator<Entry<RowBasedKey>>>, List<Future>> parentIteratorsAndFutures =
//          buildCombineTree(
//              childIteratorsOfNextLevel,
//              bufferSupplier,
//              combiningFactories,
//              intermediateCombineDegree,
//              mergedDictionariesSupplier
//          );
//      combineFutures.addAll(parentIteratorsAndFutures.rhs);
//      return Pair.of(parentIteratorsAndFutures.lhs, combineFutures);
      throw new UnsupportedOperationException();
    }
  }

  private Pair<CloseableIterator<Entry<RowBasedKey>>, Future<AggregateResult>> runCombiner(
      GroupByQuery query,
      List<Sequence<ResultRow>> sequences,
      Ordering<ResultRow> rowOrdering,
      ByteBuffer combineBuffer,
      AggregatorFactory[] combiningFactories,
      Supplier<MergedDictionary[]> mergedDictionariesSupplier
  )
  {
    final SettableSupplier<ResultRow> rowSupplier = new SettableSupplier<>();
    final ColumnSelectorFactory columnSelectorFactory = createResultRowBasedColumnSelectorFactory(
        query,
        rowSupplier,
        mergedDictionariesSupplier
    );
    final int keySize = query.getResultRowHasTimestamp() ? query.getDimensions().size() + 1 : query.getDimensions().size();
    final List<ValueType> valueTypes = DimensionHandlerUtils.getValueTypesFromDimensionSpecs(query.getDimensions());
    final ValueExtractFunction valueExtractFn = RowBasedGrouperHelper.makeValueExtractFunction(
        query,
        false,
        query.getResultRowHasTimestamp(),
        columnSelectorFactory,
        valueTypes,
        true
    );

    final StreamingMergeSortedGrouper<RowBasedKey> grouper = new StreamingMergeSortedGrouper<>(
        Suppliers.ofInstance(combineBuffer),
        combineKeySerdeFactory.factorize(),
        columnSelectorFactory,
        combiningFactories,
        queryTimeoutAt
    );
    grouper.init(); // init() must be called before iterator(), so cannot be called inside the below callable.

    final Sequence<ResultRow> mergeSequence = Sequences.withBaggage(
        new MergeSequence<>(rowOrdering, Sequences.simple(sequences)),
        grouper::finish
    );
    final Comparable[] key = new Comparable[keySize];
    final Accumulator<AggregateResult, ResultRow> accumulator = (priorResult, row) -> {
      BaseQuery.checkInterrupted();

      if (priorResult != null && !priorResult.isOk()) {
        // Pass-through error returns without doing more work.
        return priorResult;
      }

      rowSupplier.set(row);

      valueExtractFn.apply(row, key);
//      System.err.println("row: " + row);

      final AggregateResult aggregateResult = grouper.aggregate(new RowBasedKey(key));
      rowSupplier.set(null);

      return aggregateResult;
    };

    final ListenableFuture<AggregateResult> future = executor.submit(
        new AbstractPrioritizedCallable<AggregateResult>(priority)
        {
          @Override
          public AggregateResult call()
          {
//            System.err.println("submit combiner");
            return mergeSequence.accumulate(AggregateResult.ok(), accumulator);
          }
        }
    );

    return new Pair<>(grouper.iterator(), future);
  }

  private static ColumnSelectorFactory createResultRowBasedColumnSelectorFactory(
      final GroupByQuery query,
      final Supplier<ResultRow> supplier,
      @Nullable final Supplier<MergedDictionary[]> mergedDictionarySupplier
  )
  {
    final RowAdapter<ResultRow> adapter =
        new RowAdapter<ResultRow>()
        {
          @Override
          public ToLongFunction<ResultRow> timestampFunction()
          {
            if (query.getResultRowHasTimestamp()) {
              return row -> row.getLong(0);
            } else {
              final long timestamp = query.getUniversalTimestamp().getMillis();
              return row -> timestamp;
            }
          }

          @Override
          public Function<ResultRow, Object> columnFunction(final String columnName)
          {
            if (mergedDictionarySupplier != null) {
              final Optional<DimensionSpec> columnSpec = query
                  .getDimensions()
                  .stream()
                  .filter(dimensionSpec -> dimensionSpec.getDimension().equals(columnName))
                  .findAny();
              if (columnSpec.isPresent()) {
                final int dimensionIndex = query.getResultRowSignature().indexOf(columnSpec.get().getOutputName());
                if (dimensionIndex < 0) {
                  return row -> null;
                } else {
                  return row -> row.getInt(dimensionIndex);
                }
              }

//              final boolean isDimension = query.getDimensions()
//                                               .stream()
//                                               .anyMatch(dimensionSpec -> dimensionSpec.getDimension().equals(columnName));
//              if (isDimension) {
//                final int dimensionIndex = query.getResultRowSignature().indexOf(columnName) - query.getResultRowDimensionStart();
//                if (dimensionIndex < 0) {
//                  return row -> null;
//                } else {
//                  return row -> {
////                  final PerSegmentEncodedResultRow perSegmentEncodedResultRow = (PerSegmentEncodedResultRow) row;
////                  return mergedDictionarySupplier.get()[dimensionIndex].getNewDictId(
////                      perSegmentEncodedResultRow.getSegmentId(dimensionIndex),
////                      perSegmentEncodedResultRow.getInt(dimensionIndex)
////                  );
//                    return row.getInt(dimensionIndex);
//                  };
//                }
//              }
            }
            final int columnIndex = query.getResultRowSignature().indexOf(columnName);
            if (columnIndex < 0) {
              return row -> null;
            } else {
              return row -> row.get(columnIndex);
            }
          }
        };

    return new DictionaryEncodedRowBasedColumnSelectorFactory(
        query,
        supplier::get,
        adapter,
        mergedDictionarySupplier::get,
        query.getResultRowSignature()
    );
  }
}
