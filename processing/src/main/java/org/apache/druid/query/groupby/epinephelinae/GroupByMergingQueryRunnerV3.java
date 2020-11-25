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
import com.google.common.collect.FluentIterable;
import com.google.common.collect.Iterables;
import com.google.common.collect.Ordering;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import it.unimi.dsi.fastutil.ints.Int2IntMap;
import it.unimi.dsi.fastutil.ints.Int2IntOpenHashMap;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.ints.IntList;
import org.apache.druid.collections.BlockingPool;
import org.apache.druid.collections.ReferenceCountingResourceHolder;
import org.apache.druid.common.guava.SettableSupplier;
import org.apache.druid.java.util.common.guava.MappedSequence;
import org.apache.druid.java.util.common.guava.ParallelMergeCombiningSequence;
import org.apache.druid.java.util.common.guava.Sequence;
import org.apache.druid.java.util.common.guava.Sequences;
import org.apache.druid.java.util.common.guava.Yielder;
import org.apache.druid.java.util.common.guava.Yielders;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.java.util.common.parsers.CloseableIterator;
import org.apache.druid.query.DictionaryConversion;
import org.apache.druid.query.DictionaryMergeQuery;
import org.apache.druid.query.DictionaryMergingQueryRunner;
import org.apache.druid.query.DruidProcessingConfig;
import org.apache.druid.query.QueryContexts;
import org.apache.druid.query.QueryInterruptedException;
import org.apache.druid.query.QueryPlus;
import org.apache.druid.query.QueryRunner;
import org.apache.druid.query.QueryWatcher;
import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.query.context.ResponseContext;
import org.apache.druid.query.dimension.DimensionSpec;
import org.apache.druid.query.groupby.GroupByQuery;
import org.apache.druid.query.groupby.GroupByQueryConfig;
import org.apache.druid.query.groupby.PerSegmentEncodedResultRow;
import org.apache.druid.query.groupby.ResultRow;
import org.apache.druid.query.groupby.epinephelinae.Grouper.Entry;
import org.apache.druid.query.groupby.epinephelinae.Grouper.KeySerdeFactory;
import org.apache.druid.query.groupby.epinephelinae.RowBasedGrouperHelper.RowBasedKey;
import org.apache.druid.query.groupby.epinephelinae.RowBasedGrouperHelper.RowBasedKeySerdeFactory;
import org.apache.druid.query.groupby.epinephelinae.RowBasedGrouperHelper.ValueExtractFunction;
import org.apache.druid.query.groupby.orderby.DefaultLimitSpec;
import org.apache.druid.query.groupby.orderby.OrderByColumnSpec;
import org.apache.druid.query.ordering.StringComparator;
import org.apache.druid.query.ordering.StringComparators;
import org.apache.druid.segment.ColumnSelectorFactory;
import org.apache.druid.segment.DictionaryEncodedRowBasedColumnSelectorFactory;
import org.apache.druid.segment.DimensionHandlerUtils;
import org.apache.druid.segment.RowAdapter;
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.segment.column.ValueType;

import javax.annotation.Nullable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Function;
import java.util.function.ToLongFunction;

public class GroupByMergingQueryRunnerV3 implements QueryRunner<ResultRow>
{
  private static final Logger log = new Logger(GroupByMergingQueryRunnerV3.class);

  private final GroupByQueryConfig config;
  private final Iterable<QueryRunner<ResultRow>> queryables;
  private final ListeningExecutorService exec;
  private final QueryWatcher queryWatcher;
  private final DictionaryMergingQueryRunner dictionaryMergingRunner;
  private final DruidProcessingConfig processingConfig;
  private final ForkJoinPool pool;
  private final BlockingPool<ByteBuffer> mergeBufferPool;

  public GroupByMergingQueryRunnerV3(
      GroupByQueryConfig config,
      ExecutorService exec,
      QueryWatcher queryWatcher,
      Iterable<QueryRunner<ResultRow>> queryables,
      DictionaryMergingQueryRunner dictionaryMergingRunner,
      DruidProcessingConfig processingConfig,
      ForkJoinPool pool,
      BlockingPool<ByteBuffer> mergeBufferPool
  )
  {
    this.config = config;
    this.queryables = queryables;
    this.exec = MoreExecutors.listeningDecorator(exec);
    this.queryWatcher = queryWatcher;
    this.dictionaryMergingRunner = dictionaryMergingRunner;
    this.processingConfig = processingConfig;
    this.pool = pool;
    this.mergeBufferPool = mergeBufferPool;
  }

  @Override
  public Sequence<ResultRow> run(QueryPlus<ResultRow> queryPlus, ResponseContext responseContext)
  {
    final GroupByQuery query = (GroupByQuery) queryPlus.getQuery();

    final QueryPlus<ResultRow> queryPlusForRunners = queryPlus
        .withQuery(query)
        .withoutThreadUnsafeState();

    final int priority = QueryContexts.getPriority(query);

    // Figure out timeoutAt
    final long queryTimeout = QueryContexts.getTimeout(query);
    final boolean hasTimeout = QueryContexts.hasTimeout(query);
    final long timeoutAt = System.currentTimeMillis() + queryTimeout;

    final Supplier<MergedDictionary[]> mergedDictionariesSupplier = createMergedDictionariesSupplier(
        query,
        hasTimeout,
        timeoutAt
    );

    final int keySize = query.getResultRowHasTimestamp() ? query.getDimensions().size() + 1 : query.getDimensions().size();
    final List<ValueType> valueTypes = DimensionHandlerUtils.getValueTypesFromDimensionSpecs(query.getDimensions());

    final List<? extends CloseableIterator<Entry<RowBasedKey>>> iterators = FluentIterable
        .from(queryables)
        .transform(runner -> {
          final SettableSupplier<ResultRow> rowSupplier = new SettableSupplier<>();
          final ColumnSelectorFactory columnSelectorFactory = createResultRowBasedColumnSelectorFactory(
              query,
              rowSupplier,
              mergedDictionariesSupplier
          );
          final ValueExtractFunction valueExtractFn = RowBasedGrouperHelper.makeValueExtractFunction(
              query,
              false,
              query.getResultRowHasTimestamp(),
              columnSelectorFactory,
              valueTypes,
              true
          );

          final Future<Yielder<Entry<RowBasedKey>>> future = exec.submit(() -> Yielders.each(
              runner.run(queryPlusForRunners, responseContext).map(row -> {
                final PerSegmentEncodedResultRow encodedResultRow = (PerSegmentEncodedResultRow) row;
                rowSupplier.set(row);
                final Comparable[] key = new Comparable[keySize];
                valueExtractFn.apply(row, key);
                final RowBasedKey rowBasedKey = new RowBasedKey(key);
                final Object[] values = new Object[query.getAggregatorSpecs().size()];
                for (int i = 0; i < values.length; i++) {
                  values[i] = encodedResultRow.get(i + query.getResultRowAggregatorStart());
                }
//                System.err.println("runner: " + runner + ", key: " + rowBasedKey + ", values: " + Arrays.toString(values));
                return new Entry<>(rowBasedKey, values);
              })
          ));

          return new CloseableIterator<Entry<RowBasedKey>>()
          {
            CloseableIterator<Entry<RowBasedKey>> delegate;
            
            @Override
            public boolean hasNext()
            {
              if (delegate == null) {
                try {
                  delegate = Yielders.iterator(future.get());
                }
                catch (InterruptedException | ExecutionException e) {
                  Thread.currentThread().interrupt();
                  throw new RuntimeException(e);
                }
              }
              return delegate.hasNext();
            }

            @Override
            public Entry<RowBasedKey> next()
            {
              Entry<RowBasedKey> entry = delegate.next();
//              System.err.println("entry: " + entry);
              return entry;
            }

            @Override
            public void close() throws IOException
            {
              delegate.close();
            }
          };
        })
        .toList();

    final AggregatorFactory[] aggregatorFactories = query.getAggregatorSpecs().toArray(new AggregatorFactory[0]);
    final Grouper.KeySerdeFactory<RowBasedKey> combineKeySerdeFactory = new RowBasedKeySerdeFactory(
        query.getResultRowHasTimestamp(),
        query.getContextSortByDimsFirst(),
        query.getDimensions(),
        Long.MAX_VALUE,
        valueTypes,
        aggregatorFactories,
        null,
        mergedDictionariesSupplier
    );

    // TODO: check deadlock
    final ReferenceCountingResourceHolder<ByteBuffer> mergeBufferHolder = Iterables.getOnlyElement(mergeBufferPool.takeBatch(1));
    final CloseableGrouperIterator<RowBasedKey, ResultRow> combineIterator = RowBasedGrouperHelper.makeGrouperIterator(
        mergeAndCombine(
            query,
            mergedDictionariesSupplier,
            mergeBufferHolder, // TODO: handle timeout and error better
            ConcurrentGrouper.getCombiningFactories(aggregatorFactories),
            combineKeySerdeFactory,
            priority,
            hasTimeout,
            queryTimeout,
            iterators
        ),
        query,
        null,
        mergeBufferHolder::close,
        mergedDictionariesSupplier
    );
    return Sequences.withBaggage(
        Sequences.simple(() -> combineIterator),
        combineIterator
    );
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
                  return row -> {
                    final PerSegmentEncodedResultRow perSegmentEncodedResultRow = (PerSegmentEncodedResultRow) row;
                    return mergedDictionarySupplier.get()[dimensionIndex - query.getResultRowDimensionStart()].getNewDictId(
                        perSegmentEncodedResultRow.getSegmentId(dimensionIndex),
                        perSegmentEncodedResultRow.getInt(dimensionIndex)
                    );
                  };
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

//  @Override
//  public Sequence<ResultRow> run(QueryPlus<ResultRow> queryPlus, ResponseContext responseContext)
//  {
//    final GroupByQuery query = (GroupByQuery) queryPlus.getQuery();
//
//    final QueryPlus<ResultRow> queryPlusForRunners = queryPlus
//        .withQuery(query)
//        .withoutThreadUnsafeState();
//
//    final int priority = QueryContexts.getPriority(query);
//
//    // Figure out timeoutAt
//    final long queryTimeout = QueryContexts.getTimeout(query);
//    final boolean hasTimeout = QueryContexts.hasTimeout(query);
//    final long timeoutAt = System.currentTimeMillis() + queryTimeout;
//
//    final Supplier<MergedDictionary[]> mergedDictionariesSupplier = createMergedDictionariesSupplier(
//        query,
//        hasTimeout,
//        timeoutAt
//    );
//
//    final List<Sequence<ResultRow>> sequences = FluentIterable
//        .from(queryables)
//        .transform(runner -> {
//          final Future<Yielder<ResultRow>> future = exec.submit(() -> Yielders.each(
//              runner.run(queryPlusForRunners, responseContext).map(row -> {
//                final MergedDictionary[] mergedDictionaries = mergedDictionariesSupplier.get(); // TODO: per row??
//                final PerSegmentEncodedResultRow encodedResultRow = (PerSegmentEncodedResultRow) row;
////          System.err.println("encodedResultRow: " + encodedResultRow + " decoded: " + decode(mergedDictionaries, encodedResultRow, query.getResultRowDimensionStart(), query.getResultRowAggregatorStart()));
//                final ResultRow resultRow = ResultRow.create(row.length());
//
//                for (int i = 0; i < query.getResultRowDimensionStart(); i++) {
//                  resultRow.set(i, encodedResultRow.get(i));
//                }
//
//                // (segmentId, oldDictId) -> newDictId
//                for (int i = query.getResultRowDimensionStart(); i < query.getResultRowAggregatorStart(); i++) {
//                  final int dimIndex = i - query.getResultRowDimensionStart();
//                  final MergedDictionary mergedDictionary = mergedDictionaries[dimIndex];
//                  resultRow.set(i, mergedDictionary.getNewDictId(encodedResultRow.getSegmentId(i), encodedResultRow.getInt(i)));
//                }
//
//                for (int i = query.getResultRowAggregatorStart(); i < row.length(); i++) {
//                  resultRow.set(i, encodedResultRow.get(i));
//                }
////          System.err.println(query.getIntervals() + " new row: " + resultRow + " decoded: " + decode(mergedDictionaries, resultRow, query.getResultRowDimensionStart(), query.getResultRowAggregatorStart()));
//                return resultRow;
//              })
//          ));
//          return (Sequence<ResultRow>) new LazySequence<>(() -> {
//            final Yielder<ResultRow> yielder;
//            try {
//              yielder = future.get();
//            }
//            catch (InterruptedException | ExecutionException e) {
//              throw new RuntimeException(e);
//            }
//            return new BaseSequence<>(
//                new IteratorMaker<ResultRow, CloseableIterator<ResultRow>>()
//                {
//                  @Override
//                  public CloseableIterator<ResultRow> make()
//                  {
//                    return Yielders.iterator(yielder);
//                  }
//
//                  @Override
//                  public void cleanup(CloseableIterator<ResultRow> iterFromMake)
//                  {
//                    try {
//                      iterFromMake.close();
//                    }
//                    catch (IOException e) {
//                      throw new RuntimeException(e);
//                    }
//                  }
//                }
//            );
//          });
//        })
//        .toList();
//
//    final AggregatorFactory[] aggregatorFactories = query.getAggregatorSpecs().toArray(new AggregatorFactory[0]);
//    final List<ValueType> valueTypes = DimensionHandlerUtils.getValueTypesFromDimensionSpecs(query.getDimensions());
//    final Grouper.KeySerdeFactory<RowBasedKey> combineKeySerdeFactory = new RowBasedKeySerdeFactory(
//        query.getResultRowHasTimestamp(),
//        query.getContextSortByDimsFirst(),
//        query.getDimensions(),
//        Long.MAX_VALUE,
//        valueTypes,
//        aggregatorFactories,
//        null,
//        mergedDictionariesSupplier
//    );
//
//    // TODO: check deadlock
//    final ReferenceCountingResourceHolder<ByteBuffer> mergeBufferHolder = Iterables.getOnlyElement(mergeBufferPool.takeBatch(1));
//    return Sequences.withBaggage(
//        mergeAndCombine(
//            query,
//            mergedDictionariesSupplier,
//            mergeBufferHolder, // TODO: handle timeout and error better
//            ConcurrentGrouper.getCombiningFactories(aggregatorFactories),
//            combineKeySerdeFactory,
//            priority,
//            hasTimeout,
//            queryTimeout,
//            sequences
//        ),
//        mergeBufferHolder
//    );
//  }

  private Sequence<ResultRow> mergeSequences(
      GroupByQuery query,
      List<Sequence<ResultRow>> sequences,
      Supplier<MergedDictionary[]> mergedDictionariesSupplier,
      int priority,
      boolean hasTimeout,
      long queryTimeout
  )
  {

    final ParallelMergeCombiningSequence<ResultRow> mergeCombiningSequence = new ParallelMergeCombiningSequence<>(
        pool,
        sequences,
        getRowOrdering(query),
        new GroupByBinaryFnV2(query),
        hasTimeout,
        queryTimeout,
        priority,
        QueryContexts.getParallelMergeParallelism(query, processingConfig.getMergePoolDefaultMaxQueryParallelism()),
        QueryContexts.getParallelMergeInitialYieldRows(query, processingConfig.getMergePoolTaskInitialYieldRows()),
        QueryContexts.getParallelMergeSmallBatchRows(query, processingConfig.getMergePoolSmallBatchRows()),
        processingConfig.getMergePoolTargetTaskRunTimeMillis(),
        metrics -> {} // TODO: metrics
    );
//    final CombiningSequence<ResultRow> mergeCombiningSequence = CombiningSequence.create(
//        new MergeSequence<>(getRowOrdering(query), Sequences.simple(sequences)),
//        getRowOrdering(query),
//        new GroupByBinaryFnV2(query)
//    );

    return new MappedSequence<>(
        mergeCombiningSequence,
        row -> {
          final MergedDictionary[] mergedDictionaries = mergedDictionariesSupplier.get(); // TODO: per row??
//          System.err.println(query.getIntervals() + " after merge row: " + row + " decoded: " + decode(mergedDictionaries, row, query.getResultRowDimensionStart(), query.getResultRowAggregatorStart()));
          for (int i = query.getResultRowDimensionStart(); i < query.getResultRowAggregatorStart(); i++) {
            final int dimIndex = i - query.getResultRowDimensionStart();
            final MergedDictionary mergedDictionary = mergedDictionaries[dimIndex];
            row.set(i, mergedDictionary.lookup(row.getInt(i)));
          }
          return row;
        }
    );
  }

  private CloseableIterator<Entry<RowBasedKey>> mergeAndCombine(
      GroupByQuery query,
      Supplier<MergedDictionary[]> mergedDictionariesSupplier,
      ReferenceCountingResourceHolder<ByteBuffer> combineBufferHolder,
      AggregatorFactory[] combiningFactories,
      KeySerdeFactory<RowBasedKey> combineKeySerdeFactory,
      int priority,
      boolean hasTimeout,
      long queryTimeout,
      List<? extends CloseableIterator<Entry<RowBasedKey>>> iterators
  )
  {
    final long timeoutAt = System.currentTimeMillis() + queryTimeout;
    final ParallelCombiner<RowBasedKey> sequenceCombiner = new ParallelCombiner<>(
        combineBufferHolder,
        combiningFactories,
        combineKeySerdeFactory,
        exec,
        false, // TODO: set properly
        processingConfig.getNumThreads(),
        priority,
        timeoutAt,
        1024 // TODO: not a param. the tree height can be always 2
    );

    return sequenceCombiner.combine(iterators, null);

    // TODO: something something similar to ParallelCombiner..
    // it should accept sequences instead of CloseableIterators
    // it should also use the mergedDictionarySupplier instead of dictionary
    // it should be able to spill when it's full
    //  - spilling should be a blocking operation for making things simple. in practice, reading and spilling will not likely happen at the same time.
    //  - it can retry a couple of times when it's full before it starts spilling
    //  - it can cancel spilling if some slots become available for writing
  }

  private ResultRow decode(MergedDictionary[] mergedDictionaries, ResultRow row, int dimStart, int dimEnd)
  {
    final ResultRow decoded = ResultRow.create(row.length());
    for (int i = 0; i < dimStart; i++) {
      decoded.set(i, row.get(i));
    }
    for (int i = dimStart; i < dimEnd; i++) {
      decoded.set(i, mergedDictionaries[i - dimStart].lookup(row.getInt(i)));
    }
    for (int i = dimEnd; i < row.length(); i++) {
      decoded.set(i, row.get(i));
    }
    return decoded;
  }

  private Supplier<MergedDictionary[]> createMergedDictionariesSupplier(
      GroupByQuery query,
      boolean hasTimeout,
      long timeoutAt
  )
  {
    final DictionaryMergeQuery dictionaryMergeQuery = new DictionaryMergeQuery(
        query.getDataSource(),
        query.getQuerySegmentSpec(),
        query.getDimensions()
    );
    final Sequence<List<Iterator<DictionaryConversion>>> conversionSequence = dictionaryMergingRunner.run(
        QueryPlus.wrap(dictionaryMergeQuery)
    );

    // TODO: maybe i don't need this accumulation at all if i just compute these maps in the queryRunner
    final ListenableFuture<MergingDictionary[]> dictionaryMergingFuture = exec.submit(() -> {
      final MergingDictionary[] merging = new MergingDictionary[query.getDimensions().size()];
      for (int i = 0; i < merging.length; i++) {
        merging[i] = new MergingDictionary(dictionaryMergingRunner.getNumQueryRunners());
      }
      return conversionSequence.accumulate(
          merging,
          (accumulated, conversions) -> {
            assert accumulated.length == conversions.size();
            for (int i = 0; i < conversions.size(); i++) {
              while (conversions.get(i).hasNext()) {
                final DictionaryConversion conversion = conversions.get(i).next();
                accumulated[i].add(conversion.getVal(), conversion.getSegmentId(), conversion.getOldDictionaryId());
              }
            }
            return accumulated;
          }
      );
    });
    return Suppliers.memoize(() -> waitForDictionaryMergeFutureCompletion(
        query,
        dictionaryMergingFuture,
        hasTimeout,
        timeoutAt - System.currentTimeMillis()
    ));
  }

  private MergedDictionary[] waitForDictionaryMergeFutureCompletion(
      GroupByQuery query,
      ListenableFuture<MergingDictionary[]> future,
      boolean hasTimeout,
      long timeout
  )
  {
    try {
      if (hasTimeout && timeout <= 0) {
        throw new TimeoutException();
      }

      final MergingDictionary[] result;
      if (hasTimeout) {
        result = future.get(timeout, TimeUnit.MILLISECONDS);
      } else {
        result = future.get();
      }
      final MergedDictionary[] mergedDictionaries = new MergedDictionary[result.length];
      for (int i = 0; i < mergedDictionaries.length; i++) {
        mergedDictionaries[i] = result[i].toImmutable();
      }
      return mergedDictionaries;
    }
    catch (InterruptedException e) {
      log.warn(e, "Query interrupted, cancelling pending results, query id [%s]", query.getId());
      future.cancel(true);
      throw new QueryInterruptedException(e);
    }
    catch (CancellationException e) {
      future.cancel(true);
      throw new QueryInterruptedException(e);
    }
    catch (TimeoutException e) {
      log.info("Query timeout, cancelling pending results for query id [%s]", query.getId());
      future.cancel(true);
      throw new QueryInterruptedException(e);
    }
    catch (ExecutionException e) {
      future.cancel(true);
      throw new RuntimeException(e);
    }
  }

  public static class MergingDictionary
  {
    // segmentId -> original dict id -> new dict id
    private final Int2IntMap[] dictIdConversion;
    private final List<String> dictionary = new ArrayList<>();
    @Nullable
    private String prevVal = null;

    public MergingDictionary(int numSegments)
    {
      this.dictIdConversion = new Int2IntMap[numSegments];
      for (int i = 0; i < numSegments; i++) {
        this.dictIdConversion[i] = new Int2IntOpenHashMap();
      }
    }

    public void add(@Nullable String val, int segmentId, int oldDictId)
    {
      if (val != null) {
        final int newDictId = getNewDictId(val);
        dictIdConversion[segmentId].put(oldDictId, newDictId);
      }
    }

    private int getNewDictId(String val)
    {
      if (prevVal == null || !prevVal.equals(val)) {
        prevVal = val;
        dictionary.add(val);
      }
      return dictionary.size() - 1;
    }

    public MergedDictionary toImmutable()
    {
      return new MergedDictionary(dictIdConversion, dictionary);
    }
  }

  private Ordering<ResultRow> getRowOrdering(GroupByQuery query)
  {
    if (query.isApplyLimitPushDown()) {
      if (!DefaultLimitSpec.sortingOrderHasNonGroupingFields((DefaultLimitSpec) query.getLimitSpec(), query.getDimensions())) {
        return getRowOrderingForPushDown(query, (DefaultLimitSpec) query.getLimitSpec());
      }
    }

    final boolean sortByDimsFirst = query.getContextSortByDimsFirst();
    final Comparator<ResultRow> timeComparator = query.getTimeComparator(false);

    if (timeComparator == null) {
      return Ordering.from((lhs, rhs) -> compareDims(query, lhs, rhs));
    } else if (sortByDimsFirst) {
      return Ordering.from(
          (lhs, rhs) -> {
            final int cmp = compareDims(query, lhs, rhs);
            if (cmp != 0) {
              return cmp;
            }

            return timeComparator.compare(lhs, rhs);
          }
      );
    } else {
      return Ordering.from(
          (lhs, rhs) -> {
            final int timeCompare = timeComparator.compare(lhs, rhs);

            if (timeCompare != 0) {
              return timeCompare;
            }

            return compareDims(query, lhs, rhs);
          }
      );
    }
  }

  private int compareDims(GroupByQuery query, ResultRow lhs, ResultRow rhs)
  {
    final int dimensionStart = query.getResultRowDimensionStart();
    final List<DimensionSpec> dimensions = query.getDimensions();

    for (int i = 0; i < dimensions.size(); i++) {
      DimensionSpec dimension = dimensions.get(i);
      final int dimCompare;
      if (dimension.getOutputType() == ValueType.STRING) {
        // integer dictionary id
        dimCompare = Integer.compare(lhs.getInt(dimensionStart + i), rhs.getInt(dimensionStart + i));
      } else {
        dimCompare = DimensionHandlerUtils.compareObjectsAsType(
            lhs.get(dimensionStart + i),
            rhs.get(dimensionStart + i),
            dimension.getOutputType()
        );
      }
      if (dimCompare != 0) {
        return dimCompare;
      }
    }

    return 0;
  }

  private Ordering<ResultRow> getRowOrderingForPushDown(GroupByQuery query, DefaultLimitSpec limitSpec)
  {
    final boolean sortByDimsFirst = query.getContextSortByDimsFirst();
    final List<DimensionSpec> dimensions = query.getDimensions();
    final RowSignature resultRowSignature = query.getResultRowSignature();

    final IntList orderedFieldNumbers = new IntArrayList();
    final Set<Integer> dimsInOrderBy = new HashSet<>();
    final List<Boolean> needsReverseList = new ArrayList<>();
    final List<ValueType> dimensionTypes = new ArrayList<>();
    final List<StringComparator> comparators = new ArrayList<>();

    for (OrderByColumnSpec orderSpec : limitSpec.getColumns()) {
      boolean needsReverse = orderSpec.getDirection() != OrderByColumnSpec.Direction.ASCENDING;
      int dimIndex = OrderByColumnSpec.getDimIndexForOrderBy(orderSpec, dimensions);
      if (dimIndex >= 0) {
        DimensionSpec dim = dimensions.get(dimIndex);
        orderedFieldNumbers.add(resultRowSignature.indexOf(dim.getOutputName()));
        dimsInOrderBy.add(dimIndex);
        needsReverseList.add(needsReverse);
        final ValueType type = dimensions.get(dimIndex).getOutputType();
        dimensionTypes.add(type);
        comparators.add(orderSpec.getDimensionComparator());
      }
    }

    for (int i = 0; i < dimensions.size(); i++) {
      if (!dimsInOrderBy.contains(i)) {
        orderedFieldNumbers.add(resultRowSignature.indexOf(dimensions.get(i).getOutputName()));
        needsReverseList.add(false);
        final ValueType type = dimensions.get(i).getOutputType();
        dimensionTypes.add(type);
        comparators.add(StringComparators.LEXICOGRAPHIC);
      }
    }

    final Comparator<ResultRow> timeComparator = query.getTimeComparator(false);

    if (timeComparator == null) {
      return Ordering.from(
          (lhs, rhs) -> compareDimsForLimitPushDown(
              orderedFieldNumbers,
              needsReverseList,
              dimensionTypes,
              comparators,
              lhs,
              rhs
          )
      );
    } else if (sortByDimsFirst) {
      return Ordering.from(
          (lhs, rhs) -> {
            final int cmp = compareDimsForLimitPushDown(
                orderedFieldNumbers,
                needsReverseList,
                dimensionTypes,
                comparators,
                lhs,
                rhs
            );
            if (cmp != 0) {
              return cmp;
            }

            return timeComparator.compare(lhs, rhs);
          }
      );
    } else {
      return Ordering.from(
          (lhs, rhs) -> {
            final int timeCompare = timeComparator.compare(lhs, rhs);

            if (timeCompare != 0) {
              return timeCompare;
            }

            return compareDimsForLimitPushDown(
                orderedFieldNumbers,
                needsReverseList,
                dimensionTypes,
                comparators,
                lhs,
                rhs
            );
          }
      );
    }
  }

  private static int compareDimsForLimitPushDown(
      final IntList fields,
      final List<Boolean> needsReverseList,
      final List<ValueType> dimensionTypes,
      final List<StringComparator> comparators,
      final ResultRow lhs,
      final ResultRow rhs
  )
  {
    for (int i = 0; i < fields.size(); i++) {
      final int fieldNumber = fields.getInt(i);
      final StringComparator comparator = comparators.get(i);
      final ValueType dimensionType = dimensionTypes.get(i);

      final int dimCompare;
      final Object lhsObj = lhs.get(fieldNumber);
      final Object rhsObj = rhs.get(fieldNumber);

      if (ValueType.isNumeric(dimensionType)) {
        if (comparator.equals(StringComparators.NUMERIC)) {
          dimCompare = DimensionHandlerUtils.compareObjectsAsType(lhsObj, rhsObj, dimensionType);
        } else {
          dimCompare = comparator.compare(String.valueOf(lhsObj), String.valueOf(rhsObj));
        }
      } else {
//        dimCompare = comparator.compare((String) lhsObj, (String) rhsObj);
        // TODO: handle when the comparator is not lexicographic
        dimCompare = Integer.compare((int) lhsObj, (int) rhsObj);
      }

      if (dimCompare != 0) {
        return needsReverseList.get(i) ? -dimCompare : dimCompare;
      }
    }
    return 0;
  }
}
