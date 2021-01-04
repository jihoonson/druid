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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Predicate;
import com.google.common.base.Predicates;
import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.Iterables;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.SettableFuture;
import org.apache.datasketches.memory.Memory;
import org.apache.druid.collections.BlockingPool;
import org.apache.druid.collections.ReferenceCountingResourceHolder;
import org.apache.druid.collections.Releaser;
import org.apache.druid.common.guava.SettableSupplier;
import org.apache.druid.java.util.common.CloseableIterators;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.guava.BaseSequence;
import org.apache.druid.java.util.common.guava.CloseQuietly;
import org.apache.druid.java.util.common.guava.Sequence;
import org.apache.druid.java.util.common.io.Closer;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.java.util.common.parsers.CloseableIterator;
import org.apache.druid.query.AbstractPrioritizedCallable;
import org.apache.druid.query.ColumnSelectorPlus;
import org.apache.druid.query.DictionaryConversion;
import org.apache.druid.query.DictionaryMergeQuery;
import org.apache.druid.query.DictionaryMergingQueryRunner;
import org.apache.druid.query.QueryContexts;
import org.apache.druid.query.QueryInterruptedException;
import org.apache.druid.query.QueryPlus;
import org.apache.druid.query.QueryRunner;
import org.apache.druid.query.QueryWatcher;
import org.apache.druid.query.ResourceLimitExceededException;
import org.apache.druid.query.SegmentGroupByQueryProcessor;
import org.apache.druid.query.aggregation.AggregatorAdapters;
import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.query.context.ResponseContext;
import org.apache.druid.query.dimension.DimensionSpec;
import org.apache.druid.query.filter.ValueMatcher;
import org.apache.druid.query.groupby.GroupByQuery;
import org.apache.druid.query.groupby.GroupByQueryConfig;
import org.apache.druid.query.groupby.PerSegmentEncodedResultRow;
import org.apache.druid.query.groupby.ResultRow;
import org.apache.druid.query.groupby.epinephelinae.GroupByMergingQueryRunnerV3.MergingDictionary;
import org.apache.druid.query.groupby.epinephelinae.GroupByQueryEngineV2.GroupByEngineKeySerde;
import org.apache.druid.query.groupby.epinephelinae.Grouper.Entry;
import org.apache.druid.query.groupby.epinephelinae.column.GroupByColumnSelectorPlus;
import org.apache.druid.query.groupby.epinephelinae.column.GroupByColumnSelectorStrategy;
import org.apache.druid.query.groupby.epinephelinae.vector.VectorGroupByEngine2.TimestampedIterator;
import org.apache.druid.query.monomorphicprocessing.RuntimeShapeInspector;
import org.apache.druid.segment.AbstractDimensionSelector;
import org.apache.druid.segment.ColumnSelectorFactory;
import org.apache.druid.segment.ColumnValueSelector;
import org.apache.druid.segment.DimensionHandlerUtils;
import org.apache.druid.segment.DimensionSelector;
import org.apache.druid.segment.IdLookup;
import org.apache.druid.segment.column.ColumnCapabilities;
import org.apache.druid.segment.column.ColumnCapabilitiesImpl;
import org.apache.druid.segment.column.ValueType;
import org.apache.druid.segment.data.IndexedInts;
import org.apache.druid.segment.data.SingleIndexedInt;
import org.joda.time.DateTime;

import javax.annotation.Nullable;
import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class GroupByShuffleMergingQueryRunner implements QueryRunner<ResultRow>
{
  private static final Logger log = new Logger(GroupByMergingQueryRunnerV2.class);

  private final GroupByQueryConfig config;
  private final Iterable<SegmentGroupByQueryProcessor<ResultRow>> queryables;
  private final ListeningExecutorService exec;
  private final QueryWatcher queryWatcher;
  private final int concurrencyHint;
  private final BlockingPool<ByteBuffer> mergeBufferPool;
  private final ObjectMapper spillMapper;
  private final String processingTmpDir;
  private final int mergeBufferSize;
  private final @Nullable DictionaryMergingQueryRunner dictionaryMergingRunner;
  private final int numHashBuckets;

  public GroupByShuffleMergingQueryRunner(
      GroupByQueryConfig config,
      ExecutorService exec,
      QueryWatcher queryWatcher,
      Iterable<SegmentGroupByQueryProcessor<ResultRow>> queryables,
      int concurrencyHint,
      BlockingPool<ByteBuffer> mergeBufferPool,
      int mergeBufferSize,
      ObjectMapper spillMapper,
      String processingTmpDir,
      @Nullable DictionaryMergingQueryRunner dictionaryMergingRunner
  )
  {
    this.config = config;
    this.exec = MoreExecutors.listeningDecorator(exec);
    this.queryWatcher = queryWatcher;
    this.queryables = Iterables.unmodifiableIterable(Iterables.filter(queryables, Predicates.notNull()));
    this.concurrencyHint = concurrencyHint;
    this.mergeBufferPool = mergeBufferPool;
    this.spillMapper = spillMapper;
    this.processingTmpDir = processingTmpDir;
    this.mergeBufferSize = mergeBufferSize;
    this.dictionaryMergingRunner = dictionaryMergingRunner;
    this.numHashBuckets = concurrencyHint;
  }

  @Override
  public Sequence<ResultRow> run(QueryPlus<ResultRow> queryPlus, ResponseContext responseContext)
  {
    final GroupByQuery query = (GroupByQuery) queryPlus.getQuery();
    final GroupByQueryConfig querySpecificConfig = config.withOverrides(query);

    final QueryPlus<ResultRow> queryPlusForRunners = queryPlus
        .withQuery(query)
        .withoutThreadUnsafeState();

    final File temporaryStorageDirectory = new File(
        processingTmpDir,
        StringUtils.format("druid-groupBy-%s_%s", UUID.randomUUID(), query.getId())
    );

    final int priority = QueryContexts.getPriority(query);

    // Figure out timeoutAt time now, so we can apply the timeout to both the mergeBufferPool.take and the actual
    // query processing together.
    final long queryTimeout = QueryContexts.getTimeout(query);
    final boolean hasTimeout = QueryContexts.hasTimeout(query);
    final long timeoutAt = System.currentTimeMillis() + queryTimeout;

    return new BaseSequence<>(
        new BaseSequence.IteratorMaker<ResultRow, CloseableIterator<ResultRow>>()
        {
          @Override
          public CloseableIterator<ResultRow> make()
          {
            final Closer resources = Closer.create();

            try {
              final LimitedTemporaryStorage temporaryStorage = new LimitedTemporaryStorage(
                  temporaryStorageDirectory,
                  querySpecificConfig.getMaxOnDiskStorage()
              );
              final ReferenceCountingResourceHolder<LimitedTemporaryStorage> temporaryStorageHolder =
                  ReferenceCountingResourceHolder.fromCloseable(temporaryStorage);
              resources.register(temporaryStorageHolder);

              // If parallelCombine is enabled, we need two merge buffers for parallel aggregating and parallel combining
              final int numMergeBuffers = querySpecificConfig.getNumParallelCombineThreads() > 1 ? 2 : 1;

              final List<ReferenceCountingResourceHolder<ByteBuffer>> mergeBufferHolders = getMergeBuffersHolder(
                  numMergeBuffers,
                  hasTimeout,
                  timeoutAt
              );
              resources.registerAll(mergeBufferHolders);

              final ReferenceCountingResourceHolder<ByteBuffer> mergeBufferHolder = mergeBufferHolders.get(0);
              final int sliceSize = mergeBufferHolder.get().capacity() / numHashBuckets; // TODO: what is mergeBufferSize for?

              final Supplier<MergedDictionary[]> mergedDictionariesSupplier;
              if (config.isEarlyDictMerge() && dictionaryMergingRunner != null) { // TODO: no null check
                final List<ListenableFuture<MergingDictionary[]>> dictionaryMergingFutures = new ArrayList<>(query.getDimensions().size());
                for (int i = 0; i < query.getDimensions().size(); i++) {
                  final List<DimensionSpec> dimension = Collections.singletonList(query.getDimensions().get(i));
                  final DictionaryMergeQuery dictionaryMergeQuery = new DictionaryMergeQuery(
                      query.getDataSource(),
                      query.getQuerySegmentSpec(),
                      dimension
                  );
                  final Sequence<List<Iterator<DictionaryConversion>>> conversionSequence = dictionaryMergingRunner.run(
                      QueryPlus.wrap(dictionaryMergeQuery)
                  );
                  // TODO: maybe i don't need this accumulation at all if i just compute these maps in the queryRunner
                  dictionaryMergingFutures.add(
                      exec.submit(
                          new AbstractPrioritizedCallable<MergingDictionary[]>(priority)
                          {
                            @Override
                            public MergingDictionary[] call()
                            {
                              final MergingDictionary[] merging = new MergingDictionary[1];
                              merging[0] = new MergingDictionary(dictionaryMergingRunner.getNumQueryRunners());
                              return conversionSequence.accumulate(
                                  merging,
                                  (accumulated, conversions) -> {
//                                    assert accumulated.length == conversions.size();
                                    assert conversions.size() == 1;
                                    while (conversions.get(0).hasNext()) {
                                      final DictionaryConversion conversion = conversions.get(0).next();
                                      accumulated[0].add(
                                          conversion.getVal(),
                                          conversion.getSegmentId(),
                                          conversion.getOldDictionaryId()
                                      );
                                    }
                                    return accumulated;
                                  }
                              );
                            }
                          })
                  );
                }

                // TODO: maybe i don't need this accumulation at all if i just compute these maps in the queryRunner
                final ListenableFuture<List<MergingDictionary[]>> dictionaryMergingFuture = Futures.allAsList(dictionaryMergingFutures);
                mergedDictionariesSupplier = Suppliers.memoize(() -> waitForDictionaryMergeFutureCompletion(
                    query,
                    dictionaryMergingFuture,
                    hasTimeout,
                    timeoutAt - System.currentTimeMillis()
                ));
              } else {
                // TODO: make it not nullable
                mergedDictionariesSupplier = null;
              }

              List<CloseableIterator<TimestampedIterators>> hashedSequencesList = FluentIterable
                  .from(queryables)
                  .transform(runner2 -> runner2.process(queryPlusForRunners, responseContext))
                  .toList();

              final ProcessingCallableScheduler processingCallableScheduler = new ProcessingCallableScheduler(
                  exec,
                  priority,
                  concurrencyHint,
                  numHashBuckets,
                  hashedSequencesList.size()
              );

              // this iterator is blocking whenever hasNext() is called. Maybe better to block when next() is called.
              TimeSortedIterators timeSortedHashedIterators = new TimeSortedIterators(
                  hashedSequencesList,
                  processingCallableScheduler,
                  priority
              );

              final int numDimensions = query.getResultRowAggregatorStart() - query.getResultRowDimensionStart();
              final int[] keyOffsets = new int[numDimensions];
              int offset = 0;
              int sumTypeSize = 0;
              for (int dimIndex = 0; dimIndex < numDimensions; dimIndex++) {
                keyOffsets[dimIndex] = offset;
                offset += typeSize(query.getDimensions().get(dimIndex).getOutputType());
                sumTypeSize += typeSize(query.getDimensions().get(dimIndex).getOutputType());
              }
              final int keySize = sumTypeSize;
              final List<ColumnCapabilities> dimensionCapabilities = IntStream
                  .range(0, numDimensions)
                  .mapToObj(i -> ColumnCapabilitiesImpl.createDefault())
                  .collect(Collectors.toList());

              return new CloseableIterator<ResultRow>()
              {
                final List<TimestampedIterator<Entry<Memory>>>[] partitionedIterators = new List[numHashBuckets];
                @Nullable CloseableIterator<ResultRow> delegate;
                @Nullable DateTime currentTime = null;

                @Override
                public boolean hasNext()
                {
                  while ((delegate == null || !delegate.hasNext()) && timeSortedHashedIterators.hasNext()) {
                    if (delegate != null) {
                      try {
                        delegate.close();
                      }
                      catch (IOException e) {
                        throw new RuntimeException(e);
                      }
                    }
                    for (int i = 0; i < numHashBuckets; i++) {
                      partitionedIterators[i] = new ArrayList<>();
                    }
                    List<TimestampedIterators> iteratorsOfSameTimestamp = timeSortedHashedIterators.next();
                    currentTime = iteratorsOfSameTimestamp.get(0).getTimestamp();
                    for (TimestampedIterators eachIterators : iteratorsOfSameTimestamp) {
                      for (int i = 0; i < eachIterators.iterators.length; i++) {
                        partitionedIterators[i].add(eachIterators.iterators[i]);
                      }
                    }

                    delegate = nextDelegate(
                        processingCallableScheduler,
                        currentTime,
                        mergedDictionariesSupplier,
                        partitionedIterators,
                        keySize,
                        keyOffsets,
                        mergeBufferHolder,
                        sliceSize,
                        dimensionCapabilities
                    );
                  }
                  return delegate != null && delegate.hasNext();
                }

                @Override
                public ResultRow next()
                {
                  if (delegate == null || !delegate.hasNext()) {
                    throw new NoSuchElementException();
                  }
                  return delegate.next();
                }

                @Override
                public void close() throws IOException
                {
                  Closer closer = Closer.create();
                  closer.register(() -> processingCallableScheduler.shutdown());
                  if (delegate != null) {
                    closer.register(delegate);
                  }
                  closer.register(timeSortedHashedIterators);
                  closer.register(resources);
                  closer.close();
                }
              };
            }
            catch (Throwable t) {
              // Exception caught while setting up the iterator; release resources.
              try {
                resources.close();
              }
              catch (Exception ex) {
                t.addSuppressed(ex);
              }
              throw new RuntimeException(t); // TODO: proper exception handling
            }
          }

          private CloseableIterator<ResultRow> nextDelegate(
              ProcessingCallableScheduler processingCallableScheduler,
              @Nullable DateTime currentTime,
              Supplier<MergedDictionary[]> mergedDictionariesSupplier,
              List<TimestampedIterator<Entry<Memory>>>[] partitionedIterators,
              int keySize,
              int[] keyOffsets,
              ReferenceCountingResourceHolder<ByteBuffer> mergeBufferHolder,
              int sliceSize,
              List<ColumnCapabilities> dimensionCapabilities
          )
          {
            List<AggregatorFactory> combiningFactories = query
                .getAggregatorSpecs()
                .stream()
                .map(AggregatorFactory::getCombiningFactory)
                .collect(Collectors.toList());

            final int numDims = query.getResultRowAggregatorStart() - query.getResultRowDimensionStart();
            final List<ListenableFuture<CloseableIterator<ResultRow>>> futures = new ArrayList<>();
            for (int i = 0; i < numHashBuckets; i++) {
              // TODO: should not do this
              final byte[] buf = new byte[keySize];
              final CloseableIterator<Entry<ByteBuffer>> concatIterator = CloseableIterators.concat(
                  partitionedIterators[i]
              ).map(entry -> {
                final Memory keyMemory = entry.getKey();
                final ByteBuffer keyBuffer = ByteBuffer.wrap(buf);

                // Convert dictionary
                for (int j = 0; j < numDims; j++) {
                  if (query.getDimensions().get(j).getOutputType() == ValueType.STRING) {
                    if (mergedDictionariesSupplier != null) {
                      final int oldDictId = keyMemory.getInt(keyOffsets[j]);
                      assert entry.segmentId > -1;
                      final int newDictId = mergedDictionariesSupplier.get()[j].getNewDictId(
                          entry.segmentId,
                          oldDictId
                      );
                      keyBuffer.putInt(keyOffsets[j], newDictId);
                    } else {
                      throw new UnsupportedOperationException();
                    }
                  } else {
                    final int thisKeyLen = j == numDims - 1 ? (keySize - keyOffsets[j]) : (keyOffsets[j + 1] - keyOffsets[j]);
                    keyMemory.getByteArray(keyOffsets[j], buf, keyOffsets[j], thisKeyLen);
                  }
                }

//                System.err.println(Thread.currentThread() + ", dict conversion: " + Groupers.deserializeToRow(entry.segmentId, keyMemory, entry.getValues()) + " -> " + Groupers.deserializeToRow(-1, keyBuffer, entry.getValues()));

                return new Entry<>(keyBuffer, entry.getValues());
              });

              final SettableSupplier<Entry<ByteBuffer>> currentBufferSupplier = new SettableSupplier<>();
              final ColumnSelectorFactory columnSelectorFactory = new EntryColumnSelectorFactory(
                  query,
                  currentBufferSupplier,
                  keyOffsets,
                  mergedDictionariesSupplier,
                  combiningFactories
              );
              final ColumnSelectorPlus<GroupByColumnSelectorStrategy>[] selectorPlus = DimensionHandlerUtils
                  .createColumnSelectorPluses(
                      querySpecificConfig.isEarlyDictMerge()
                      ? GroupByQueryEngineV2.STRATEGY_FACTORY_ENCODE_STRINGS
                      : GroupByQueryEngineV2.STRATEGY_FACTORY_NO_ENCODE,
                      query.getDimensions(),
                      columnSelectorFactory
                  );
              final GroupByColumnSelectorPlus[] dims = GroupByQueryEngineV2.createGroupBySelectorPlus(
                  selectorPlus,
                  query.getResultRowDimensionStart()
              );

              // TODO: maybe can reuse
              final Grouper<ByteBuffer> grouper = new BufferHashGrouper<>(
                  Suppliers.ofInstance(Groupers.getSlice(mergeBufferHolder.get(), sliceSize, i)),
                  new GroupByEngineKeySerde(dims, query),
                  AggregatorAdapters.factorizeBuffered(
                      columnSelectorFactory,
                      combiningFactories
                  ),
                  querySpecificConfig.getBufferGrouperMaxSize(),
                  querySpecificConfig.getBufferGrouperMaxLoadFactor(),
                  querySpecificConfig.getBufferGrouperInitialBuckets(),
                  true
              );
              grouper.init();
              final SettableFuture<CloseableIterator<ResultRow>> resultFuture = SettableFuture.create();
              processingCallableScheduler.schedule(
                  new ProcessingTask<CloseableIterator<ResultRow>>()
                  {
                    @Override
                    public CloseableIterator<ResultRow> run() throws Exception
                    {
                      //noinspection unused
                      try (Releaser releaser = mergeBufferHolder.increment();
                           CloseableIterator<Entry<ByteBuffer>> iteratorCloser = concatIterator) {
                        // TODO: should do something like in groupByQueryEngineV2 when earlyDictMerge = false
                        while (concatIterator.hasNext()) {
                          final Entry<ByteBuffer> entry = concatIterator.next();
                          currentBufferSupplier.set(entry);
                          grouper.aggregate(entry.getKey());
                        }
                        return CloseableIterators.wrap(
                            grouper.iterator(true)
                                   .map(entry -> {
                                     final PerSegmentEncodedResultRow resultRow = PerSegmentEncodedResultRow.create(
                                         query.getResultRowSizeWithoutPostAggregators()
                                     );
                                     if (query.getResultRowHasTimestamp()) {
                                       assert currentTime != null;
                                       resultRow.set(0, currentTime.getMillis());
                                     }

                                     for (int j = 0; j < dims.length; j++) {
                                       dims[j].getColumnSelectorStrategy().processValueFromGroupingKey(
                                           dims[j],
                                           entry.getKey(),
                                           resultRow,
                                           dims[j].getKeyBufferPosition(),
                                           0
                                       );
                                       // Decode dictionaries
                                       if (query.getDimensions().get(j).getOutputType() == ValueType.STRING) {
                                         resultRow.set(
                                             dims[j].getResultRowPosition(),
                                             mergedDictionariesSupplier.get()[j].lookup(resultRow.getInt(dims[j].getResultRowPosition()))
                                         );
                                       }
                                     }
                                     GroupByQueryEngineV2.convertRowTypesToOutputTypes(
                                         query.getDimensions(),
                                         resultRow,
                                         query.getResultRowDimensionStart(),
                                         0,
                                         dimensionCapabilities,
                                         false
                                     );

                                     for (int j = 0; j < entry.getValues().length; j++) {
                                       resultRow.set(
                                           query.getResultRowAggregatorStart() + j,
                                           0,
                                           entry.getValues()[j]
                                       );
                                     }
//                                     System.err.println(Thread.currentThread() + ", row conversion: " + Groupers.deserializeToRow(-1, entry.getKey(), entry.getValues()) + " -> " + resultRow);
                                     return resultRow;
                                   }),
                            grouper
                        );
                      }
                    }

                    @Override
                    public SettableFuture<CloseableIterator<ResultRow>> getResultFuture()
                    {
                      return resultFuture;
                    }

                    @Override
                    public boolean isSentinel()
                    {
                      return false;
                    }
                  }
              );

              futures.add(resultFuture);
            }

            // TODO: handle timeout
            try {
              Futures.allAsList(futures).get();
            }
            catch (InterruptedException | ExecutionException e) {
              throw new RuntimeException(e); // TODO: exception handling
            }
            final List<CloseableIterator<ResultRow>> resultIterators = futures.stream().map(f -> {
              try {
                return f.get();
              }
              catch (InterruptedException | ExecutionException e) {
                throw new RuntimeException(e); // TODO: exception handling
              }
            }).collect(Collectors.toList());

            return CloseableIterators.mergeSorted(
                resultIterators,
                query.getRowOrdering(true)
            );
          }

          @Override
          public void cleanup(CloseableIterator<ResultRow> iterFromMake)
          {
            try {
              iterFromMake.close();
            }
            catch (IOException e) {
              CloseQuietly.close(iterFromMake);
            }
          }
        }
    );
  }

  private int typeSize(ValueType valueType)
  {
    switch (valueType) {
      case DOUBLE:
        return Double.BYTES;
      case FLOAT:
        return Float.BYTES;
      case LONG:
        return Long.BYTES;
      case STRING:
        return Integer.BYTES;
      default:
        throw new UnsupportedOperationException(valueType.name());
    }
  }

  private static class EntryColumnSelectorFactory implements ColumnSelectorFactory
  {
    private final GroupByQuery query;
    private final Supplier<Entry<ByteBuffer>> currentEntrySupplier;
    private final int[] keyOffsets;
    private final Supplier<MergedDictionary[]> mergedDictionariesSupplier;
    private final List<AggregatorFactory> combiningFactories;

    private EntryColumnSelectorFactory(
        GroupByQuery query,
        Supplier<Entry<ByteBuffer>> currentEntrySupplier,
        int[] keyOffsets,
        Supplier<MergedDictionary[]> mergedDictionariesSupplier,
        List<AggregatorFactory> combiningFactories
    )
    {
      this.query = query;
      this.currentEntrySupplier = currentEntrySupplier;
      this.keyOffsets = keyOffsets;
      this.mergedDictionariesSupplier = mergedDictionariesSupplier;
      this.combiningFactories = combiningFactories;
    }

    @Override
    public DimensionSelector makeDimensionSelector(DimensionSpec dimensionSpec)
    {
      // TODO: should be able to handle non-string types
      final int dimIndex = dimIndex(dimensionSpec.getDimension());
      if (dimIndex < 0) {
        throw new ISE("Cannot find dimension[%s]", dimensionSpec.getDimension());
      }

      return new AbstractDimensionSelector()
      {
        final SingleIndexedInt indexedInts = new SingleIndexedInt();

        private int getCurrentVal()
        {
          return currentEntrySupplier.get().getKey().getInt(keyOffsets[dimIndex]);
        }

        @Override
        public IndexedInts getRow()
        {
          indexedInts.setValue(getCurrentVal());
          return indexedInts;
        }

        @Override
        public ValueMatcher makeValueMatcher(@Nullable String value)
        {
          return new ValueMatcher()
          {
            @Override
            public boolean matches()
            {
              return Objects.equals(
                  mergedDictionariesSupplier.get()[dimIndex].lookup(getCurrentVal()),
                  value
              );
            }

            @Override
            public void inspectRuntimeShape(RuntimeShapeInspector inspector)
            {
            }
          };
        }

        @Override
        public ValueMatcher makeValueMatcher(Predicate<String> predicate)
        {
          return new ValueMatcher()
          {
            @Override
            public boolean matches()
            {
              return predicate.apply(mergedDictionariesSupplier.get()[dimIndex].lookup(getCurrentVal()));
            }

            @Override
            public void inspectRuntimeShape(RuntimeShapeInspector inspector)
            {
            }
          };
        }

        @Override
        public void inspectRuntimeShape(RuntimeShapeInspector inspector)
        {
        }

        @Override
        public Class<?> classOfObject()
        {
          return String.class;
        }

        @Override
        public int getValueCardinality()
        {
          return mergedDictionariesSupplier.get()[dimIndex].size();
        }

        @Nullable
        @Override
        public String lookupName(int id)
        {
          return mergedDictionariesSupplier.get()[dimIndex].lookup(id);
        }

        @Override
        public boolean nameLookupPossibleInAdvance()
        {
          return true;
        }

        @Nullable
        @Override
        public IdLookup idLookup()
        {
          return null;
        }
      };
    }

    private int dimIndex(String columnName)
    {
      return IntStream.range(0, query.getResultRowAggregatorStart() - query.getResultRowDimensionStart())
                      .filter(i -> query.getDimensions().get(i).getDimension().equals(columnName))
                      .findAny()
                      .orElse(-1);
    }

    private int aggIndex(String columnName)
    {
      return IntStream.range(0, query.getResultRowPostAggregatorStart() - query.getResultRowAggregatorStart())
                      // TODO: this seems.. wrong
                      .filter(i -> combiningFactories.get(i).requiredFields().contains(columnName))
                      .findAny()
                      .orElse(-1);
    }

    private ValueType dimType(int dimIndex)
    {
      return query.getDimensions().get(dimIndex).getOutputType();
    }

    private ValueType aggType(int aggIndex)
    {
      return combiningFactories.get(aggIndex).getType();
    }

    @Override
    public ColumnValueSelector makeColumnValueSelector(String columnName)
    {
      final int dimIndex = dimIndex(columnName);
      if (dimIndex > -1) {
        final ValueType valueType = dimType(dimIndex);
        return new ColumnValueSelector()
        {
          @Override
          public double getDouble()
          {
            switch (valueType) {
              case DOUBLE:
                return currentEntrySupplier.get().getKey().getDouble(keyOffsets[dimIndex]);
              case FLOAT:
                return currentEntrySupplier.get().getKey().getFloat(keyOffsets[dimIndex]);
              case LONG:
                return currentEntrySupplier.get().getKey().getLong(keyOffsets[dimIndex]);
              case STRING:
                return currentEntrySupplier.get().getKey().getInt(keyOffsets[dimIndex]);
              default:
                throw new UnsupportedOperationException(valueType.name());
            }
          }

          @Override
          public float getFloat()
          {
            switch (valueType) {
              case DOUBLE:
                return (float) currentEntrySupplier.get().getKey().getDouble(keyOffsets[dimIndex]);
              case FLOAT:
                return currentEntrySupplier.get().getKey().getFloat(keyOffsets[dimIndex]);
              case LONG:
                return currentEntrySupplier.get().getKey().getLong(keyOffsets[dimIndex]);
              case STRING:
                return currentEntrySupplier.get().getKey().getInt(keyOffsets[dimIndex]);
              default:
                throw new UnsupportedOperationException(valueType.name());
            }
          }

          @Override
          public long getLong()
          {
            switch (valueType) {
              case DOUBLE:
                return (long) currentEntrySupplier.get().getKey().getDouble(keyOffsets[dimIndex]);
              case FLOAT:
                return (long) currentEntrySupplier.get().getKey().getFloat(keyOffsets[dimIndex]);
              case LONG:
                return currentEntrySupplier.get().getKey().getLong(keyOffsets[dimIndex]);
              case STRING:
                return currentEntrySupplier.get().getKey().getInt(keyOffsets[dimIndex]);
              default:
                throw new UnsupportedOperationException(valueType.name());
            }
          }

          @Override
          public void inspectRuntimeShape(RuntimeShapeInspector inspector)
          {
          }

          @Override
          public boolean isNull()
          {
            // TODO
            return false;
          }

          @Nullable
          @Override
          public Object getObject()
          {
            switch (valueType) {
              case DOUBLE:
                return currentEntrySupplier.get().getKey().getDouble(keyOffsets[dimIndex]);
              case FLOAT:
                return currentEntrySupplier.get().getKey().getFloat(keyOffsets[dimIndex]);
              case LONG:
                return currentEntrySupplier.get().getKey().getLong(keyOffsets[dimIndex]);
              case STRING:
                return currentEntrySupplier.get().getKey().getInt(keyOffsets[dimIndex]);
              default:
                throw new UnsupportedOperationException(valueType.name());
            }
          }

          @Override
          public Class classOfObject()
          {
            switch (valueType) {
              case DOUBLE:
                return Double.class;
              case FLOAT:
                return Float.class;
              case LONG:
                return Long.class;
              case STRING:
                return Integer.class;
              default:
                throw new UnsupportedOperationException(valueType.name());
            }
          }
        };
      } else {
        final int aggIndex = aggIndex(columnName);
        if (aggIndex < 0) {
          throw new ISE("Cannot find column[%s]", columnName);
        }
        final ValueType valueType = aggType(aggIndex);
        return new ColumnValueSelector()
        {
          @Override
          public double getDouble()
          {
            switch (valueType) {
              case DOUBLE:
              case FLOAT:
              case LONG:
                return ((Number) currentEntrySupplier.get().getValues()[aggIndex]).doubleValue();
              default:
                throw new UnsupportedOperationException(valueType.name());
            }
          }

          @Override
          public float getFloat()
          {
            switch (valueType) {
              case DOUBLE:
              case FLOAT:
              case LONG:
                return ((Number) currentEntrySupplier.get().getValues()[aggIndex]).floatValue();
              default:
                throw new UnsupportedOperationException(valueType.name());
            }
          }

          @Override
          public long getLong()
          {
            switch (valueType) {
              case DOUBLE:
              case FLOAT:
              case LONG:
                return ((Number) currentEntrySupplier.get().getValues()[aggIndex]).longValue();
              default:
                throw new UnsupportedOperationException(valueType.name());
            }
          }

          @Override
          public void inspectRuntimeShape(RuntimeShapeInspector inspector)
          {
          }

          @Override
          public boolean isNull()
          {
            // TODO
            return false;
          }

          @Nullable
          @Override
          public Object getObject()
          {
            switch (valueType) {
              case DOUBLE:
              case FLOAT:
              case LONG:
                return currentEntrySupplier.get().getValues()[aggIndex];
              default:
                throw new UnsupportedOperationException(valueType.name());
            }
          }

          @Override
          public Class classOfObject()
          {
            switch (valueType) {
              case DOUBLE:
                return Double.class;
              case FLOAT:
                return Float.class;
              case LONG:
                return Long.class;
              case STRING:
                return Integer.class;
              default:
                throw new UnsupportedOperationException(valueType.name());
            }
          }
        };
      }
    }

    @Nullable
    @Override
    public ColumnCapabilities getColumnCapabilities(String column)
    {
      final ValueType valueType;
      int colIndex = dimIndex(column);
      if (colIndex > -1) {
        valueType = dimType(colIndex);
      } else {
        colIndex = aggIndex(column);
        if (colIndex < 0) {
          throw new ISE("Cannot find column[%s]", column);
        }
        valueType = aggType(colIndex);
      }
      switch (valueType) {
        case DOUBLE:
        case FLOAT:
        case LONG:
          return ColumnCapabilitiesImpl.createSimpleNumericColumnCapabilities(valueType);
        case STRING:
          return ColumnCapabilitiesImpl.createDefault()
                                       .setType(ValueType.STRING)
                                       .setDictionaryEncoded(true)
                                       .setDictionaryValuesSorted(true)
                                       .setDictionaryValuesUnique(true);
        default:
          throw new UnsupportedOperationException(valueType.name());
      }
    }
  }

  private static class TimeSortedIterators implements CloseableIterator<List<TimestampedIterators>>
  {
    private final List<CloseableIterator<TimestampedIterators>> baseIterators;
    private final TimestampedIterators[] peeked;
    private final ListenableFuture<TimestampedIterators>[] futures;
    private final ProcessingCallableScheduler processingCallableScheduler;
    private final int priority;

    private TimeSortedIterators(
        List<CloseableIterator<TimestampedIterators>> baseIterators,
        ProcessingCallableScheduler processingCallableScheduler,
        int priority
    )
    {
      this.baseIterators = baseIterators;
      this.peeked = new TimestampedIterators[baseIterators.size()];
      this.futures = new ListenableFuture[baseIterators.size()];
      this.processingCallableScheduler = processingCallableScheduler;
      this.priority = priority;
    }

    @Override
    public boolean hasNext()
    {
      return Arrays.stream(peeked).anyMatch(Objects::nonNull) || baseIterators.stream().anyMatch(Iterator::hasNext);
    }

    @Override
    public List<TimestampedIterators> next()
    {
      if (!hasNext()) {
        throw new NoSuchElementException();
      }

      for (int i = 0; i < peeked.length; i++) {
        if (peeked[i] == null && baseIterators.get(i).hasNext()) {
          final CloseableIterator<TimestampedIterators> baseIterator = baseIterators.get(i);
          // baseIterator.next() computes hash aggregation on one segment (VectorGroupByEngineIterator.next())
          final SettableFuture<TimestampedIterators> resultFuture = SettableFuture.create();
          processingCallableScheduler.schedule(
              new ProcessingTask<TimestampedIterators>()
              {
                @Override
                public TimestampedIterators run()
                {
                  return baseIterator.next();
                }

                @Override
                public SettableFuture<TimestampedIterators> getResultFuture()
                {
                  return resultFuture;
                }

                @Override
                public boolean isSentinel()
                {
                  return false;
                }
              }
          );

          futures[i] = resultFuture;
        }
      }

      DateTime minTime = null;
      for (int i = 0; i < peeked.length; i++) {
        if (futures[i] != null) {
          try {
            peeked[i] = futures[i].get();
            futures[i] = null;
          }
          catch (InterruptedException | ExecutionException e) {
            throw new RuntimeException(e); // TODO: handle
          }
        }
        if (peeked[i] != null
            && (minTime == null || (peeked[i].getTimestamp() != null && minTime.isAfter(peeked[i].getTimestamp())))) {
          minTime = peeked[i].getTimestamp();
        }
      }

      final List<TimestampedIterators> iteratorsOfMinTime = new ArrayList<>();
      for (int i = 0; i < peeked.length; i++) {
        if (peeked[i] != null) {
          if (peeked[i].getTimestamp() == null && minTime == null
              || Objects.equals(peeked[i].getTimestamp(), minTime)) {
            iteratorsOfMinTime.add(peeked[i]);
            peeked[i] = null;
          }
        }
      }

      return iteratorsOfMinTime;
    }

    @Override
    public void close() throws IOException
    {
      Closer closer = Closer.create();
      closer.registerAll(baseIterators);
      closer.close();
    }
  }

  public static class ProcessingCallableScheduler
  {
    private final ExecutorService exec;
    private final int numCallables;
    private final List<ProcessingCallable> callables;
    private final List<Future<Void>> callableFutures;
    private final BlockingQueue<ProcessingTask> workQueue;

    public ProcessingCallableScheduler(
        ExecutorService exec,
        int priority,
        int concurrencyHint,
        int numHashBuckets,
        int numQueryables
    )
    {
      this.exec = exec;
      this.numCallables = Math.min(concurrencyHint, Math.max(numHashBuckets, numQueryables));
      this.callables = new ArrayList<>(numCallables);
      this.callableFutures = new ArrayList<>(numCallables);
      this.workQueue = new LinkedBlockingDeque<>();
      IntStream.range(0, numCallables).forEach(i -> {
        final ProcessingCallable callable = new ProcessingCallable(priority, workQueue);
        callableFutures.add(exec.submit(callable));
        callables.add(callable);
      });
    }

    public void schedule(ProcessingTask<?> task)
    {
      workQueue.offer(task);
    }

    public void shutdown()
    {
      // TODO: fix how to shut down
      callables.forEach(c -> {
        c.taskQueue.offer(ShutdownTask.SHUTDOWN_TASK);
      });
      for (Future<Void> future : callableFutures) {
        try {
          future.get(); // TODO: handle timeout
        }
        catch (InterruptedException | ExecutionException e) {
          throw new RuntimeException(e); // TODO: handle exceptions
        }
      }
    }
  }

  public interface ProcessingTask<T>
  {
    T run() throws Exception;

    SettableFuture<T> getResultFuture();

    boolean isSentinel(); // TODO: looks lame
  }

  private static class ShutdownTask implements ProcessingTask<Object>
  {
    private static final ShutdownTask SHUTDOWN_TASK = new ShutdownTask();

    @Override
    public Object run()
    {
      return null;
    }

    @Override
    public SettableFuture<Object> getResultFuture()
    {
      return null;
    }

    @Override
    public boolean isSentinel()
    {
      return true;
    }
  }

  public static class ProcessingCallable extends AbstractPrioritizedCallable<Void>
  {
    private final BlockingQueue<ProcessingTask> taskQueue;

    public ProcessingCallable(int priority, BlockingQueue<ProcessingTask> taskQueue)
    {
      super(priority);
      this.taskQueue = taskQueue;
    }

    @Override
    public Void call() throws Exception
    {
      while (!Thread.currentThread().isInterrupted()) {
        final ProcessingTask task = taskQueue.take();
        if (task != null) {
          if (task.isSentinel()) {
            break;
          }
          try {
            task.getResultFuture().set(task.run());
          }
          catch (Throwable e) {
            task.getResultFuture().setException(e);
          }
        }
      }
      return null;
    }
  }

//  private interface MemoryColumnFunction
//  {
//    int serializedSize();
//
//    Comparable[] deserialize(Memory memory);
//  }

  public static class TimestampedIterators implements Closeable
  {
    private final TimestampedIterator<Entry<Memory>>[] iterators;

    public TimestampedIterators(TimestampedIterator<Entry<Memory>>[] iterators)
    {
      this.iterators = iterators;
    }

    @Nullable
    DateTime getTimestamp()
    {
      return iterators[0].getTimestamp();
    }

    @Override
    public void close() throws IOException
    {
      Closer closer = Closer.create();
      Arrays.stream(iterators).forEach(closer::register);
      closer.close();
    }
  }

  private List<ReferenceCountingResourceHolder<ByteBuffer>> getMergeBuffersHolder(
      int numBuffers,
      boolean hasTimeout,
      long timeoutAt
  )
  {
    try {
      if (numBuffers > mergeBufferPool.maxSize()) {
        throw new ResourceLimitExceededException(
            "Query needs " + numBuffers + " merge buffers, but only "
            + mergeBufferPool.maxSize() + " merge buffers were configured. "
            + "Try raising druid.processing.numMergeBuffers."
        );
      }
      final List<ReferenceCountingResourceHolder<ByteBuffer>> mergeBufferHolder;
      // This will potentially block if there are no merge buffers left in the pool.
      if (hasTimeout) {
        final long timeout = timeoutAt - System.currentTimeMillis();
        if (timeout <= 0) {
          throw new TimeoutException();
        }
        if ((mergeBufferHolder = mergeBufferPool.takeBatch(numBuffers, timeout)).isEmpty()) {
          throw new TimeoutException("Cannot acquire enough merge buffers");
        }
      } else {
        mergeBufferHolder = mergeBufferPool.takeBatch(numBuffers);
      }
      return mergeBufferHolder;
    }
    catch (Exception e) {
      throw new QueryInterruptedException(e);
    }
  }

  private MergedDictionary[] waitForDictionaryMergeFutureCompletion(
      GroupByQuery query,
      ListenableFuture<List<MergingDictionary[]>> future,
      boolean hasTimeout,
      long timeout
  )
  {
    try {
      if (hasTimeout && timeout <= 0) {
        throw new TimeoutException();
      }

      final List<MergingDictionary[]> result;
      if (hasTimeout) {
        result = future.get(timeout, TimeUnit.MILLISECONDS);
      } else {
        result = future.get();
      }
      final MergedDictionary[] mergedDictionaries = new MergedDictionary[result.size()];
      for (int i = 0; i < mergedDictionaries.length; i++) {
        mergedDictionaries[i] = result.get(i)[0].toImmutable();
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

//  private void waitForFutureCompletion(
//      GroupByQuery query,
//      List<ListenableFuture<AggregateResult>> futures,
//      boolean hasTimeout,
//      long timeout
//  )
//  {
//    ListenableFuture<List<AggregateResult>> future = Futures.allAsList(futures);
//    try {
//      if (queryWatcher != null) {
//        queryWatcher.registerQueryFuture(query, future);
//      }
//
//      if (hasTimeout && timeout <= 0) {
//        throw new TimeoutException();
//      }
//
//      final List<AggregateResult> results = hasTimeout ? future.get(timeout, TimeUnit.MILLISECONDS) : future.get();
//
//      for (AggregateResult result : results) {
//        if (!result.isOk()) {
//          GuavaUtils.cancelAll(true, future, futures);
//          throw new ResourceLimitExceededException(result.getReason());
//        }
//      }
//    }
//    catch (InterruptedException e) {
//      log.warn(e, "Query interrupted, cancelling pending results, query id [%s]", query.getId());
//      GuavaUtils.cancelAll(true, future, futures);
//      throw new QueryInterruptedException(e);
//    }
//    catch (CancellationException e) {
//      GuavaUtils.cancelAll(true, future, futures);
//      throw new QueryInterruptedException(e);
//    }
//    catch (TimeoutException e) {
//      log.info("Query timeout, cancelling pending results for query id [%s]", query.getId());
//      GuavaUtils.cancelAll(true, future, futures);
//      throw new QueryInterruptedException(e);
//    }
//    catch (ExecutionException e) {
//      GuavaUtils.cancelAll(true, future, futures);
//      throw new RuntimeException(e);
//    }
//  }
//
//  private static class MemoryKeySerde implements KeySerde<Memory>
//  {
//    private final int keySize;
//    private final GroupByColumnSelectorPlus[] dims;
//    private final GroupByQuery query;
//
//    public MemoryKeySerde(GroupByColumnSelectorPlus[] dims, GroupByQuery query)
//    {
//      this.dims = dims;
//      int keySize = 0;
//      for (GroupByColumnSelectorPlus selectorPlus : dims) {
//        keySize += selectorPlus.getColumnSelectorStrategy().getGroupingKeySize();
//      }
//      this.keySize = keySize;
//
//      this.query = query;
//    }
//
//    @Override
//    public int keySize()
//    {
//      return keySize;
//    }
//
//    @Override
//    public Class<Memory> keyClazz()
//    {
//      return Memory.class;
//    }
//
//    @Override
//    public List<String> getDictionary()
//    {
//      return ImmutableList.of(); // TODO: this is for spilling dictionary. do i need it? or do we every need it if we use merged dictionary?
//    }
//
//    @Nullable
//    @Override
//    public ByteBuffer toByteBuffer(Memory key)
//    {
//      return key.getByteBuffer(); // TODO: is this correct?
//    }
//
//    @Override
//    public Memory fromByteBuffer(ByteBuffer buffer, int position)
//    {
//      return null;
//    }
//
//    @Override
//    public BufferComparator bufferComparator()
//    {
//      return null;
//    }
//
//    @Override
//    public BufferComparator bufferComparatorWithAggregators(
//        AggregatorFactory[] aggregatorFactories,
//        int[] aggregatorOffsets
//    )
//    {
//      return null;
//    }
//
//    @Override
//    public void reset()
//    {
//
//    }
//  }
}
