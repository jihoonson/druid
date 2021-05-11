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
import com.google.common.base.Preconditions;
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
import org.apache.datasketches.memory.WritableMemory;
import org.apache.druid.collections.BlockingPool;
import org.apache.druid.collections.ReferenceCountingResourceHolder;
import org.apache.druid.collections.Releaser;
import org.apache.druid.collections.ResourceHolder;
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
import org.apache.druid.query.Callables;
import org.apache.druid.query.DictionaryConversion;
import org.apache.druid.query.DictionaryMergeQuery;
import org.apache.druid.query.DictionaryMergingQueryRunner;
import org.apache.druid.query.GroupByQuerySegmentProcessor;
import org.apache.druid.query.QueryBlockingMemoryPool;
import org.apache.druid.query.QueryContexts;
import org.apache.druid.query.QueryInterruptedException;
import org.apache.druid.query.QueryPlus;
import org.apache.druid.query.QueryRunner;
import org.apache.druid.query.QueryWatcher;
import org.apache.druid.query.ResourceLimitExceededException;
import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.query.aggregation.MemoryVectorAggregators;
import org.apache.druid.query.context.ResponseContext;
import org.apache.druid.query.dimension.DimensionSpec;
import org.apache.druid.query.groupby.GroupByQuery;
import org.apache.druid.query.groupby.GroupByQueryConfig;
import org.apache.druid.query.groupby.PerSegmentEncodedResultRow;
import org.apache.druid.query.groupby.ResultRow;
import org.apache.druid.query.groupby.epinephelinae.GroupByMergingQueryRunnerV3.MergingDictionary;
import org.apache.druid.query.groupby.epinephelinae.Grouper.MemoryVectorEntry;
import org.apache.druid.query.groupby.epinephelinae.VectorGrouper.MemoryComparator;
import org.apache.druid.query.groupby.epinephelinae.vector.GroupByVectorColumnProcessorFactory;
import org.apache.druid.query.groupby.epinephelinae.vector.GroupByVectorColumnSelector;
import org.apache.druid.query.groupby.epinephelinae.vector.PartitionedHashTableIterator;
import org.apache.druid.query.groupby.epinephelinae.vector.TimeGranulizerIterator;
import org.apache.druid.query.groupby.orderby.DefaultLimitSpec;
import org.apache.druid.query.groupby.orderby.LimitSpec;
import org.apache.druid.query.ordering.StringComparator;
import org.apache.druid.query.ordering.StringComparators;
import org.apache.druid.segment.ColumnProcessors;
import org.apache.druid.segment.IdLookup;
import org.apache.druid.segment.column.ColumnCapabilities;
import org.apache.druid.segment.column.ColumnCapabilitiesImpl;
import org.apache.druid.segment.column.ValueType;
import org.apache.druid.segment.vector.MultiValueDimensionVectorSelector;
import org.apache.druid.segment.vector.ReadableVectorInspector;
import org.apache.druid.segment.vector.SingleValueDimensionVectorSelector;
import org.apache.druid.segment.vector.VectorColumnSelectorFactory;
import org.apache.druid.segment.vector.VectorObjectSelector;
import org.apache.druid.segment.vector.VectorValueSelector;
import org.joda.time.DateTime;

import javax.annotation.Nullable;
import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class GroupByShuffleMergingQueryRunner implements QueryRunner<ResultRow>
{
  private static final Logger log = new Logger(GroupByMergingQueryRunnerV2.class);

  private final GroupByQueryConfig config;
  private final Iterable<GroupByQuerySegmentProcessor<ResultRow>> queryables;
  private final ListeningExecutorService exec;
  private final QueryWatcher queryWatcher;
  private final int concurrencyHint;
  private final BlockingPool<ByteBuffer> mergeBufferPool;
  private final ObjectMapper spillMapper;
  private final String processingTmpDir;
  private final int mergeBufferSize;
  private final @Nullable DictionaryMergingQueryRunner dictionaryMergingRunner;

  public GroupByShuffleMergingQueryRunner(
      GroupByQueryConfig config,
      ExecutorService exec,
      QueryWatcher queryWatcher,
      Iterable<GroupByQuerySegmentProcessor<ResultRow>> queryables,
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
  }

  @Override
  public Sequence<ResultRow> run(QueryPlus<ResultRow> queryPlus, ResponseContext responseContext)
  {
    final GroupByQuery query = (GroupByQuery) queryPlus.getQuery();
    final GroupByQueryConfig querySpecificConfig = config.withOverrides(query);
    final int numHashBuckets = querySpecificConfig.getNumHashBuckets(concurrencyHint);

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

              // TODO: support parallel combine?
              final int minNumMergeBuffers = 2; // one for segment processing, one for merging

              final List<ReferenceCountingResourceHolder<ByteBuffer>> reserved = getMergeBuffersHolder(
                  minNumMergeBuffers,
                  hasTimeout,
                  timeoutAt
              );
              resources.registerAll(reserved);

              final QueryBlockingMemoryPool memoryPool = new QueryBlockingMemoryPool(
                  mergeBufferPool,
                  reserved.get(0)
              );
              final ReferenceCountingResourceHolder<ByteBuffer> mergeBufferHolder = reserved.get(1);
              final int sliceSize = mergeBufferHolder.get().capacity() / numHashBuckets; // TODO: what is mergeBufferSize for?

              final Supplier<MergedDictionary[]> mergedDictionariesSupplier;
              if (querySpecificConfig.isEarlyDictMerge() && dictionaryMergingRunner != null) { // TODO: no null check
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
                          Callables.withPriority(
                              () -> {
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
                              },
                              priority
                          )
                      )
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
                throw new UnsupportedOperationException("Cannot support variable-lengh keys. Enable earlyDictMerge.");
              }

              // prepare some stuffs
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
              List<AggregatorFactory> combiningFactories = query
                  .getAggregatorSpecs()
                  .stream()
                  .map(AggregatorFactory::getCombiningFactory)
                  .collect(Collectors.toList());

              // create iterators that process segments
              List<TimeGranulizerIteratorAndBufferSupplier> hashedSequencesList = FluentIterable
                  .from(queryables)
                  .transform(runner2 -> {
                    final SettableSupplier<ResourceHolder<ByteBuffer>> supplier = new SettableSupplier<>();
                    return new TimeGranulizerIteratorAndBufferSupplier(
                        runner2.process(queryPlusForRunners, supplier, responseContext),
                        supplier
                    );
                  })
                  .toList();

              final TimeOrderedIterators timeOrderedIterators = new TimeOrderedIterators(hashedSequencesList);
              final BucketProcessor[] bucketProcessors = new BucketProcessor[numHashBuckets];
              for (int i = 0; i < numHashBuckets; i++) {
                bucketProcessors[i] = new BucketProcessor(
                    querySpecificConfig,
                    query,
                    keySize,
                    keyOffsets,
                    mergedDictionariesSupplier,
                    dimensionCapabilities,
                    combiningFactories
                );
              }
              // intermediate result iterators
              final BucketedIterators bucketedIterators = new BucketedIterators(
                  mergeBufferHolder,
                  sliceSize,
                  numHashBuckets,
                  bucketProcessors
              );
              final int targetNumRowsToMergePerTask = query.getContextValue(
                  "targetNumRowsToMergePerTask",
                  10000
              );

              return new CloseableIterator<ResultRow>()
              {
                @Nullable CloseableIterator<ResultRow> delegate;
//                @Nullable DateTime currentTime = null;

                @Override
                public boolean hasNext()
                {
                  return (delegate != null && delegate.hasNext()) || timeOrderedIterators.hasNext();
                }

                @Override
                public ResultRow next()
                {
                  if (!hasNext()) {
                    throw new NoSuchElementException();
                  }
                  if (delegate == null || !delegate.hasNext()) {
                    delegate = nextDelegate();
                  }
                  return delegate.next();
                }

                private CloseableIterator<ResultRow> nextDelegate()
                {
                  List<TimeGranulizerIteratorAndBufferSupplier> iteratorsOfSameTimestamp
                      = timeOrderedIterators.next();
                  @Nullable DateTime currentTime = iteratorsOfSameTimestamp.get(0).peekTime();

                  // TODO: sync list
                  List<ListenableFuture<TimestampedBucketedSegmentIterators>> shuffleFutures = new ArrayList<>();
                  List<SettableFuture<Void>> mergeFutures = new ArrayList<>();
                  while (!iteratorsOfSameTimestamp.isEmpty()) {
                    ReferenceCountingResourceHolder<ByteBuffer> bufferHolder = memoryPool.take(); // TODO: timeout
                    TimeGranulizerIteratorAndBufferSupplier granulizerIterator
                        = iteratorsOfSameTimestamp.remove(0);
                    granulizerIterator.setBuffer(bufferHolder);
                    ListenableFuture<TimestampedBucketedSegmentIterators> future = exec.submit(
                        Callables.withPriority(
                            () -> {
                              //noinspection unused
                              try (Releaser releaser = bufferHolder.increment()) {
                                TimestampedBucketedSegmentIterators iterators = granulizerIterator.next();
                                bucketedIterators.add(iterators);

                                if (mergeFutures.size() < numHashBuckets) {
                                  SettableFuture<Void> mergeFuture = SettableFuture.create();
                                  exec.submit(
                                      Callables.withPriority(
                                          new MergeCallable(
                                              exec,
                                              shuffleFutures,
                                              bucketedIterators,
                                              targetNumRowsToMergePerTask,
                                              mergeFuture
                                          ),
                                          priority
                                      )
                                  );
                                  mergeFutures.add(mergeFuture);
                                }
                                return iterators;
                              }
                            },
                            priority
                        )
                    );
                    shuffleFutures.add(future);
                  }

                  // merge remaining
                  // while shuffleFutures are running
                  // wait for shuffleFutures to add iterators
                  // merge iterators
                  for (int i = mergeFutures.size(); i < numHashBuckets && !bucketedIterators.isEmpty(); i++) {
                    SettableFuture<Void> mergeFuture = SettableFuture.create();
                    exec.submit(
                        Callables.withPriority(
                            new MergeCallable(
                                exec,
                                shuffleFutures,
                                bucketedIterators,
                                targetNumRowsToMergePerTask,
                                mergeFuture
                            ),
                            priority
                        )
                    );
                  }

                  // todo: sort and merge can be done in parallel as long as they process different buckets
                  try {
                    Futures.allAsList(mergeFutures).get();
                  }
                  catch (InterruptedException | ExecutionException e) {
                    throw new RuntimeException(e);
                  }

                  // sort

                  List<ListenableFuture<CloseableIterator<ResultRow>>> sortFutures = new ArrayList<>();
                  for (int i = 0; i < numHashBuckets; i++) {
                    BucketProcessor bucketProcessor = bucketProcessors[i];
                    sortFutures.add(
                        exec.submit(
                            Callables.withPriority(
                                () -> CloseableIterators.wrap(
                                    bucketProcessor.grouper
                                        .iterator(bucketProcessor.memoryComparator)
                                        .map(entry -> {
                                          final PerSegmentEncodedResultRow resultRow = PerSegmentEncodedResultRow.create(
                                              query.getResultRowSizeWithoutPostAggregators()
                                          );
                                          if (query.getResultRowHasTimestamp()) {
                                            assert currentTime != null;
                                            resultRow.set(0, currentTime.getMillis());
                                          }

                                          // Add dimensions.
                                          int keyOffset = 0;
                                          for (int dim = 0; dim < bucketProcessor.columnSelectors.size(); dim++) {
                                            final GroupByVectorColumnSelector selector = bucketProcessor.columnSelectors.get(dim);

                                            selector.writeKeyToResultRow(
                                                entry.getKey(),
                                                keyOffset,
                                                resultRow,
                                                query.getResultRowDimensionStart() + dim,
                                                -1
                                            );

                                            keyOffset += selector.getGroupingKeySize();
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
                                                -1,
                                                entry.getValues()[j]
                                            );
                                          }
//                                     System.err.println(Thread.currentThread() + ", row conversion: " + Groupers.deserializeToRow(-1, entry.getKey(), entry.getValues()) + " -> " + resultRow);
                                          return resultRow;
                                        }),
                                    bucketProcessor.grouper
                                ),
                                priority
                            )
                        )
                    );
                  }

                  final List<CloseableIterator<ResultRow>> resultIterators = sortFutures.stream().map(f -> {
                    try {
                      return f.get(); // TODO: timeout
                    }
                    catch (InterruptedException | ExecutionException e) {
                      throw new RuntimeException(e); // TODO: exception handling
                    }
                  }).collect(Collectors.toList());

                  return CloseableIterators.mergeSorted(
                      resultIterators,
                      // TODO: i don't have to compare timestamp at all because all rows should have the same timestamp
                      // in the one delegate iterator. however, this doesn't seem very expensive. so maybe i can fix it later.
                      query.getRowOrdering(false)
                  );
                }

                @Override
                public void close() throws IOException
                {
                  Closer closer = Closer.create();
                  if (delegate != null) {
                    closer.register(delegate);
                  }
                  closer.register(resources);
                  closer.close();
                }
              };


//              return new CloseableIterator<ResultRow>()
//              {
//                final List<TimestampedIterator<MemoryVectorEntry>>[] partitionedIterators = new List[numHashBuckets];
//                @Nullable CloseableIterator<ResultRow> delegate;
//                @Nullable DateTime currentTime = null;
//
//                @Override
//                public boolean hasNext()
//                {
//                  boolean hasNextDelegateIterators = true;
//                  while ((delegate == null || !delegate.hasNext()) && (hasNextDelegateIterators = timeSortedHashedIterators.hasNext())) {
//                    if (delegate != null) {
//                      try {
//                        delegate.close();
//                      }
//                      catch (IOException e) {
//                        throw new RuntimeException(e);
//                      }
//                    }
//                    for (int i = 0; i < numHashBuckets; i++) {
//                      partitionedIterators[i] = new ArrayList<>();
//                    }
//                    List<TimestampedIterators> iteratorsOfSameTimestamp = timeSortedHashedIterators.next();
//                    currentTime = iteratorsOfSameTimestamp.get(0).getTimestamp();
//                    for (TimestampedIterators eachIterators : iteratorsOfSameTimestamp) {
//                      for (int i = 0; i < eachIterators.iterators.length; i++) {
//                        partitionedIterators[i].add(eachIterators.iterators[i]);
//                      }
//                    }
//
//                    delegate = nextDelegate(
//                        processingCallableScheduler,
//                        currentTime,
//                        mergedDictionariesSupplier,
//                        partitionedIterators,
//                        keySize,
//                        keyOffsets,
//                        mergeBufferHolder,
//                        sliceSize,
//                        dimensionCapabilities
//                    );
//                  }
//                  if (!hasNextDelegateIterators) {
////                     no more computation
//                    processingCallableScheduler.shutdown();
//                  }
//                  return delegate != null && delegate.hasNext();
//                }
//
//                @Override
//                public ResultRow next()
//                {
//                  if (delegate == null || !delegate.hasNext()) {
//                    throw new NoSuchElementException();
//                  }
//                  return delegate.next();
//                }
//
//                @Override
//                public void close() throws IOException
//                {
//                  Closer closer = Closer.create();
//                  closer.register(() -> processingCallableScheduler.shutdown());
//                  if (delegate != null) {
//                    closer.register(delegate);
//                  }
//                  closer.register(timeSortedHashedIterators);
//                  closer.register(resources);
//                  closer.close();
//                }
//              };
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

//          private CloseableIterator<ResultRow> nextDelegate(
//              ProcessingCallableScheduler processingCallableScheduler,
//              @Nullable DateTime currentTime,
//              @Nullable Supplier<MergedDictionary[]> mergedDictionariesSupplier,
//              List<TimestampedIterator<MemoryVectorEntry>>[] partitionedIterators,
//              int keySize,
//              int[] keyOffsets,
//              ReferenceCountingResourceHolder<ByteBuffer> mergeBufferHolder,
//              int sliceSize,
//              List<ColumnCapabilities> dimensionCapabilities
//          )
//          {
//            List<AggregatorFactory> combiningFactories = query
//                .getAggregatorSpecs()
//                .stream()
//                .map(AggregatorFactory::getCombiningFactory)
//                .collect(Collectors.toList());
//
//            final int numDims = query.getResultRowAggregatorStart() - query.getResultRowDimensionStart();
//            final List<ListenableFuture<CloseableIterator<ResultRow>>> futures = new ArrayList<>();
//            for (int i = 0; i < numHashBuckets; i++) {
//              final CloseableIterator<MemoryVectorEntry> concatIterator = CloseableIterators.concat(
//                  partitionedIterators[i]
//              ).map(entry -> {
//                final WritableMemory keyMemory = entry.getKeys();
//
////                StringBuilder keys = new StringBuilder(Thread.currentThread() + ", dict conversion: ");
////                for (int k = 0; k < entry.getCurrentVectorSize(); k++) {
////                  keys.append(keyMemory.getInt(k * keySize)).append(", ");
////                  keys.append(keyMemory.getInt(k * keySize + 4)).append(", ");
////                }
//
//                // Convert dictionary
//                for (int rowIdx = 0; rowIdx < entry.getCurrentVectorSize(); rowIdx++) {
//                  for (int dimIdx = 0; dimIdx < numDims; dimIdx++) {
//                    // TODO: maybe GroupByVectorColumnConvertor or something..
//                    if (query.getDimensions().get(dimIdx).getOutputType() == ValueType.STRING) {
//                      if (querySpecificConfig.isEarlyDictMerge() && mergedDictionariesSupplier != null) {
//                        final long memoryOffset = rowIdx * keySize + keyOffsets[dimIdx];
//                        final int oldDictId = keyMemory.getInt(memoryOffset);
//                        assert entry.segmentId > -1;
//                        final int newDictId = mergedDictionariesSupplier.get()[dimIdx].getNewDictId(
//                            entry.segmentId,
//                            oldDictId
//                        );
//                        keyMemory.putInt(memoryOffset, newDictId);
//                      } else {
//                        throw new UnsupportedOperationException();
//                      }
//                    }
//                  }
//                }
//
////                keys.append(" -> ");
////                for (int k = 0; k < entry.getCurrentVectorSize(); k++) {
////                  keys.append(keyMemory.getInt(k * keySize)).append(", ");
////                  keys.append(keyMemory.getInt(k * keySize + 4)).append(", ");
////                }
////
////                System.err.println(keys);
//
//                return entry;
//              });
//
//              final SettableSupplier<MemoryVectorEntry> currentBufferSupplier = new SettableSupplier<>();
//              final VectorColumnSelectorFactory columnSelectorFactory = new MemoryVectorEntryColumnSelectorFactory(
//                  query,
//                  currentBufferSupplier,
//                  keySize,
//                  keyOffsets,
//                  mergedDictionariesSupplier,
//                  combiningFactories
//              );
//              final List<GroupByVectorColumnSelector> dimensions = query.getDimensions().stream().map(
//                  dimensionSpec ->
//                      DimensionHandlerUtils.makeVectorProcessor(
//                          dimensionSpec,
//                          GroupByVectorColumnProcessorFactory.instance(),
//                          columnSelectorFactory,
//                          false
//                      )
//              ).collect(Collectors.toList());
//              final MemoryComparator memoryComparator = GrouperBufferComparatorUtils.memoryComparator(
//                  false,
//                  query.getContextSortByDimsFirst(),
//                  query.getDimensions().size(),
//                  getDimensionComparators(dimensions, query.getLimitSpec())
//              );
//
//              // TODO: maybe can reuse
//              final FixedSizeHashVectorGrouper grouper = new FixedSizeHashVectorGrouper(
//                  Suppliers.ofInstance(Groupers.getSlice(mergeBufferHolder.get(), sliceSize, i)),
//                  1,
//                  keySize,
//                  MemoryVectorAggregators.factorizeVector(
//                      columnSelectorFactory,
//                      combiningFactories
//                  ),
//                  querySpecificConfig.getBufferGrouperMaxSize(),
//                  querySpecificConfig.getBufferGrouperMaxLoadFactor(),
//                  querySpecificConfig.getBufferGrouperInitialBuckets()
//              );
//              grouper.initVectorized(QueryContexts.getVectorSize(query)); // TODO: can i get the max vector size in a different way? this seems no good
//              final SettableFuture<CloseableIterator<ResultRow>> resultFuture = SettableFuture.create();
//              processingCallableScheduler.schedule(
//                  new ProcessingTask<CloseableIterator<ResultRow>>()
//                  {
//                    @Override
//                    public CloseableIterator<ResultRow> run() throws Exception
//                    {
//                      //noinspection unused
//                      try (Releaser releaser = mergeBufferHolder.increment();
//                           CloseableIterator<MemoryVectorEntry> iteratorCloser = concatIterator) {
//                        while (concatIterator.hasNext()) {
//                          final MemoryVectorEntry entry = concatIterator.next();
//                          currentBufferSupplier.set(entry);
//                          grouper.aggregateVector(entry.getKeys(), 0, entry.curVectorSize);
//                        }
//                        return CloseableIterators.wrap(
//                            grouper.iterator(memoryComparator)
//                                   .map(entry -> {
//                                     final PerSegmentEncodedResultRow resultRow = PerSegmentEncodedResultRow.create(
//                                         query.getResultRowSizeWithoutPostAggregators()
//                                     );
//                                     if (query.getResultRowHasTimestamp()) {
//                                       assert currentTime != null;
//                                       resultRow.set(0, currentTime.getMillis());
//                                     }
//
//                                     // Add dimensions.
//                                     int keyOffset = 0;
//                                     for (int i = 0; i < dimensions.size(); i++) {
//                                       final GroupByVectorColumnSelector selector = dimensions.get(i);
//
//                                       selector.writeKeyToResultRow(
//                                           entry.getKey(),
//                                           keyOffset,
//                                           resultRow,
//                                           query.getResultRowDimensionStart() + i,
//                                           -1
//                                       );
//
//                                       keyOffset += selector.getGroupingKeySize();
//                                     }
//
//                                     GroupByQueryEngineV2.convertRowTypesToOutputTypes(
//                                         query.getDimensions(),
//                                         resultRow,
//                                         query.getResultRowDimensionStart(),
//                                         0,
//                                         dimensionCapabilities,
//                                         false
//                                     );
//
//                                     for (int j = 0; j < entry.getValues().length; j++) {
//                                       resultRow.set(
//                                           query.getResultRowAggregatorStart() + j,
//                                           -1,
//                                           entry.getValues()[j]
//                                       );
//                                     }
////                                     System.err.println(Thread.currentThread() + ", row conversion: " + Groupers.deserializeToRow(-1, entry.getKey(), entry.getValues()) + " -> " + resultRow);
//                                     return resultRow;
//                                   }),
//                            grouper
//                        );
//                      }
//                    }
//
//                    @Override
//                    public SettableFuture<CloseableIterator<ResultRow>> getResultFuture()
//                    {
//                      return resultFuture;
//                    }
//
//                    @Override
//                    public boolean isSentinel()
//                    {
//                      return false;
//                    }
//                  }
//              );
//
//              futures.add(resultFuture);
//            }
//
//            // TODO: handle timeout
//            try {
//              Futures.allAsList(futures).get();
//            }
//            catch (InterruptedException | ExecutionException e) {
//              throw new RuntimeException(e); // TODO: exception handling
//            }
//            final List<CloseableIterator<ResultRow>> resultIterators = futures.stream().map(f -> {
//              try {
//                return f.get();
//              }
//              catch (InterruptedException | ExecutionException e) {
//                throw new RuntimeException(e); // TODO: exception handling
//              }
//            }).collect(Collectors.toList());
//
//            return CloseableIterators.mergeSorted(
//                resultIterators,
//                // TODO: i don't have to compare timestamp at all because all rows should have the same timestamp
//                // in the one delegate iterator. however, this doesn't seem very expensive. so maybe i can fix it later.
//                query.getRowOrdering(false)
//            );
//          }

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

          private MemoryComparator[] getDimensionComparators(List<GroupByVectorColumnSelector> selectors, LimitSpec limitSpec)
          {
            MemoryComparator[] dimComparators = new MemoryComparator[selectors.size()];

            int keyOffset = 0;
            for (int i = 0; i < selectors.size(); i++) {
              final String dimName = query.getDimensions().get(i).getOutputName();
              final StringComparator stringComparator;
              if (limitSpec instanceof DefaultLimitSpec) {
                stringComparator = DefaultLimitSpec.getComparatorForDimName(
                    (DefaultLimitSpec) limitSpec,
                    dimName
                );
              } else {
                stringComparator = StringComparators.LEXICOGRAPHIC;
              }
              dimComparators[i] = selectors.get(i).memoryComparator(keyOffset, stringComparator);
              keyOffset += selectors.get(i).getGroupingKeySize();
            }

            return dimComparators;
          }
        }
    );
  }

  private CloseableIterator<MemoryVectorEntry> decorateDictConversion(
      GroupByQuery query,
      GroupByQueryConfig querySpecificConfig,
      @Nullable CloseableIterator<MemoryVectorEntry> iterator,
      int numDims,
      Supplier<MergedDictionary[]> mergedDictionariesSupplier,
      int keySize,
      int[] keyOffsets
  )
  {
    return iterator
        .map(entry -> {
          final WritableMemory keyMemory = entry.getKeys();

//                StringBuilder keys = new StringBuilder(Thread.currentThread() + ", dict conversion: ");
//                for (int k = 0; k < entry.getCurrentVectorSize(); k++) {
//                  keys.append(keyMemory.getInt(k * keySize)).append(", ");
//                  keys.append(keyMemory.getInt(k * keySize + 4)).append(", ");
//                }

          // Convert dictionary
          for (int rowIdx = 0; rowIdx < entry.getCurrentVectorSize(); rowIdx++) {
            for (int dimIdx = 0; dimIdx < numDims; dimIdx++) {
              // TODO: maybe GroupByVectorColumnConvertor or something..
              if (query.getDimensions().get(dimIdx).getOutputType() == ValueType.STRING) {
                if (querySpecificConfig.isEarlyDictMerge() && mergedDictionariesSupplier != null) {
                  final long memoryOffset = rowIdx * keySize + keyOffsets[dimIdx];
                  final int oldDictId = keyMemory.getInt(memoryOffset);
                  assert entry.segmentId > -1;
                  final int newDictId = mergedDictionariesSupplier.get()[dimIdx].getNewDictId(
                      entry.segmentId,
                      oldDictId
                  );
                  keyMemory.putInt(memoryOffset, newDictId);
                } else {
                  throw new UnsupportedOperationException();
                }
              }
            }
          }

//                keys.append(" -> ");
//                for (int k = 0; k < entry.getCurrentVectorSize(); k++) {
//                  keys.append(keyMemory.getInt(k * keySize)).append(", ");
//                  keys.append(keyMemory.getInt(k * keySize + 4)).append(", ");
//                }
//
//                System.err.println(keys);

          return entry;
        });
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

  private static class MemoryVectorEntryColumnSelectorFactory implements VectorColumnSelectorFactory, ReadableVectorInspector
  {
    private final GroupByQuery query;
    private final Supplier<MemoryVectorEntry> currentEntrySupplier;
    private final int keySize;
    private final int[] keyOffsets;
    private final Supplier<MergedDictionary[]> mergedDictionariesSupplier;
    private final List<AggregatorFactory> combiningFactories;

    private MemoryVectorEntryColumnSelectorFactory(
        GroupByQuery query,
        Supplier<MemoryVectorEntry> currentVectorSupplier,
        int keySize,
        int[] keyOffsets,
        Supplier<MergedDictionary[]> mergedDictionariesSupplier,
        List<AggregatorFactory> combiningFactories
    )
    {
      this.query = query;
      this.currentEntrySupplier = currentVectorSupplier;
      this.keySize = keySize;
      this.keyOffsets = keyOffsets;
      this.mergedDictionariesSupplier = mergedDictionariesSupplier;
      this.combiningFactories = combiningFactories;
    }

    @Override
    public int getId()
    {
      return currentEntrySupplier.get().getVectorId();
    }

    @Override
    public ReadableVectorInspector getReadableVectorInspector()
    {
      return this;
    }

    @Override
    public int getMaxVectorSize()
    {
      return currentEntrySupplier.get().maxVectorSize;
    }

    @Override
    public int getCurrentVectorSize()
    {
      return currentEntrySupplier.get().curVectorSize;
    }

    @Override
    public SingleValueDimensionVectorSelector makeSingleValueDimensionSelector(DimensionSpec dimensionSpec)
    {
      final int dimIndex = dimIndex(dimensionSpec.getDimension());
      if (dimIndex < 0) {
        throw new ISE("Cannot find dimension[%s]", dimensionSpec.getDimension());
      }

      return new SingleValueDimensionVectorSelector()
      {
        int[] rowVector = null;

        @Override
        public int[] getRowVector()
        {
          if (rowVector == null) {
            rowVector = new int[getMaxVectorSize()];
          }

          for (int i = 0; i < getCurrentVectorSize(); i++) {
            rowVector[i] = currentEntrySupplier.get().getKeys().getInt(i * keySize + keyOffsets[dimIndex]);
          }

          return rowVector;
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

        @Override
        public int getMaxVectorSize()
        {
          return currentEntrySupplier.get().maxVectorSize;
        }

        @Override
        public int getCurrentVectorSize()
        {
          return currentEntrySupplier.get().curVectorSize;
        }
      };
    }

    @Override
    public MultiValueDimensionVectorSelector makeMultiValueDimensionSelector(DimensionSpec dimensionSpec)
    {
      throw new UnsupportedOperationException();
    }

    @Override
    public VectorValueSelector makeValueSelector(String column)
    {
      final int dimIndex = dimIndex(column);
      if (dimIndex > -1) {
        final ValueType valueType = dimType(dimIndex);

        return new VectorValueSelector()
        {
          long[] longs = null;
          float[] floats = null;
          double[] doubles = null;

          @Override
          public long[] getLongVector()
          {
            if (longs == null) {
              longs = new long[getMaxVectorSize()];
            }

            switch (valueType) {
              case DOUBLE:
                for (int i = 0; i < getCurrentVectorSize(); i++) {
                  longs[i] = (long) currentEntrySupplier.get().getKeys().getDouble(i * keySize + keyOffsets[dimIndex]);
                }
                break;
              case FLOAT:
                for (int i = 0; i < getCurrentVectorSize(); i++) {
                  longs[i] = (long) currentEntrySupplier.get().getKeys().getFloat(i * keySize + keyOffsets[dimIndex]);
                }
                break;
              case LONG:
                for (int i = 0; i < getCurrentVectorSize(); i++) {
                  longs[i] = currentEntrySupplier.get().getKeys().getLong(i * keySize + keyOffsets[dimIndex]);
                }
                break;
              case STRING:
                for (int i = 0; i < getCurrentVectorSize(); i++) {
                  longs[i] = currentEntrySupplier.get().getKeys().getInt(i * keySize + keyOffsets[dimIndex]);
                }
                break;
              default:
                throw new UnsupportedOperationException(valueType.name());
            }

            return longs;
          }

          @Override
          public float[] getFloatVector()
          {
            if (floats == null) {
              floats = new float[getMaxVectorSize()];
            }

            switch (valueType) {
              case DOUBLE:
                for (int i = 0; i < getCurrentVectorSize(); i++) {
                  floats[i] = (float) currentEntrySupplier.get().getKeys().getDouble(i * keySize + keyOffsets[dimIndex]);
                }
                break;
              case FLOAT:
                for (int i = 0; i < getCurrentVectorSize(); i++) {
                  floats[i] = currentEntrySupplier.get().getKeys().getFloat(i * keySize + keyOffsets[dimIndex]);
                }
                break;
              case LONG:
                for (int i = 0; i < getCurrentVectorSize(); i++) {
                  floats[i] = currentEntrySupplier.get().getKeys().getLong(i * keySize + keyOffsets[dimIndex]);
                }
                break;
              case STRING:
                for (int i = 0; i < getCurrentVectorSize(); i++) {
                  floats[i] = currentEntrySupplier.get().getKeys().getInt(i * keySize + keyOffsets[dimIndex]);
                }
                break;
              default:
                throw new UnsupportedOperationException(valueType.name());
            }

            return floats;
          }

          @Override
          public double[] getDoubleVector()
          {
            if (doubles == null) {
              doubles = new double[getMaxVectorSize()];
            }

            switch (valueType) {
              case DOUBLE:
                for (int i = 0; i < getCurrentVectorSize(); i++) {
                  doubles[i] = currentEntrySupplier.get().getKeys().getDouble(i * keySize + keyOffsets[dimIndex]);
                }
                break;
              case FLOAT:
                for (int i = 0; i < getCurrentVectorSize(); i++) {
                  doubles[i] = currentEntrySupplier.get().getKeys().getFloat(i * keySize + keyOffsets[dimIndex]);
                }
                break;
              case LONG:
                for (int i = 0; i < getCurrentVectorSize(); i++) {
                  doubles[i] = currentEntrySupplier.get().getKeys().getLong(i * keySize + keyOffsets[dimIndex]);
                }
                break;
              case STRING:
                for (int i = 0; i < getCurrentVectorSize(); i++) {
                  doubles[i] = currentEntrySupplier.get().getKeys().getInt(i * keySize + keyOffsets[dimIndex]);
                }
                break;
              default:
                throw new UnsupportedOperationException(valueType.name());
            }

            return doubles;
          }

          @Nullable
          @Override
          public boolean[] getNullVector()
          {
            // TODO
            return null;
          }

          @Override
          public int getMaxVectorSize()
          {
            return currentEntrySupplier.get().maxVectorSize;
          }

          @Override
          public int getCurrentVectorSize()
          {
            return currentEntrySupplier.get().curVectorSize;
          }
        };
      } else {
        final int aggIndex = aggIndex(column);
        if (aggIndex < 0) {
          throw new ISE("Cannot find column[%s]", column);
        }
        final ValueType valueType = aggType(aggIndex);
        return new VectorValueSelector()
        {
          @Override
          public int getMaxVectorSize()
          {
            return currentEntrySupplier.get().maxVectorSize;
          }

          @Override
          public int getCurrentVectorSize()
          {
            return currentEntrySupplier.get().curVectorSize;
          }

          @Override
          public long[] getLongVector()
          {
            switch (valueType) {
              case DOUBLE:
              case FLOAT:
              case LONG:
                return (long[]) currentEntrySupplier.get().getValues()[aggIndex];
              default:
                throw new UnsupportedOperationException(valueType.name());
            }
          }

          @Override
          public float[] getFloatVector()
          {
            switch (valueType) {
              case DOUBLE:
              case FLOAT:
              case LONG:
                return (float[]) currentEntrySupplier.get().getValues()[aggIndex];
              default:
                throw new UnsupportedOperationException(valueType.name());
            }
          }

          @Override
          public double[] getDoubleVector()
          {
            switch (valueType) {
              case DOUBLE:
              case FLOAT:
              case LONG:
                return (double[]) currentEntrySupplier.get().getValues()[aggIndex];
              default:
                throw new UnsupportedOperationException(valueType.name());
            }
          }

          @Nullable
          @Override
          public boolean[] getNullVector()
          {
            // TODO
            return null;
          }
        };
      }
    }

    @Override
    public VectorObjectSelector makeObjectSelector(String column)
    {
      throw new UnsupportedOperationException();
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
  }

  private static class SegmentProcessingTaskScheduler
  {
    private final List<SegmentProcessing> segmentProcessings;
    private final ProcessingCallableScheduler processingCallableScheduler;

    private SegmentProcessingTaskScheduler(
        List<TimeGranulizerIterator<TimestampedBucketedSegmentIterators>> baseIterators,
        ProcessingCallableScheduler processingCallableScheduler
    )
    {
      this.segmentProcessings = baseIterators.stream().map(SegmentProcessing::new).collect(Collectors.toList());
      this.processingCallableScheduler = processingCallableScheduler;
    }

    @Nullable
    private DateTime nextTimestamp()
    {
      return segmentProcessings
          .stream()
          .filter(p -> p.baseIterator.hasNextTime())
          .map(p -> p.baseIterator.peekTime())
          .min(Comparator.nullsLast(Comparator.naturalOrder()))
          .orElse(null);
    }

    private List<TimeGranulizerIterator<TimestampedBucketedSegmentIterators>> findSegmentProcessings(@Nullable DateTime currentTime)
    {
      return segmentProcessings
          .stream()
          .filter(p -> p.baseIterator.hasNextTime())
          .filter(p -> Objects.equals(p.baseIterator.peekTime(), currentTime))
          .map(p -> p.baseIterator)
          .collect(Collectors.toList());
    }

//    public List<TimestampedIterators> getReadyIteratorsOfTime(@Nullable DateTime time)
//    {
//      for (SegmentProcessing processing : activeProcessings) {
//        if (processing.future != null && processing.future.isDone()) {
//          try {
//            processing.peeked = processing.future.get();
//            processing.future = null;
//          }
//          catch (InterruptedException | ExecutionException e) {
//            throw new RuntimeException(e); // TODO: handle properly
//          }
//        }
//      }
//
//      return activeProcessings
//          .stream()
//          .filter(processing -> Objects.equals(processing.baseIterator.peekTime(), time))
//          .map(processing -> processing.peeked)
//          .filter(Objects::nonNull)
//          .collect(Collectors.toList());
//    }

    public boolean isFinished()
    {
      return segmentProcessings.stream().allMatch(SegmentProcessing::isDone);
    }
  }

  private static class SegmentProcessing
  {
    private final TimeGranulizerIterator<TimestampedBucketedSegmentIterators> baseIterator;

    @Nullable
    private TimestampedBucketedSegmentIterators peeked;
    @Nullable
    private ListenableFuture<TimestampedBucketedSegmentIterators> future;

    private SegmentProcessing(TimeGranulizerIterator<TimestampedBucketedSegmentIterators> baseIterator)
    {
      this.baseIterator = baseIterator;
    }

    private boolean isDone()
    {
      return !baseIterator.hasNext();
    }
  }

  private static class SegmentProcessTask implements ProcessingTask<TimestampedBucketedSegmentIterators>
  {
    private final ProcessingCallablePool pool;
    private final BlockingQueue<TimeGranulizerIterator<TimestampedBucketedSegmentIterators>> baseIterators;
    private final SettableFuture<TimestampedBucketedSegmentIterators> resultFuture;
    private final int concurrencyHint;

    private SegmentProcessTask(
        ProcessingCallablePool pool,
        BlockingQueue<TimeGranulizerIterator<TimestampedBucketedSegmentIterators>> baseIterators,
        SettableFuture<TimestampedBucketedSegmentIterators> resultFuture,
        int concurrencyHint
    )
    {
      this.pool = pool;
      this.baseIterators = baseIterators;
      this.resultFuture = resultFuture;
      this.concurrencyHint = concurrencyHint;
    }

    @Override
    public TimestampedBucketedSegmentIterators run() throws Exception
    {
      pool.startCallablesUpToMax(Math.min(concurrencyHint, baseIterators.size()));
      final TimeGranulizerIterator<TimestampedBucketedSegmentIterators> baseIterator = baseIterators.poll();
      if (baseIterator == null) {
        // queue is empty, which means we have no more work
        // TODO: schedule sentinal task to the callable where it is running this task. or we can schedule it when this returns null
        // TODO: maybe should not happen
      } else {
        assert baseIterator.hasNext();
        final TimestampedBucketedSegmentIterators result = baseIterator.next();
        return result;
      }
      throw new ISE("WTH?");
    }

    @Override
    public SettableFuture<TimestampedBucketedSegmentIterators> getResultFuture()
    {
      return resultFuture;
    }

    @Override
    public boolean isSentinel()
    {
      return false;
    }
  }

  private static final class TimeGranulizerIteratorAndBufferSupplier implements TimeGranulizerIterator<TimestampedBucketedSegmentIterators>
  {
    private final TimeGranulizerIterator<TimestampedBucketedSegmentIterators> iterator;
    private final SettableSupplier<ResourceHolder<ByteBuffer>> bufferSupplier;

    private TimeGranulizerIteratorAndBufferSupplier(
        TimeGranulizerIterator<TimestampedBucketedSegmentIterators> iterator,
        SettableSupplier<ResourceHolder<ByteBuffer>> bufferSupplier
    )
    {
      this.iterator = iterator;
      this.bufferSupplier = bufferSupplier;
    }

    private void setBuffer(ResourceHolder<ByteBuffer> resourceHolder)
    {
      bufferSupplier.set(resourceHolder);
    }

    @Override
    public boolean hasNextTime()
    {
      return iterator.hasNextTime();
    }

    @Nullable
    @Override
    public DateTime peekTime()
    {
      return iterator.peekTime();
    }

    @Override
    public void close() throws IOException
    {
      iterator.close();
    }

    @Override
    public boolean hasNext()
    {
      return iterator.hasNext();
    }

    @Override
    public TimestampedBucketedSegmentIterators next()
    {
      return iterator.next();
    }
  }

  private static class TimeOrderedIterators implements CloseableIterator<List<TimeGranulizerIteratorAndBufferSupplier>>
  {
    private final List<TimeGranulizerIteratorAndBufferSupplier> baseIterators;

    // TODO: should be created per timestamp
    private TimeOrderedIterators(List<TimeGranulizerIteratorAndBufferSupplier> baseIterators)
    {
      this.baseIterators = baseIterators;
    }

    @Override
    public boolean hasNext()
    {
      return baseIterators.stream().anyMatch(Iterator::hasNext);
    }

    @Override
    public List<TimeGranulizerIteratorAndBufferSupplier> next()
    {
      if (!hasNext()) {
        throw new NoSuchElementException();
      }

      @Nullable
      DateTime minTime = baseIterators.stream()
                                      .filter(Iterator::hasNext)
                                      .map(TimeGranulizerIterator::peekTime)
                                      .filter(Objects::nonNull)
                                      .min(Comparator.naturalOrder())
                                      .orElse(null);

      return baseIterators.stream()
                          .filter(Iterator::hasNext)
                          .filter(it -> Objects.equals(it.peekTime(), minTime))
                          .collect(Collectors.toList());
    }

    @Override
    public void close() throws IOException
    {
      Closer closer = Closer.create();
      closer.registerAll(baseIterators);
      closer.close();
    }
  }

  public static class ProcessingCallablePool
  {
    private final ExecutorService exec;
    private final int priority;
    private final int maxNumCallables; // concurrencyHint
    private final List<ProcessingCallable> callables;
    private final List<Future<Void>> callableFutures;
    private final BlockingQueue<ProcessingTask> workQueue;
    private final AtomicInteger numActiveCallables = new AtomicInteger();

    private boolean closed;

    public ProcessingCallablePool(
        ExecutorService exec,
        int priority,
        int concurrencyHint
    )
    {
      this.exec = exec;
      this.priority = priority;
      this.maxNumCallables = concurrencyHint;
      this.callables = new ArrayList<>(maxNumCallables);
      this.callableFutures = new ArrayList<>(maxNumCallables);
      this.workQueue = new LinkedBlockingDeque<>();
    }

    public void startCallablesUpToMax(int max)
    {
      // TODO: validate concurrency model
      synchronized (this) {
        IntStream.range(callables.size(), max).forEach(i -> startNewCallable());
      }
    }

    private void startNewCallable()
    {
      assert callables.size() < maxNumCallables;
      final ProcessingCallable callable = new ProcessingCallable(
          priority,
          workQueue,
          numActiveCallables::incrementAndGet,
          numActiveCallables::decrementAndGet
      );
      callableFutures.add(exec.submit(callable));
      callables.add(callable);
    }

    private void stopCallable(int i)
    {
      assert callables.size() > 0;
      // TODO: maybe interrupt is better?? or not because we will not want to interrupt things.. maybe?
      // do you want to interrupt per-segment processing?
      // do you want to interrupt merge task? no, it is split anyway
      callableFutures.remove(i);
      callables.remove(i).taskQueue.offer(ShutdownTask.SHUTDOWN_TASK);
    }

    public void schedule(ProcessingTask<?> task)
    {
      workQueue.offer(task);
    }

    public void shutdown()
    {
      if (!closed) {
        closed = true;
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
  }

  public static class ProcessingCallableScheduler
  {
    private final ProcessingCallablePool callablePool;

    public ProcessingCallableScheduler(
        ExecutorService exec,
        int priority,
        int concurrencyHint
    )
    {
      this.callablePool = new ProcessingCallablePool(exec, priority, concurrencyHint);

      // start callables aggressively so that the tasks to compute per-segment aggregates can be executed
      // at the same time as much as possible.
      callablePool.startCallablesUpToMax(concurrencyHint);
    }

    public ListenableFuture<TimestampedBucketedSegmentIterators> scheduleSegmentProcessingTask(Supplier<TimestampedBucketedSegmentIterators> iteratorsSupplier)
    {
      // take one idle callable
//      callablePool.startCallablesUpToMax(); // TODO: is this correct?
      // run task
      final SettableFuture<TimestampedBucketedSegmentIterators> resultFuture = SettableFuture.create();
      callablePool.schedule(
          new ProcessingTask<TimestampedBucketedSegmentIterators>()
          {
            @Override
            public TimestampedBucketedSegmentIterators run()
            {
              return iteratorsSupplier.get();
            }

            @Override
            public SettableFuture<TimestampedBucketedSegmentIterators> getResultFuture()
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
      // return callable
      return resultFuture;
    }

    public void shutdown()
    {

    }
  }

//  public static class ProcessingCallableScheduler
//  {
//    private final ExecutorService exec;
//    private final int numCallables;
//    private final List<ProcessingCallable> callables;
//    private final List<Future<Void>> callableFutures;
//    private final BlockingQueue<ProcessingTask> workQueue;
//
//    private boolean closed;
//
//    public ProcessingCallableScheduler(
//        ExecutorService exec,
//        int priority,
//        int concurrencyHint,
//        int numHashBuckets,
//        int numQueryables
//    )
//    {
//      this.exec = exec;
//      this.numCallables = Math.min(concurrencyHint, Math.max(numHashBuckets, numQueryables));
//      this.callables = new ArrayList<>(numCallables);
//      this.callableFutures = new ArrayList<>(numCallables);
//      this.workQueue = new LinkedBlockingDeque<>();
//      IntStream.range(0, numCallables).forEach(i -> {
//        final ProcessingCallable callable = new ProcessingCallable(priority, workQueue);
//        callableFutures.add(exec.submit(callable));
//        callables.add(callable);
//      });
//    }
//
//    public void schedule(ProcessingTask<?> task)
//    {
//      workQueue.offer(task);
//    }
//
//    public void shutdown()
//    {
//      if (!closed) {
//        closed = true;
//        // TODO: fix how to shut down
//        callables.forEach(c -> {
//          c.taskQueue.offer(ShutdownTask.SHUTDOWN_TASK);
//        });
//        for (Future<Void> future : callableFutures) {
//          try {
//            future.get(); // TODO: handle timeout
//          }
//          catch (InterruptedException | ExecutionException e) {
//            throw new RuntimeException(e); // TODO: handle exceptions
//          }
//        }
//      }
//    }
//  }

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

    // TODO: this is ugly
    @Override
    public boolean isSentinel()
    {
      return true;
    }
  }

  public static class ProcessingCallable extends AbstractPrioritizedCallable<Void>
  {
    private final BlockingQueue<ProcessingTask> taskQueue;
    private final Runnable startupHook;
    private final Runnable shutdownHook;

    public ProcessingCallable(
        int priority,
        BlockingQueue<ProcessingTask> taskQueue,
        Runnable startupHook,
        Runnable shutdownHook
    )
    {
      super(priority);
      this.taskQueue = taskQueue;
      this.startupHook = startupHook;
      this.shutdownHook = shutdownHook;
    }

    @Override
    public Void call() throws Exception
    {
      startupHook.run();
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
      shutdownHook.run();
      return null;
    }
  }

  /**
   * TODO: more javadoc
   *
   * Granularized and partitioned hash table iterators for the same segment
   */
  public static class TimestampedBucketedSegmentIterators
  {
    private final PartitionedHashTableIterator[] iterators;
    @Nullable
    private final DateTime timestamp;

    public TimestampedBucketedSegmentIterators(PartitionedHashTableIterator[] iterators, @Nullable DateTime timestamp)
    {
      this.iterators = iterators;
      this.timestamp = timestamp;
    }
  }

  /**
   * Set of iterators for the same bucket
   */
  public static class BucketIterators
  {
    private final PriorityBlockingQueue<PartitionedHashTableIterator> iterators = new PriorityBlockingQueue<>(
        11, // default initial capacity
        Comparator.comparingInt(PartitionedHashTableIterator::size).reversed()
    );
    private final int bucket;

    public BucketIterators(int bucket)
    {
      this.bucket = bucket;
    }

    public void offer(PartitionedHashTableIterator iterator)
    {
      Preconditions.checkArgument(
          bucket == iterator.getBucket(),
          "Expected bucket[%s], but got[%s]",
          bucket,
          iterator.getBucket()
      );
      iterators.add(iterator);
    }

    public List<PartitionedHashTableIterator> poll(int targetSize)
    {
      final List<PartitionedHashTableIterator> toReturn = new ArrayList<>();
      for (int size = 0; size < targetSize; ) {
        PartitionedHashTableIterator next = iterators.poll();
        if (next == null) {
          break;
        }
        toReturn.add(next);
        size += next.size();
      }
      return toReturn;
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

  private static class BucketedIterators
  {
    // input iterators
    private final List<PartitionedHashTableIterator>[] iteratorLists;
    // output processors
    private final BucketProcessor[] bucketProcessors;

    /**
     * output buffr slices for buckets
     */
    private final ByteBuffer[] slices;
    /**
     * true when each slice is writable.
     */
    private final AtomicBoolean[] sliceLocks;

    private BucketedIterators(
        ReferenceCountingResourceHolder<ByteBuffer> mergeBufferHolder,
        int sliceSize,
        int numBuckets,
        BucketProcessor[] bucketProcessors
    )
    {
      this.iteratorLists = new List[numBuckets];
      for (int i = 0; i < numBuckets; i++) {
        this.iteratorLists[i] = new ArrayList<>();
      }
      this.bucketProcessors = bucketProcessors;

      mergeBufferHolder.increment(); // TODO: release
      slices = new ByteBuffer[iteratorLists.length];
      sliceLocks = new AtomicBoolean[iteratorLists.length];
      for (int i = 0; i < slices.length; i++) {
        slices[i] = Groupers.getSlice(mergeBufferHolder.get(), sliceSize, i);
        sliceLocks[i] = new AtomicBoolean(true);
        bucketProcessors[i].setMergeBuffer(slices[i]);
      }
    }

    private void add(TimestampedBucketedSegmentIterators iterators)
    {
      for (int i = 0; i < iterators.iterators.length; i++) {
        add(iterators.iterators[i], i);
      }
    }

    private void add(PartitionedHashTableIterator iterator, int bucket)
    {
      List<PartitionedHashTableIterator> iteratorList = iteratorLists[bucket];
      synchronized (iteratorList) {
        iteratorList.add(iterator);
      }
    }

    private Optional<Closeable> tryLock(int bucket)
    {
      if (sliceLocks[bucket].compareAndSet(true, false)) {
        return Optional.of(() -> sliceLocks[bucket].set(true));
      }
      return Optional.empty();
    }

    private boolean isEmpty()
    {
      for (List<PartitionedHashTableIterator> iteratorList : iteratorLists) {
        synchronized (iteratorList) {
          if (!iteratorList.isEmpty()) {
            return false;
          }
        }
      }
      return true;
    }

    /**
     *
     * @param targetSize target number of rows
     * @return
     */
    private void doMerge(int targetSize) throws IOException
    {
      int remaining = targetSize;
      for (int i = 0; i < sliceLocks.length && remaining > 0; i++) {
        Optional<Closeable> lock = tryLock(i);
        if (lock.isPresent()) {
          List<PartitionedHashTableIterator> iteratorList = iteratorLists[i];
          List<PartitionedHashTableIterator> drained = new ArrayList<>();
          synchronized (iteratorList) {
            // TODO: is sort cool? benchmark?
            iteratorList.sort(
                Comparator.comparingInt(PartitionedHashTableIterator::size).reversed()
            );
            while (remaining > 0 && !iteratorList.isEmpty()) {
              PartitionedHashTableIterator it = iteratorList.remove(0);
              drained.add(it);
              remaining -= it.size();
            }
          }
          if (!drained.isEmpty()) {
            CloseableIterator<MemoryVectorEntry> concated = CloseableIterators.concat(drained);
            Closer closer = Closer.create();
            closer.register(concated);
            closer.register(lock.get());
            // TODO: should i drain all first?
            // or is this better because threads can intervene?
            // or should one merge task process one bucket only?
            bucketProcessors[i].process(CloseableIterators.wrap(concated, closer));
          }
        }
      }
    }
  }

  private static class MergeCallable implements Callable<Void>
  {
    private final BucketedIterators bucketedIterators;
    private final int targetNumRowsToMergePerTask;
    private final List<ListenableFuture<TimestampedBucketedSegmentIterators>> shuffleFutures;
    private final ListeningExecutorService exec;
    private final SettableFuture<Void> resultFuture;

    private MergeCallable(
        ListeningExecutorService exec,
        List<ListenableFuture<TimestampedBucketedSegmentIterators>> shuffleFutures,
        BucketedIterators bucketedIterators,
        int targetNumRowsToMergePerTask,
        SettableFuture<Void> resultFuture
    )
    {
      this.exec = exec;
      this.shuffleFutures = shuffleFutures;
      this.bucketedIterators = bucketedIterators;
      this.targetNumRowsToMergePerTask = targetNumRowsToMergePerTask;
      this.resultFuture = resultFuture;
    }

    @Override
    public Void call() throws Exception
    {
      if (!bucketedIterators.isEmpty()) {
        bucketedIterators.doMerge(targetNumRowsToMergePerTask);
      }
      if (!bucketedIterators.isEmpty()
          || !shuffleFutures.stream().allMatch(Future::isDone)) {
        exec.submit(
            new MergeCallable(exec, shuffleFutures, bucketedIterators, targetNumRowsToMergePerTask, resultFuture)
        );
      } else {
        resultFuture.set(null);
      }
      return null;
    }
  }

  private static class BucketProcessor implements Closeable
  {
    private final SettableSupplier<MemoryVectorEntry> currentRowSupplier = new SettableSupplier<>();
    private final SettableSupplier<ByteBuffer> mergeBufferSupplier = new SettableSupplier<>();

    private final GroupByQuery query;
    private final List<ColumnCapabilities> dimensionCapabilities;

    private final VectorColumnSelectorFactory columnSelectorFactory;
    private final List<GroupByVectorColumnSelector> columnSelectors;
    private final MemoryComparator memoryComparator;
    private final FixedSizeHashVectorGrouper grouper;

    private BucketProcessor(
        GroupByQueryConfig querySpecificConfig,
        GroupByQuery query,
        int keySize,
        int[] keyOffsets,
        @Nullable Supplier<MergedDictionary[]> mergedDictionariesSupplier,
        List<ColumnCapabilities> dimensionCapabilities,
        List<AggregatorFactory> combiningFactories
    )
    {
      this.query = query;
      this.dimensionCapabilities = dimensionCapabilities;

      this.columnSelectorFactory = new MemoryVectorEntryColumnSelectorFactory(
          query,
          currentRowSupplier,
          keySize,
          keyOffsets,
          mergedDictionariesSupplier,
          combiningFactories
      );
      this.columnSelectors = query
          .getDimensions()
          .stream()
          .map(dimensionSpec -> ColumnProcessors.makeVectorProcessor(
              dimensionSpec,
              GroupByVectorColumnProcessorFactory.instance(),
              columnSelectorFactory,
              false
          ))
          .collect(Collectors.toList());
      this.memoryComparator = GrouperBufferComparatorUtils.memoryComparator(
          false,
          query.getContextSortByDimsFirst(),
          query.getDimensions().size(),
          getDimensionComparators(columnSelectors, query.getLimitSpec())
      );
      this.grouper = new FixedSizeHashVectorGrouper(
          mergeBufferSupplier,
          1,
          keySize,
          MemoryVectorAggregators.factorizeVector(
              columnSelectorFactory,
              combiningFactories
          ),
          querySpecificConfig.getBufferGrouperMaxSize(),
          querySpecificConfig.getBufferGrouperMaxLoadFactor(),
          querySpecificConfig.getBufferGrouperInitialBuckets()
      );
    }

    private MemoryComparator[] getDimensionComparators(List<GroupByVectorColumnSelector> selectors, LimitSpec limitSpec)
    {
      MemoryComparator[] dimComparators = new MemoryComparator[selectors.size()];

      int keyOffset = 0;
      for (int i = 0; i < selectors.size(); i++) {
        final String dimName = query.getDimensions().get(i).getOutputName();
        final StringComparator stringComparator;
        if (limitSpec instanceof DefaultLimitSpec) {
          stringComparator = DefaultLimitSpec.getComparatorForDimName(
              (DefaultLimitSpec) limitSpec,
              dimName
          );
        } else {
          stringComparator = StringComparators.LEXICOGRAPHIC;
        }
        dimComparators[i] = selectors.get(i).memoryComparator(keyOffset, stringComparator);
        keyOffset += selectors.get(i).getGroupingKeySize();
      }

      return dimComparators;
    }

    private void setMergeBuffer(ByteBuffer bufferSlice)
    {
      mergeBufferSupplier.set(bufferSlice);
    }

    private void process(CloseableIterator<MemoryVectorEntry> iterator) throws IOException
    {
      grouper.initVectorized(QueryContexts.getVectorSize(query)); // TODO: can i get the max vector size in a different way? this seems no good
      //noinspection unused
      try (CloseableIterator<MemoryVectorEntry> iteratorCloser = iterator) {
        while (iterator.hasNext()) {
          final MemoryVectorEntry entry = iterator.next();
          currentRowSupplier.set(entry);
          grouper.aggregateVector(entry.getKeys(), 0, entry.curVectorSize);
        }
      }
    }

    @Override
    public void close() throws IOException
    {
      grouper.close();
    }
  }
}
