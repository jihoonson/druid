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
import com.google.common.base.Predicates;
import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import org.apache.datasketches.memory.Memory;
import org.apache.druid.collections.BlockingPool;
import org.apache.druid.collections.ReferenceCountingResourceHolder;
import org.apache.druid.collections.Releaser;
import org.apache.druid.common.guava.GuavaUtils;
import org.apache.druid.java.util.common.CloseableIterators;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.guava.BaseSequence;
import org.apache.druid.java.util.common.guava.ConcatSequence;
import org.apache.druid.java.util.common.guava.Sequence;
import org.apache.druid.java.util.common.guava.Sequences;
import org.apache.druid.java.util.common.io.Closer;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.java.util.common.parsers.CloseableIterator;
import org.apache.druid.query.AbstractPrioritizedCallable;
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
import org.apache.druid.query.groupby.GroupByQuery;
import org.apache.druid.query.groupby.GroupByQueryConfig;
import org.apache.druid.query.groupby.ResultRow;
import org.apache.druid.query.groupby.epinephelinae.GroupByMergingQueryRunnerV3.MergingDictionary;
import org.apache.druid.query.groupby.epinephelinae.GroupByQueryEngineV2.GroupByEngineKeySerde;
import org.apache.druid.query.groupby.epinephelinae.Grouper.BufferComparator;
import org.apache.druid.query.groupby.epinephelinae.Grouper.Entry;
import org.apache.druid.query.groupby.epinephelinae.Grouper.KeySerde;
import org.apache.druid.query.groupby.epinephelinae.RowBasedGrouperHelper.RowBasedKey;
import org.apache.druid.query.groupby.epinephelinae.column.GroupByColumnSelectorPlus;
import org.apache.druid.query.groupby.epinephelinae.vector.VectorGroupByEngine2.TimestampedIterator;
import org.joda.time.DateTime;

import javax.annotation.Nullable;
import java.io.File;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;

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
  private final DictionaryMergingQueryRunner dictionaryMergingRunner;
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
      DictionaryMergingQueryRunner dictionaryMergingRunner,
      int numHashBuckets
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
    this.numHashBuckets = numHashBuckets;
  }

  @Override
  public Sequence<ResultRow> run(QueryPlus<ResultRow> queryPlus, ResponseContext responseContext)
  {
    final GroupByQuery query = (GroupByQuery) queryPlus.getQuery();
    final GroupByQueryConfig querySpecificConfig = config.withOverrides(query);

    final QueryPlus<ResultRow> queryPlusForRunners = queryPlus
        .withQuery(query)
        .withoutThreadUnsafeState();

    final boolean isSingleThreaded = querySpecificConfig.isSingleThreaded();

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
        new BaseSequence.IteratorMaker<ResultRow, CloseableGrouperIterator<RowBasedKey, ResultRow>>()
        {
          @Override
          public CloseableGrouperIterator<RowBasedKey, ResultRow> make()
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
              final ByteBuffer mergeBuffer = mergeBufferHolder.get();
              final int sliceSize = mergeBufferSize / numHashBuckets;

              final Supplier<MergedDictionary[]> mergedDictionariesSupplier;
              if (config.isEarlyDictMerge() && dictionaryMergingRunner != null) { // TODO: no null check
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
              CloseableIterator<TimestampedIterators> timeSortedHashedIterators = CloseableIterators.mergeSorted(
                  hashedSequencesList,
                  Comparator.comparing(TimestampedIterators::getTimestamp)
              );

              DateTime currentTime = null;
              TimestampedIterators timestampedIterators = null;
              while (timeSortedHashedIterators.hasNext()) {
                List<TimestampedIterator<Entry<Memory>>>[] partitionedIterators = new List[numHashBuckets];
                for (int i = 0; i < numHashBuckets; i++) {
                  partitionedIterators[i] = new ArrayList<>();
                }
                if (timestampedIterators != null) {
                  currentTime = timestampedIterators.getTimestamp();
                  for (int i = 0; i < timestampedIterators.iterators.length; i++) {
                    partitionedIterators[i].add(timestampedIterators.iterators[i]);
                  }
                  timestampedIterators = null;
                }
                while (timeSortedHashedIterators.hasNext()) {
                  timestampedIterators = timeSortedHashedIterators.next();
                  if (currentTime == null || currentTime.equals(timestampedIterators.getTimestamp())) {
                    currentTime = timestampedIterators.getTimestamp();
                    for (int i = 0; i < timestampedIterators.iterators.length; i++) {
                      partitionedIterators[i].add(timestampedIterators.iterators[i]);
                    }
                  } else {
                    break;
                  }
                }

                final List<ListenableFuture<CloseableIterator<Entry<ByteBuffer>>>> futures = new ArrayList<>();
                for (int i = 0; i < numHashBuckets; i++) {
                  final CloseableIterator<Entry<Memory>> concatIterator = CloseableIterators.concat(
                      partitionedIterators[i]
                  );
                  // TODO: memory -> byteBuffer & dictionary conversion

                  // TODO: maybe i can just use Grouper<ByteBuffer> with GroupByEngineKeySerde
                  final Grouper<Memory> grouper = new BufferHashGrouper<>(
                      Suppliers.ofInstance(Groupers.getSlice(mergeBuffer, sliceSize, i)),
                      new KeySerde<Memory>()
                      {
                        @Override
                        public int keySize()
                        {
                          // TODO
                          return 0;
                        }

                        @Override
                        public Class<Memory> keyClazz()
                        {
                          return Memory.class;
                        }

                        @Override
                        public List<String> getDictionary()
                        {
                          // no dictionary to spill
                          return null;
                        }

                        @Nullable
                        @Override
                        public ByteBuffer toByteBuffer(Memory key)
                        {
                          return key.getByteBuffer();
                        }

                        @Override
                        public Memory fromByteBuffer(ByteBuffer buffer, int position)
                        {
                          if (buffer.position() != position && buffer.limit() != position + keySize()) {
                            buffer.position(position);
                            buffer.limit(position + keySize());
                            return Memory.wrap(buffer.slice());
                          } else {
                            return Memory.wrap(buffer);
                          }
                        }

                        @Override
                        public BufferComparator bufferComparator()
                        {
                          // TODO
                          return null;
                        }

                        @Override
                        public BufferComparator bufferComparatorWithAggregators(
                            AggregatorFactory[] aggregatorFactories,
                            int[] aggregatorOffsets
                        )
                        {
                          throw new UnsupportedOperationException();
                        }

                        @Override
                        public void reset()
                        {
                          // nothing to reset
                        }
                      },
                      AggregatorAdapters.factorizeBuffered(
                          selectorFactory,
                          query.getAggregatorSpecs()
                      ),
                      querySpecificConfig.getBufferGrouperMaxSize(),
                      querySpecificConfig.getBufferGrouperMaxLoadFactor(),
                      querySpecificConfig.getBufferGrouperInitialBuckets(),
                      true
                  );
                  while (concatIterator.hasNext()) {
                    final Entry<Memory> memoryEntry = concatIterator.next();
                    // TODO: update metrics columnValueSelector
                    grouper.aggregate(memoryEntry.getKey());
                  }
                }
              }



              List<TimestampedIterator<Entry<Memory>>[]> partitionedIteratorsForQueryables = hashedSequencesList
                  .stream()
                  .filter(Iterator::hasNext)
                  .map(Iterator::next)
                  .collect(Collectors.toList());

              while (!partitionedIteratorsForQueryables.isEmpty()) {
                // group iterators by hash bucket
                TimestampedIterator<Entry<Memory>>[] groupedIterators = new TimestampedIterator[numHashBuckets];
                for (int i = 0; i < numHashBuckets; i++) {

                }
              }



              for (int i = 0; i < numHashBuckets; i++) {
                final List<Sequence<ResultRow>> sequencesToProcess = new ArrayList<>(hashedSequencesList.size()); // # of queryables
                for (List<Sequence<ResultRow>> sequencesPerQueryable : hashedSequencesList) {
                  sequencesToProcess.add(sequencesPerQueryable.get(i));
                }
                final Sequence<ResultRow> concatSequence = new ConcatSequence<>(Sequences.simple(sequencesToProcess));
                ListenableFuture<AggregateResult> future = exec.submit(
                    new AbstractPrioritizedCallable<AggregateResult>(priority)
                    {
                      @Override
                      public AggregateResult call()
                      {
                        try (
                            // These variables are used to close releasers automatically.
                            @SuppressWarnings("unused")
                            Releaser bufferReleaser = mergeBufferHolder.increment();
                            @SuppressWarnings("unused")
                            Releaser grouperReleaser = grouperHolder.increment()
                        ) {

                          // Return true if OK, false if resources were exhausted.
                          return concatSequence.accumulate(AggregateResult.ok(), accumulator);
                        }
                        catch (QueryInterruptedException e) {
                          throw e;
                        }
                        catch (Exception e) {
                          log.error(e, "Exception with one of the sequences!");
                          throw new RuntimeException(e);
                        }
                      }
                    }
                );
                futures.add(future);
              }

              if (!isSingleThreaded) {
                waitForFutureCompletion(
                    query,
                    futures,
                    hasTimeout,
                    timeoutAt - System.currentTimeMillis()
                );
              }

              return RowBasedGrouperHelper.makeGrouperIterator(
                  grouper,
                  query,
                  resources,
                  mergedDictionariesSupplier
              );
            }
            catch (Throwable t) {
              // Exception caught while setting up the iterator; release resources.
              try {
                resources.close();
              }
              catch (Exception ex) {
                t.addSuppressed(ex);
              }
              throw t;
            }
          }

          @Override
          public void cleanup(CloseableGrouperIterator<RowBasedKey, ResultRow> iterFromMake)
          {
            iterFromMake.close();
          }
        }
    );
  }

  private interface MemoryColumnFunction
  {
    int serializedSize();

    Comparable[] deserialize(Memory memory);
  }

  private static class StringMemoryColumnFunction implements MemoryColumnFunction
  {
    private final long offset;

    private StringMemoryColumnFunction(long offset)
    {
      this.offset = offset;
    }

    @Override
    public int serializedSize()
    {
      return Integer.BYTES;
    }

    @Override
    public Comparable[] deserialize(Memory memory)
    {
      // TODO: handle multi-valued columns later..
      return new Comparable[]{memory.getInt(offset)};
    }
  }

  public static class TimestampedIterators
  {
    private final TimestampedIterator<Entry<Memory>>[] iterators;

    TimestampedIterators(TimestampedIterator<Entry<Memory>>[] iterators)
    {
      this.iterators = iterators;
    }

    @Nullable
    DateTime getTimestamp()
    {
      return iterators[0].getTimestamp();
    }

    TimestampedIterator<Entry<Memory>> getIterator(int hashBucket)
    {
      assert hashBucket < iterators.length;
      return iterators[hashBucket];
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

  private void waitForFutureCompletion(
      GroupByQuery query,
      List<ListenableFuture<AggregateResult>> futures,
      boolean hasTimeout,
      long timeout
  )
  {
    ListenableFuture<List<AggregateResult>> future = Futures.allAsList(futures);
    try {
      if (queryWatcher != null) {
        queryWatcher.registerQueryFuture(query, future);
      }

      if (hasTimeout && timeout <= 0) {
        throw new TimeoutException();
      }

      final List<AggregateResult> results = hasTimeout ? future.get(timeout, TimeUnit.MILLISECONDS) : future.get();

      for (AggregateResult result : results) {
        if (!result.isOk()) {
          GuavaUtils.cancelAll(true, future, futures);
          throw new ResourceLimitExceededException(result.getReason());
        }
      }
    }
    catch (InterruptedException e) {
      log.warn(e, "Query interrupted, cancelling pending results, query id [%s]", query.getId());
      GuavaUtils.cancelAll(true, future, futures);
      throw new QueryInterruptedException(e);
    }
    catch (CancellationException e) {
      GuavaUtils.cancelAll(true, future, futures);
      throw new QueryInterruptedException(e);
    }
    catch (TimeoutException e) {
      log.info("Query timeout, cancelling pending results for query id [%s]", query.getId());
      GuavaUtils.cancelAll(true, future, futures);
      throw new QueryInterruptedException(e);
    }
    catch (ExecutionException e) {
      GuavaUtils.cancelAll(true, future, futures);
      throw new RuntimeException(e);
    }
  }

  private static class MemoryKeySerde implements KeySerde<Memory>
  {
    private final int keySize;
    private final GroupByColumnSelectorPlus[] dims;
    private final GroupByQuery query;

    public MemoryKeySerde(GroupByColumnSelectorPlus[] dims, GroupByQuery query)
    {
      this.dims = dims;
      int keySize = 0;
      for (GroupByColumnSelectorPlus selectorPlus : dims) {
        keySize += selectorPlus.getColumnSelectorStrategy().getGroupingKeySize();
      }
      this.keySize = keySize;

      this.query = query;
    }

    @Override
    public int keySize()
    {
      return keySize;
    }

    @Override
    public Class<Memory> keyClazz()
    {
      return Memory.class;
    }

    @Override
    public List<String> getDictionary()
    {
      return ImmutableList.of(); // TODO: this is for spilling dictionary. do i need it? or do we every need it if we use merged dictionary?
    }

    @Nullable
    @Override
    public ByteBuffer toByteBuffer(Memory key)
    {
      return key.getByteBuffer(); // TODO: is this correct?
    }

    @Override
    public Memory fromByteBuffer(ByteBuffer buffer, int position)
    {
      return null;
    }

    @Override
    public BufferComparator bufferComparator()
    {
      return null;
    }

    @Override
    public BufferComparator bufferComparatorWithAggregators(
        AggregatorFactory[] aggregatorFactories,
        int[] aggregatorOffsets
    )
    {
      return null;
    }

    @Override
    public void reset()
    {

    }
  }
}
