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

package org.apache.druid.query.groupby.epinephelinae.vector;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import org.apache.datasketches.memory.Memory;
import org.apache.datasketches.memory.WritableMemory;
import org.apache.druid.java.util.common.CloseableIterators;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.io.Closer;
import org.apache.druid.java.util.common.parsers.CloseableIterator;
import org.apache.druid.query.DruidProcessingConfig;
import org.apache.druid.query.QueryContexts;
import org.apache.druid.query.aggregation.MemoryVectorAggregators;
import org.apache.druid.query.filter.Filter;
import org.apache.druid.query.groupby.GroupByQuery;
import org.apache.druid.query.groupby.GroupByQueryConfig;
import org.apache.druid.query.groupby.epinephelinae.AggregateResult;
import org.apache.druid.query.groupby.epinephelinae.FixedSizeHashVectorGrouper;
import org.apache.druid.query.groupby.epinephelinae.GroupByQueryEngineV2;
import org.apache.druid.query.groupby.epinephelinae.GroupByShuffleMergingQueryRunner.TimestampedIterators;
import org.apache.druid.query.groupby.epinephelinae.Grouper.Entry;
import org.apache.druid.query.groupby.epinephelinae.VectorGrouper.MemoryComparator;
import org.apache.druid.query.groupby.orderby.DefaultLimitSpec;
import org.apache.druid.query.groupby.orderby.LimitSpec;
import org.apache.druid.query.ordering.StringComparator;
import org.apache.druid.query.ordering.StringComparators;
import org.apache.druid.query.vector.VectorCursorGranularizer;
import org.apache.druid.segment.DimensionHandlerUtils;
import org.apache.druid.segment.IdentifiableStorageAdapter;
import org.apache.druid.segment.StorageAdapter;
import org.apache.druid.segment.column.ColumnCapabilities;
import org.apache.druid.segment.filter.Filters;
import org.apache.druid.segment.vector.VectorColumnSelectorFactory;
import org.apache.druid.segment.vector.VectorCursor;
import org.joda.time.DateTime;
import org.joda.time.Interval;

import javax.annotation.Nullable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.stream.Collectors;

public class VectorGroupByEngine2
{
  public static CloseableIterator<TimestampedIterators> process(
      final GroupByQuery query,
      final IdentifiableStorageAdapter storageAdapter,
      Supplier<ByteBuffer> bufferSupplier,
      @Nullable final DateTime fudgeTimestamp,
      @Nullable final Filter filter,
      final Interval interval,
      final GroupByQueryConfig config,
      final DruidProcessingConfig processingConfig
  )
  {
    if (!VectorGroupByEngine.canVectorize(query, storageAdapter, filter)) {
      throw new ISE("Cannot vectorize");
    }

    // processingConfig.numThreads == numHashTables
    final int numHashTables = processingConfig.getNumThreads();
    final ByteBuffer processingBuffer = bufferSupplier.get();

    return new CloseableIterator<TimestampedIterators>()
    {
      CloseableIterator<TimestampedIterators> delegate;

      private CloseableIterator<TimestampedIterators> initDelegate()
      {
        final VectorCursor cursor = storageAdapter.makeVectorCursor(
            Filters.toFilter(query.getDimFilter()),
            interval,
            query.getVirtualColumns(),
            false,
            QueryContexts.getVectorSize(query),
            null
        );

        if (cursor == null) {
          // Return empty iterator.
          return CloseableIterators.withEmptyBaggage(Collections.emptyIterator());
        }

        try {
          final VectorColumnSelectorFactory columnSelectorFactory = cursor.getColumnSelectorFactory();
          final List<GroupByVectorColumnSelector> dimensions = query.getDimensions().stream().map(
              dimensionSpec ->
                  DimensionHandlerUtils.makeVectorProcessor(
                      dimensionSpec,
                      GroupByVectorColumnProcessorFactory.instance(),
                      columnSelectorFactory,
                      config.isEarlyDictMerge()
                  )
          ).collect(Collectors.toList());

          return new VectorGroupByEngineIterator(
              query,
              config,
              storageAdapter,
              cursor,
              interval,
              dimensions,
              processingBuffer,
              fudgeTimestamp,
              processingConfig.getNumThreads()
          );
        }
        catch (Throwable e) {
          try {
            cursor.close();
          }
          catch (Throwable e2) {
            e.addSuppressed(e2);
          }
          throw e;
        }
      }

      @Override
      public boolean hasNext()
      {
        if (delegate == null) {
          delegate = initDelegate();
        }
        return delegate.hasNext();
      }

      @Override
      public TimestampedIterators next()
      {
        if (!hasNext()) {
          throw new NoSuchElementException();
        }
        return delegate.next();
      }

      @Override
      public void close() throws IOException
      {
        if (delegate != null) {
          delegate.close();
        }
      }
    };
  }

  static class VectorGroupByEngineIterator implements CloseableIterator<TimestampedIterators>
  {
    private final int segmentId;
    private final GroupByQuery query;
    private final GroupByQueryConfig querySpecificConfig;
    private final StorageAdapter storageAdapter;
    private final VectorCursor cursor;
    private final List<GroupByVectorColumnSelector> selectors;
    private final ByteBuffer processingBuffer;
    private final DateTime fudgeTimestamp;
    private final int keySize;
    private final WritableMemory keySpace;

    @Nullable
    private final VectorCursorGranularizer granulizer;

    // Granularity-bucket iterator and current bucket.
    private final Iterator<Interval> bucketIterator;

    private final List<ColumnCapabilities> dimensionCapabilities;

    private final int numHashTables;

    @Nullable
    private Interval bucketInterval;

    private int partiallyAggregatedRows = -1;

    VectorGroupByEngineIterator(
        final GroupByQuery query,
        final GroupByQueryConfig config,
        final IdentifiableStorageAdapter storageAdapter,
        final VectorCursor cursor,
        final Interval queryInterval,
        final List<GroupByVectorColumnSelector> selectors,
        final ByteBuffer processingBuffer,
        @Nullable final DateTime fudgeTimestamp,
        final int numHashTables
    )
    {
      this.segmentId = storageAdapter.getId();
      this.query = query;
      this.querySpecificConfig = config;
      this.storageAdapter = storageAdapter;
      this.cursor = cursor;
      this.selectors = selectors;
      this.processingBuffer = processingBuffer;
      this.fudgeTimestamp = fudgeTimestamp;
      this.keySize = selectors.stream().mapToInt(GroupByVectorColumnSelector::getGroupingKeySize).sum();
      this.keySpace = WritableMemory.allocate(keySize * cursor.getMaxVectorSize());
      this.numHashTables = numHashTables;
      this.granulizer = VectorCursorGranularizer.create(storageAdapter, cursor, query.getGranularity(), queryInterval);

      if (granulizer != null) {
        this.bucketIterator = granulizer.getBucketIterable().iterator();
      } else {
        this.bucketIterator = Collections.emptyIterator();
      }

      this.bucketInterval = this.bucketIterator.hasNext() ? this.bucketIterator.next() : null;
      this.dimensionCapabilities = GroupByQueryEngineV2.getDimensionCapabilities(query, storageAdapter);
    }

    @Override
    public boolean hasNext()
    {
      final boolean moreToRead = !cursor.isDone() || partiallyAggregatedRows >= 0;
      return bucketInterval != null && moreToRead; // TODO: is this right?
    }

    @Override
    public TimestampedIterators next()
    {
      if (!hasNext()) {
        throw new NoSuchElementException();
      }
      return makeGrouperIterators();
    }

    @Override
    public void close() throws IOException
    {
      Closer closer = Closer.create();
      closer.register(cursor);
      closer.close();
    }

    @VisibleForTesting
    FixedSizeHashVectorGrouper makeGrouper()
    {
      final FixedSizeHashVectorGrouper grouper = new FixedSizeHashVectorGrouper(
          Suppliers.ofInstance(processingBuffer),
          numHashTables,
          keySize,
          MemoryVectorAggregators.factorizeVector(
              cursor.getColumnSelectorFactory(),
              query.getAggregatorSpecs()
          ),
          querySpecificConfig.getBufferGrouperMaxSize(),
          querySpecificConfig.getBufferGrouperMaxLoadFactor(),
          querySpecificConfig.getBufferGrouperInitialBuckets()
      );

      grouper.initVectorized(cursor.getMaxVectorSize());

      return grouper;
    }

    private TimestampedIterators makeGrouperIterators()
    {
      // Method must not be called unless there's a current bucketInterval.
      assert bucketInterval != null;

      final DateTime timestamp = fudgeTimestamp != null
                                 ? fudgeTimestamp
                                 : query.getGranularity().toDateTime(bucketInterval.getStartMillis());

      final FixedSizeHashVectorGrouper vectorGrouper = makeGrouper();

      while (!cursor.isDone()) {
        final int startOffset;

        if (partiallyAggregatedRows < 0) {
          granulizer.setCurrentOffsets(bucketInterval);
          startOffset = granulizer.getStartOffset();
        } else {
          startOffset = granulizer.getStartOffset() + partiallyAggregatedRows;
        }

        if (granulizer.getEndOffset() > startOffset) {
          // Write keys to the keySpace.
          int keyOffset = 0;
          for (final GroupByVectorColumnSelector selector : selectors) {
            selector.writeKeys(keySpace, keySize, keyOffset, startOffset, granulizer.getEndOffset());
            keyOffset += selector.getGroupingKeySize();
          }

          // Aggregate this vector.
          final AggregateResult result = vectorGrouper.aggregateVector(
              keySpace,
              startOffset,
              granulizer.getEndOffset()
          );

          if (result.isOk()) {
            partiallyAggregatedRows = -1;
          } else {
            if (partiallyAggregatedRows < 0) {
              partiallyAggregatedRows = result.getCount();
            } else {
              partiallyAggregatedRows += result.getCount();
            }
          }
        } else {
          partiallyAggregatedRows = -1;
        }

        if (partiallyAggregatedRows >= 0) {
          break;
        } else if (!granulizer.advanceCursorWithinBucket()) {
          // Advance bucketInterval.
          bucketInterval = bucketIterator.hasNext() ? bucketIterator.next() : null;
          break;
        }
      }

      final boolean resultRowHasTimestamp = query.getResultRowHasTimestamp();

      final List<CloseableIterator<Entry<ByteBuffer>>> entryIterators = vectorGrouper.iterators(segmentId);
//      System.err.println("new entry iterators for timestamp " + timestamp.getMillis() + ", grouper: " + vectorGrouper);
      return new TimestampedIterators(
          entryIterators
              .stream()
              .map(entryIterator -> new TimestampedIterator<Entry<ByteBuffer>>() {
                @Nullable
                @Override
                public DateTime getTimestamp()
                {
                  return resultRowHasTimestamp ? timestamp : null;
                }

                @Override
                public boolean hasNext()
                {
                  return entryIterator.hasNext();
                }

                @Override
                public Entry<ByteBuffer> next()
                {
                  if (!hasNext()) {
                    throw new NoSuchElementException();
                  }
                  return entryIterator.next();
                }

                @Override
                public void close() throws IOException
                {
                  Closer closer = Closer.create();
                  closer.register(vectorGrouper);
                  closer.register(entryIterator);
                  closer.close();
                }
              })
              .collect(Collectors.toList()).toArray(new TimestampedIterator[0])
      );
    }

    private MemoryComparator[] getDimensionComparators(LimitSpec limitSpec)
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
        dimComparators[i] = selectors.get(i).bufferComparator(keyOffset, stringComparator);
        keyOffset += selectors.get(i).getGroupingKeySize();
      }

      return dimComparators;
    }
  }

  public interface TimestampedIterator<T> extends CloseableIterator<T>
  {
    @Nullable
    DateTime getTimestamp();
  }

//  static class VectorGroupByEngineIterators implements Closeable
//  {
//    private final int segmentId;
//    private final GroupByQuery query;
//    private final GroupByQueryConfig querySpecificConfig;
//    private final StorageAdapter storageAdapter;
//    private final VectorCursor cursor;
//    private final List<GroupByVectorColumnSelector> selectors;
//    private final Supplier<ByteBuffer> bufferSupplier;
//    private final DateTime fudgeTimestamp;
//    private final int keySize;
//    private final WritableMemory keySpace;
////    private final HashVectorGrouper vectorGrouper;
//
//    @Nullable
//    private final VectorCursorGranularizer granulizer;
//
//    // Granularity-bucket iterator and current bucket.
//    private final Iterator<Interval> bucketIterator;
//
//    private final List<ColumnCapabilities> dimensionCapabilities;
//
//    private final int numHashTables;
//
//    @Nullable
//    private Interval bucketInterval;
//
//    private int partiallyAggregatedRows = -1;
//
//    private final AtomicBoolean closed = new AtomicBoolean(false);
//
//    VectorGroupByEngineIterators(
//        final GroupByQuery query,
//        final GroupByQueryConfig config,
//        final IdentifiableStorageAdapter storageAdapter,
//        final VectorCursor cursor,
//        final Interval queryInterval,
//        final List<GroupByVectorColumnSelector> selectors,
//        Supplier<ByteBuffer> bufferSupplier,
//        @Nullable final DateTime fudgeTimestamp,
//        final int numHashTables
//    )
//    {
//      this.segmentId = storageAdapter.getId();
//      this.query = query;
//      this.querySpecificConfig = config;
//      this.storageAdapter = storageAdapter;
//      this.cursor = cursor;
//      this.selectors = selectors;
//      this.bufferSupplier = bufferSupplier;
//      this.fudgeTimestamp = fudgeTimestamp;
//      this.keySize = selectors.stream().mapToInt(GroupByVectorColumnSelector::getGroupingKeySize).sum();
//      this.keySpace = WritableMemory.allocate(keySize * cursor.getMaxVectorSize());
//      this.numHashTables = numHashTables;
////      this.vectorGrouper = makeGrouper();
//      this.granulizer = VectorCursorGranularizer.create(storageAdapter, cursor, query.getGranularity(), queryInterval);
//
//      if (granulizer != null) {
//        this.bucketIterator = granulizer.getBucketIterable().iterator();
//      } else {
//        this.bucketIterator = Collections.emptyIterator();
//      }
//
//      this.bucketInterval = this.bucketIterator.hasNext() ? this.bucketIterator.next() : null;
//      this.dimensionCapabilities = GroupByQueryEngineV2.getDimensionCapabilities(query, storageAdapter);
//    }
//
//    @VisibleForTesting
//    HashVectorGrouper makeGrouper()
//    {
//      final HashVectorGrouper grouper;
//
//      grouper = new HashVectorGrouper(
//          Suppliers.ofInstance(bufferSupplier.get()),
//          numHashTables,
//          keySize,
//          AggregatorAdapters.factorizeVector(
//              cursor.getColumnSelectorFactory(),
//              query.getAggregatorSpecs()
//          ),
//          querySpecificConfig.getBufferGrouperMaxSize(),
//          querySpecificConfig.getBufferGrouperMaxLoadFactor(),
//          querySpecificConfig.getBufferGrouperInitialBuckets()
//      );
//
//      grouper.initVectorized(cursor.getMaxVectorSize());
//
//      return grouper;
//    }
//
//    private List<CloseableIterator<ResultRow>> totalIterators(int numHashTables)
//    {
//      // list of hash-partitioned iterators
//      final List<List<CloseableGrouperIterator<Memory, ResultRow>>> iteratorLists = new ArrayList<>();
//      iteratorLists.add(grouperIterators());
//      final List<CloseableIterator<ResultRow>> iterators = new ArrayList<>(numHashTables);
//      for (int i = 0; i < numHashTables; i++) {
//        final int tablePointer = i;
//        iterators.add(
//            new CloseableIterator<ResultRow>()
//            {
//              int currentIteratorPointer = 0;
//              CloseableGrouperIterator<Memory, ResultRow> iterator = iteratorLists.get(currentIteratorPointer).get(tablePointer);
//
//              @Override
//              public boolean hasNext()
//              {
//                while (!iterator.hasNext()) {
//                  iterator.close();
//                  currentIteratorPointer++;
//                  if (currentIteratorPointer == iteratorLists.size()) {
//                    final boolean moreToRead = !cursor.isDone() || partiallyAggregatedRows >= 0;
//
//                    if (bucketInterval != null && moreToRead) {
//                      iteratorLists.add(grouperIterators());
//                    }
//                  }
//                  if (currentIteratorPointer < iteratorLists.size()) {
//                    iterator = iteratorLists.get(currentIteratorPointer).get(tablePointer);
//                  } else {
//                    assert !iterator.hasNext();
//                    break;
//                  }
//                }
//                return iterator.hasNext();
//              }
//
//              @Override
//              public ResultRow next()
//              {
//                if (!iterator.hasNext()) {
//                  throw new NoSuchElementException();
//                }
//                return iterator.next();
//              }
//
//              @Override
//              public void close() throws IOException
//              {
//                for (int i = currentIteratorPointer; i < iteratorLists.size(); i++) {
//                  iteratorLists.get(i).get(tablePointer).close();
//                }
//              }
//            }
//        );
//      }
//      return iterators;
//    }
//
//    private List<CloseableGrouperIterator<Memory, ResultRow>> grouperIterators()
//    {
//      // Method must not be called unless there's a current bucketInterval.
//      assert bucketInterval != null;
//
//      final DateTime timestamp = fudgeTimestamp != null
//                                 ? fudgeTimestamp
//                                 : query.getGranularity().toDateTime(bucketInterval.getStartMillis());
//
//      final HashVectorGrouper vectorGrouper = makeGrouper();
//
//      while (!cursor.isDone()) {
//        final int startOffset;
//
//        if (partiallyAggregatedRows < 0) {
//          granulizer.setCurrentOffsets(bucketInterval);
//          startOffset = granulizer.getStartOffset();
//        } else {
//          startOffset = granulizer.getStartOffset() + partiallyAggregatedRows;
//        }
//
//        if (granulizer.getEndOffset() > startOffset) {
//          // Write keys to the keySpace.
//          int keyOffset = 0;
//          for (final GroupByVectorColumnSelector selector : selectors) {
//            selector.writeKeys(keySpace, keySize, keyOffset, startOffset, granulizer.getEndOffset());
//            keyOffset += selector.getGroupingKeySize();
//          }
//
//          // Aggregate this vector.
//          final AggregateResult result = vectorGrouper.aggregateVector(
//              keySpace,
//              startOffset,
//              granulizer.getEndOffset()
//          );
//
//          if (result.isOk()) {
//            partiallyAggregatedRows = -1;
//          } else {
//            if (partiallyAggregatedRows < 0) {
//              partiallyAggregatedRows = result.getCount();
//            } else {
//              partiallyAggregatedRows += result.getCount();
//            }
//          }
//        } else {
//          partiallyAggregatedRows = -1;
//        }
//
//        if (partiallyAggregatedRows >= 0) {
//          break;
//        } else if (!granulizer.advanceCursorWithinBucket()) {
//          // Advance bucketInterval.
//          bucketInterval = bucketIterator.hasNext() ? bucketIterator.next() : null;
//          break;
//        }
//      }
//
//      final boolean resultRowHasTimestamp = query.getResultRowHasTimestamp();
//      final int resultRowDimensionStart = query.getResultRowDimensionStart();
//      final int resultRowAggregatorStart = query.getResultRowAggregatorStart();
//
//      final List<CloseableIterator<Entry<Memory>>> entryIterators = vectorGrouper.iterators();
////      System.err.println("new entry iterators for timestamp " + timestamp.getMillis() + ", grouper: " + vectorGrouper);
//      return entryIterators
//          .stream()
//          .map(entryIterator -> new CloseableGrouperIterator<>(
//              entryIterator,
//              entry -> {
//                final PerSegmentEncodedResultRow resultRow =
//                    PerSegmentEncodedResultRow.create(query.getResultRowSizeWithoutPostAggregators());
//
//                // Add timestamp, if necessary.
//                if (resultRowHasTimestamp) {
//                  resultRow.set(0, segmentId, timestamp.getMillis());
//                }
//
//                // Add dimensions.
//                int keyOffset = 0;
//                for (int i = 0; i < selectors.size(); i++) {
//                  final GroupByVectorColumnSelector selector = selectors.get(i);
//
//                  selector.writeKeyToResultRow(
//                      entry.getKey(),
//                      keyOffset,
//                      resultRow,
//                      resultRowDimensionStart + i,
//                      segmentId
//                  );
//
//                  keyOffset += selector.getGroupingKeySize();
//                }
//
//                // Convert dimension values to desired output types, possibly.
//                GroupByQueryEngineV2.convertRowTypesToOutputTypes(
//                    query.getDimensions(),
//                    resultRow,
//                    resultRowDimensionStart,
//                    segmentId,
//                    dimensionCapabilities,
//                    querySpecificConfig.isEarlyDictMerge()
//                );
//
//                // Add aggregations.
//                for (int i = 0; i < entry.getValues().length; i++) {
//                  resultRow.set(resultRowAggregatorStart + i, segmentId, entry.getValues()[i]);
//                }
//
////                System.err.println(Thread.currentThread().getName() + ", query interval: " + query.getIntervals() + " segmentId: " + segmentId + ", row: " + resultRow + " timestamp: " + timestamp.getMillis());
//
//                return (ResultRow) resultRow;
//              },
//              vectorGrouper
//          ))
//          .collect(Collectors.toList());
//    }
//
//    @Override
//    public void close() throws IOException
//    {
//      if (closed.compareAndSet(false, true)) {
//        Closer closer = Closer.create();
//        closer.register(cursor);
//        closer.close();
//      }
//    }
//  }
}
