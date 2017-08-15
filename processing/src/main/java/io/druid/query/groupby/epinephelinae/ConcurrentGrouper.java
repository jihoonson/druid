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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Preconditions;
import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import io.druid.java.util.common.ISE;
import io.druid.query.AbstractPrioritizedCallable;
import io.druid.query.QueryInterruptedException;
import io.druid.query.aggregation.AggregatorFactory;
import io.druid.query.dimension.DimensionSpec;
import io.druid.query.groupby.orderby.DefaultLimitSpec;
import io.druid.query.monomorphicprocessing.RuntimeShapeInspector;
import io.druid.segment.ColumnSelectorFactory;
import io.druid.segment.DimensionSelector;
import io.druid.segment.DoubleColumnSelector;
import io.druid.segment.FloatColumnSelector;
import io.druid.segment.LongColumnSelector;
import io.druid.segment.ObjectColumnSelector;
import io.druid.segment.column.ColumnCapabilities;

import javax.annotation.Nullable;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

/**
 * Grouper based around a set of underlying {@link SpillingGrouper} instances. Thread-safe.
 * <p>
 * The passed-in buffer is cut up into concurrencyHint slices, and each slice is passed to a different underlying
 * grouper. Access to each slice is separately synchronized. As long as the result set fits in memory, keys are
 * partitioned between buffers based on their hash, and multiple threads can write into the same buffer. When
 * it becomes clear that the result set does not fit in memory, the table switches to a mode where each thread
 * gets its own buffer and its own spill files on disk.
 */
public class ConcurrentGrouper<KeyType> implements Grouper<KeyType>
{
  private final List<SpillingGrouper<KeyType>> groupers;
  private final ThreadLocal<SpillingGrouper<KeyType>> threadLocalGrouper;
  private final AtomicInteger threadNumber = new AtomicInteger();
  private volatile boolean spilling = false;
  private volatile boolean closed = false;

  private final Supplier<ByteBuffer> bufferSupplier;
  private final ColumnSelectorFactory columnSelectorFactory;
  private final AggregatorFactory[] aggregatorFactories;
  private final int bufferGrouperMaxSize;
  private final float bufferGrouperMaxLoadFactor;
  private final int bufferGrouperInitialBuckets;
  private final LimitedTemporaryStorage temporaryStorage;
  private final ObjectMapper spillMapper;
  private final int concurrencyHint;
  private final KeySerdeFactory<KeyType> keySerdeFactory;
  private final DefaultLimitSpec limitSpec;
  private final boolean sortHasNonGroupingFields;
  private final Comparator<Grouper.Entry<KeyType>> keyObjComparator;
  private final ListeningExecutorService grouperSorter;
  private final int priority;
  private final boolean hasQueryTimeout;
  private final long queryTimeoutAt;

  private volatile boolean initialized = false;

  public ConcurrentGrouper(
      final Supplier<ByteBuffer> bufferSupplier,
      final KeySerdeFactory<KeyType> keySerdeFactory,
      final ColumnSelectorFactory columnSelectorFactory,
      final AggregatorFactory[] aggregatorFactories,
      final int bufferGrouperMaxSize,
      final float bufferGrouperMaxLoadFactor,
      final int bufferGrouperInitialBuckets,
      final LimitedTemporaryStorage temporaryStorage,
      final ObjectMapper spillMapper,
      final int concurrencyHint,
      final DefaultLimitSpec limitSpec,
      final boolean sortHasNonGroupingFields,
      final ListeningExecutorService grouperSorter,
      final int priority,
      final boolean hasQueryTimeout,
      final long queryTimeoutAt
  )
  {
    Preconditions.checkArgument(concurrencyHint > 0, "concurrencyHint > 0");

    this.groupers = new ArrayList<>(concurrencyHint);
    this.threadLocalGrouper = new ThreadLocal<SpillingGrouper<KeyType>>()
    {
      @Override
      protected SpillingGrouper<KeyType> initialValue()
      {
        return groupers.get(threadNumber.getAndIncrement());
      }
    };

    this.bufferSupplier = bufferSupplier;
    this.columnSelectorFactory = columnSelectorFactory;
    this.aggregatorFactories = aggregatorFactories;
    this.bufferGrouperMaxSize = bufferGrouperMaxSize;
    this.bufferGrouperMaxLoadFactor = bufferGrouperMaxLoadFactor;
    this.bufferGrouperInitialBuckets = bufferGrouperInitialBuckets;
    this.temporaryStorage = temporaryStorage;
    this.spillMapper = spillMapper;
    this.concurrencyHint = concurrencyHint;
    this.keySerdeFactory = keySerdeFactory;
    this.limitSpec = limitSpec;
    this.sortHasNonGroupingFields = sortHasNonGroupingFields;
    this.keyObjComparator = keySerdeFactory.objectComparator(sortHasNonGroupingFields);
    this.grouperSorter = Preconditions.checkNotNull(grouperSorter);
    this.priority = priority;
    this.hasQueryTimeout = hasQueryTimeout;
    this.queryTimeoutAt = queryTimeoutAt;
  }

  @Override
  public void init()
  {
    if (!initialized) {
      synchronized (bufferSupplier) {
        if (!initialized) {
          final ByteBuffer buffer = bufferSupplier.get();
          final int sliceSize = (buffer.capacity() / concurrencyHint);

          for (int i = 0; i < concurrencyHint; i++) {
            final ByteBuffer slice = buffer.duplicate();
            slice.position(sliceSize * i);
            slice.limit(slice.position() + sliceSize);
            final SpillingGrouper<KeyType> grouper = new SpillingGrouper<>(
                Suppliers.ofInstance(slice.slice()),
                keySerdeFactory,
                columnSelectorFactory,
                aggregatorFactories,
                bufferGrouperMaxSize,
                bufferGrouperMaxLoadFactor,
                bufferGrouperInitialBuckets,
                temporaryStorage,
                spillMapper,
                false,
                limitSpec,
                sortHasNonGroupingFields
            );
            grouper.init();
            grouper.setSpillingAllowed(true);
            groupers.add(grouper);
          }

          initialized = true;
        }
      }
    }
  }

  @Override
  public boolean isInitialized()
  {
    return initialized;
  }

  @Override
  public AggregateResult aggregate(KeyType key, int keyHash)
  {
    if (!initialized) {
      throw new ISE("Grouper is not initialized");
    }

    if (closed) {
      throw new ISE("Grouper is closed");
    }

//    if (!spilling) {
//      final SpillingGrouper<KeyType> hashBasedGrouper = groupers.get(grouperNumberForKeyHash(keyHash));
//
//      synchronized (hashBasedGrouper) {
//        if (!spilling) {
//          if (hashBasedGrouper.aggregate(key, keyHash).isOk()) {
//            return AggregateResult.ok();
//          } else {
//            spilling = true;
//          }
//        }
//      }
//    }
//
//    // At this point we know spilling = true
//    final SpillingGrouper<KeyType> tlGrouper = threadLocalGrouper.get();
//
//    synchronized (tlGrouper) {
//      tlGrouper.setSpillingAllowed(true);
//      return tlGrouper.aggregate(key, keyHash);
//    }

    final SpillingGrouper<KeyType> tlGrouper = threadLocalGrouper.get();
    return tlGrouper.aggregate(key, keyHash);
  }

  @Override
  public void reset()
  {
    if (!initialized) {
      throw new ISE("Grouper is not initialized");
    }

    if (closed) {
      throw new ISE("Grouper is closed");
    }

    for (Grouper<KeyType> grouper : groupers) {
      synchronized (grouper) {
        grouper.reset();
      }
    }
  }

  @Override
  public Iterator<Entry<KeyType>> iterator(final boolean sorted)
  {
    if (!initialized) {
      throw new ISE("Grouper is not initialized");
    }

    if (closed) {
      throw new ISE("Grouper is closed");
    }

//    return Groupers.mergeIterators(
//        sorted && isParallelSortAvailable() ? parallelSortAndGetGroupersIterator() : getGroupersIterator(sorted),
//        sorted ? keyObjComparator : null
//    );

    return parallelCombine(
        sorted && isParallelSortAvailable() ? parallelSortAndGetGroupersIterator() : getGroupersIterator(sorted),
        sorted
    );
  }

  private Iterator<Entry<KeyType>> parallelCombine(List<Iterator<Entry<KeyType>>> sortedIterators, boolean sorted)
  {
    final List<ListenableFuture<Iterator<Entry<KeyType>>>> mergedIterators = new ArrayList<>();

    for (int i = 0; i < sortedIterators.size(); i += 2) {
      final Iterator<Entry<KeyType>> subIter = Groupers.mergeIterators(
          sortedIterators.subList(i, Math.min(i + 2, sortedIterators.size())),
          sorted ? keyObjComparator : null
      );
      mergedIterators.add(
          grouperSorter.submit(() -> {
            final SettableColumnSelectorFactory settableColumnSelectorFactory = new SettableColumnSelectorFactory(aggregatorFactories);
            final MergeSortedGrouper<KeyType> mergeGrouper = new MergeSortedGrouper<>(
                bufferSupplier,
                keySerdeFactory.factorize(),
                settableColumnSelectorFactory,
                aggregatorFactories
            );
            mergeGrouper.init();

            while (subIter.hasNext()) {
              final Entry<KeyType> next = subIter.next();

              settableColumnSelectorFactory.set(next.values);
              mergeGrouper.aggregate(next.getKey());
              settableColumnSelectorFactory.set(null);
            }

            mergeGrouper.finish();

            return mergeGrouper.iterator(true);
          })
      );
    }

    final Future<List<Iterator<Entry<KeyType>>>> allFuture = Futures.allAsList(mergedIterators);
    final Iterator<Entry<KeyType>> mergedIterator;
    try {
      mergedIterator = Groupers.mergeIterators(allFuture.get(), sorted ? keyObjComparator : null);
    }
    catch (InterruptedException | ExecutionException e) {
      throw new RuntimeException(e);
    }

    final SettableColumnSelectorFactory settableColumnSelectorFactory = new SettableColumnSelectorFactory(aggregatorFactories);
    final MergeSortedGrouper<KeyType> mergeGrouper = new MergeSortedGrouper<>(
        bufferSupplier,
        keySerdeFactory.factorize(),
        settableColumnSelectorFactory,
        aggregatorFactories
    );
    mergeGrouper.init();

    while (mergedIterator.hasNext()) {
      final Entry<KeyType> next = mergedIterator.next();

      settableColumnSelectorFactory.set(next.values);
      mergeGrouper.aggregate(next.getKey());
      settableColumnSelectorFactory.set(null);
    }

    mergeGrouper.finish();

    return mergeGrouper.iterator(true);
  }

  private boolean isParallelSortAvailable()
  {
    return concurrencyHint > 1;
  }

  private List<Iterator<Entry<KeyType>>> parallelSortAndGetGroupersIterator()
  {
    // The number of groupers is same with the number of processing threads in grouperSorter
    final ListenableFuture<List<Iterator<Entry<KeyType>>>> future = Futures.allAsList(
        groupers.stream()
                .map(grouper ->
                         grouperSorter.submit(
                             new AbstractPrioritizedCallable<Iterator<Entry<KeyType>>>(priority)
                             {
                               @Override
                               public Iterator<Entry<KeyType>> call() throws Exception
                               {
                                 return grouper.iterator(true);
                               }
                             }
                         )
                )
                .collect(Collectors.toList())
    );

    try {
      final long timeout = queryTimeoutAt - System.currentTimeMillis();
      return hasQueryTimeout ? future.get(timeout, TimeUnit.MILLISECONDS) : future.get();
    }
    catch (InterruptedException | TimeoutException e) {
      future.cancel(true);
      throw new QueryInterruptedException(e);
    }
    catch (CancellationException e) {
      throw new QueryInterruptedException(e);
    }
    catch (ExecutionException e) {
      throw new RuntimeException(e.getCause());
    }
  }

  private List<Iterator<Entry<KeyType>>> getGroupersIterator(boolean sorted)
  {
    return groupers.stream()
                   .map(grouper -> grouper.iterator(sorted))
                   .collect(Collectors.toList());
  }

  @Override
  public void close()
  {
    closed = true;
    for (Grouper<KeyType> grouper : groupers) {
      synchronized (grouper) {
        grouper.close();
      }
    }
  }

  private int grouperNumberForKeyHash(int keyHash)
  {
    return keyHash % groupers.size();
  }

  private static class SettableColumnSelectorFactory implements ColumnSelectorFactory
  {
    private final Map<String, Integer> columnIndexMap;

    private Object[] values;

    SettableColumnSelectorFactory(AggregatorFactory[] aggregatorFactories)
    {
      columnIndexMap = new HashMap<>(aggregatorFactories.length);
      for (int i = 0; i < aggregatorFactories.length; i++) {
        columnIndexMap.put(aggregatorFactories[i].getName(), i);
      }
    }

    public void set(Object[] values)
    {
      this.values = values;
    }

    @Override
    public DimensionSelector makeDimensionSelector(DimensionSpec dimensionSpec)
    {
      throw new UnsupportedOperationException();
    }

    @Override
    public FloatColumnSelector makeFloatColumnSelector(String columnName)
    {
      return new FloatColumnSelector()
      {
        @Override
        public float getFloat()
        {
          return ((Number) values[columnIndexMap.get(columnName)]).floatValue();
        }

        @Override
        public void inspectRuntimeShape(RuntimeShapeInspector inspector)
        {

        }
      };
    }

    @Override
    public LongColumnSelector makeLongColumnSelector(String columnName)
    {
      return new LongColumnSelector()
      {
        @Override
        public long getLong()
        {
          return ((Number) values[columnIndexMap.get(columnName)]).longValue();
        }

        @Override
        public void inspectRuntimeShape(RuntimeShapeInspector inspector)
        {

        }
      };
    }

    @Override
    public DoubleColumnSelector makeDoubleColumnSelector(String columnName)
    {
      return new DoubleColumnSelector()
      {
        @Override
        public double getDouble()
        {
          return ((Number) values[columnIndexMap.get(columnName)]).doubleValue();
        }

        @Override
        public void inspectRuntimeShape(RuntimeShapeInspector inspector)
        {

        }
      };
    }

    @Nullable
    @Override
    public ObjectColumnSelector makeObjectColumnSelector(String columnName)
    {
      return new ObjectColumnSelector()
      {
        @Override
        public Class classOfObject()
        {
          return Object.class;
        }

        @Override
        public Object get()
        {
          return values[columnIndexMap.get(columnName)];
        }
      };
    }

    @Nullable
    @Override
    public ColumnCapabilities getColumnCapabilities(String column)
    {
      throw new UnsupportedOperationException();
    }
  }
}
