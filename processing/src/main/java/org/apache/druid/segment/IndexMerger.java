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

package org.apache.druid.segment;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Function;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.google.common.collect.PeekingIterator;
import com.google.inject.ImplementedBy;
import org.apache.druid.common.config.NullHandling;
import org.apache.druid.common.utils.SerializerUtils;
import org.apache.druid.data.input.impl.DimensionsSpec;
import org.apache.druid.java.util.common.ByteBufferUtils;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.Pair;
import org.apache.druid.java.util.common.guava.Comparators;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.java.util.common.parsers.CloseableIterator;
import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.segment.data.Indexed;
import org.apache.druid.segment.incremental.IncrementalIndex;
import org.apache.druid.segment.writeout.SegmentWriteOutMediumFactory;
import org.apache.druid.utils.CollectionUtils;
import org.joda.time.Interval;

import javax.annotation.Nullable;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.IntBuffer;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.PriorityQueue;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Collectors;

@ImplementedBy(IndexMergerV9.class)
public interface IndexMerger
{
  Logger log = new Logger(IndexMerger.class);

  SerializerUtils SERIALIZER_UTILS = new SerializerUtils();
  int INVALID_ROW = -1;
  int UNLIMITED_MAX_COLUMNS_TO_MERGE = -1;

  static List<String> getMergedDimensionsFromQueryableIndexes(
      List<QueryableIndex> indexes,
      @Nullable DimensionsSpec dimensionsSpec
  )
  {
    return getMergedDimensions(toIndexableAdapters(indexes), dimensionsSpec);
  }

  static List<IndexableAdapter> toIndexableAdapters(List<QueryableIndex> indexes)
  {
    return indexes.stream().map(QueryableIndexIndexableAdapter::new).collect(Collectors.toList());
  }

  static List<String> getMergedDimensions(
      List<IndexableAdapter> indexes,
      @Nullable DimensionsSpec dimensionsSpec
  )
  {
    if (indexes.size() == 0) {
      return ImmutableList.of();
    }
    List<String> commonDimOrder = getLongestSharedDimOrder(indexes, dimensionsSpec);
    if (commonDimOrder == null) {
      log.warn("Indexes have incompatible dimension orders and there is no valid dimension ordering"
               + " in the ingestionSpec, using lexicographic order.");
      return getLexicographicMergedDimensions(indexes);
    } else {
      return commonDimOrder;
    }
  }

  @Nullable
  static List<String> getLongestSharedDimOrder(
      List<IndexableAdapter> indexes,
      @Nullable DimensionsSpec dimensionsSpec
  )
  {
    int maxSize = 0;
    Iterable<String> orderingCandidate = null;
    for (IndexableAdapter index : indexes) {
      int iterSize = index.getDimensionNames().size();
      if (iterSize > maxSize) {
        maxSize = iterSize;
        orderingCandidate = index.getDimensionNames();
      }
    }

    if (orderingCandidate == null) {
      return null;
    }

    if (isDimensionOrderingValid(indexes, orderingCandidate)) {
      return ImmutableList.copyOf(orderingCandidate);
    } else {
      log.info("Indexes have incompatible dimension orders, try falling back on dimension ordering from ingestionSpec");
      // Check if there is a valid dimension ordering in the ingestionSpec to fall back on
      if (dimensionsSpec == null || CollectionUtils.isNullOrEmpty(dimensionsSpec.getDimensionNames())) {
        log.info("Cannot fall back on dimension ordering from ingestionSpec as it does not exist");
        return null;
      }
      List<String> candidate = new ArrayList<>(dimensionsSpec.getDimensionNames());
      // Remove all dimensions that does not exist within the indexes from the candidate
      Set<String> allValidDimensions = indexes.stream()
                                         .flatMap(indexableAdapter -> indexableAdapter.getDimensionNames().stream())
                                         .collect(Collectors.toSet());
      candidate.retainAll(allValidDimensions);
      // Sanity check that there is no extra/missing columns
      if (candidate.size() != allValidDimensions.size()) {
        log.error("Dimension mismatched between ingestionSpec and indexes. ingestionSpec[%s] indexes[%s]",
                  candidate,
                  allValidDimensions);
        return null;
      }

      // Sanity check that all indexes dimension ordering is the same as the ordering in candidate
      if (!isDimensionOrderingValid(indexes, candidate)) {
        log.error("Dimension from ingestionSpec has invalid ordering");
        return null;
      }
      log.info("Dimension ordering from ingestionSpec is valid. Fall back on dimension ordering [%s]", candidate);
      return candidate;
    }
  }

  static boolean isDimensionOrderingValid(List<IndexableAdapter> indexes, Iterable<String> orderingCandidate)
  {
    for (IndexableAdapter index : indexes) {
      Iterator<String> candidateIter = orderingCandidate.iterator();
      for (String matchDim : index.getDimensionNames()) {
        boolean matched = false;
        while (candidateIter.hasNext()) {
          String nextDim = candidateIter.next();
          if (matchDim.equals(nextDim)) {
            matched = true;
            break;
          }
        }
        if (!matched) {
          return false;
        }
      }
    }
    return true;
  }

  static List<String> getLexicographicMergedDimensions(List<IndexableAdapter> indexes)
  {
    return mergeIndexed(
        Lists.transform(
            indexes,
            new Function<IndexableAdapter, Iterable<String>>()
            {
              @Override
              public Iterable<String> apply(@Nullable IndexableAdapter input)
              {
                return input.getDimensionNames();
              }
            }
        )
    );
  }

  static <T extends Comparable<? super T>> ArrayList<T> mergeIndexed(List<Iterable<T>> indexedLists)
  {
    Set<T> retVal = new TreeSet<>(Comparators.naturalNullsFirst());

    for (Iterable<T> indexedList : indexedLists) {
      for (T val : indexedList) {
        retVal.add(val);
      }
    }

    return Lists.newArrayList(retVal);
  }

  File persist(
      IncrementalIndex index,
      File outDir,
      IndexSpec indexSpec,
      @Nullable SegmentWriteOutMediumFactory segmentWriteOutMediumFactory
  ) throws IOException;

  /**
   * This is *not* thread-safe and havok will ensue if this is called and writes are still occurring
   * on the IncrementalIndex object.
   *
   * @param index        the IncrementalIndex to persist
   * @param dataInterval the Interval that the data represents
   * @param outDir       the directory to persist the data to
   *
   * @return the index output directory
   *
   * @throws IOException if an IO error occurs persisting the index
   */
  File persist(
      IncrementalIndex index,
      Interval dataInterval,
      File outDir,
      IndexSpec indexSpec,
      @Nullable SegmentWriteOutMediumFactory segmentWriteOutMediumFactory
  ) throws IOException;

  File persist(
      IncrementalIndex index,
      Interval dataInterval,
      File outDir,
      IndexSpec indexSpec,
      ProgressIndicator progress,
      @Nullable SegmentWriteOutMediumFactory segmentWriteOutMediumFactory
  ) throws IOException;

  File mergeQueryableIndex(
      List<QueryableIndex> indexes,
      boolean rollup,
      AggregatorFactory[] metricAggs,
      File outDir,
      IndexSpec indexSpec,
      @Nullable SegmentWriteOutMediumFactory segmentWriteOutMediumFactory,
      int maxColumnsToMerge
  ) throws IOException;

  File mergeQueryableIndex(
      List<QueryableIndex> indexes,
      boolean rollup,
      AggregatorFactory[] metricAggs,
      @Nullable DimensionsSpec dimensionsSpec,
      File outDir,
      IndexSpec indexSpec,
      @Nullable SegmentWriteOutMediumFactory segmentWriteOutMediumFactory,
      int maxColumnsToMerge
  ) throws IOException;

  File mergeQueryableIndex(
      List<QueryableIndex> indexes,
      boolean rollup,
      AggregatorFactory[] metricAggs,
      @Nullable DimensionsSpec dimensionsSpec,
      File outDir,
      IndexSpec indexSpec,
      ProgressIndicator progress,
      @Nullable SegmentWriteOutMediumFactory segmentWriteOutMediumFactory,
      int maxColumnsToMerge
  ) throws IOException;

  @VisibleForTesting
  File merge(
      List<IndexableAdapter> indexes,
      boolean rollup,
      AggregatorFactory[] metricAggs,
      File outDir,
      IndexSpec indexSpec,
      int maxColumnsToMerge
  ) throws IOException;

  // Faster than IndexMaker
  File convert(File inDir, File outDir, IndexSpec indexSpec) throws IOException;

  File append(
      List<IndexableAdapter> indexes,
      AggregatorFactory[] aggregators,
      File outDir,
      IndexSpec indexSpec,
      @Nullable SegmentWriteOutMediumFactory segmentWriteOutMediumFactory
  ) throws IOException;

  interface IndexSeeker
  {
    int NOT_EXIST = -1;
    int NOT_INIT = -1;

    int seek(int dictId);
  }

  /**
   * Get old dictId from new dictId, and only support access in order
   */
  class IndexSeekerWithConversion implements IndexSeeker
  {
    private final IntBuffer dimConversions;
    private int currIndex;
    private int currVal;
    private int lastVal;

    IndexSeekerWithConversion(IntBuffer dimConversions)
    {
      this.dimConversions = dimConversions;
      this.currIndex = 0;
      this.currVal = IndexSeeker.NOT_INIT;
      this.lastVal = IndexSeeker.NOT_INIT;
    }

    @Override
    public int seek(int dictId)
    {
      if (dimConversions == null) {
        return IndexSeeker.NOT_EXIST;
      }
      if (lastVal != IndexSeeker.NOT_INIT) {
        if (dictId <= lastVal) {
          throw new ISE(
              "Value dictId[%d] is less than the last value dictId[%d] I have, cannot be.",
              dictId, lastVal
          );
        }
        return IndexSeeker.NOT_EXIST;
      }
      if (currVal == IndexSeeker.NOT_INIT) {
        currVal = dimConversions.get();
      }
      if (currVal == dictId) {
        int ret = currIndex;
        ++currIndex;
        if (dimConversions.hasRemaining()) {
          currVal = dimConversions.get();
        } else {
          lastVal = dictId;
        }
        return ret;
      } else if (currVal < dictId) {
        throw new ISE(
            "Skipped currValue dictId[%d], currIndex[%d]; incoming value dictId[%d]",
            currVal, currIndex, dictId
        );
      } else {
        return IndexSeeker.NOT_EXIST;
      }
    }
  }

  /**
   * This method applies {@link DimensionMerger#convertSortedSegmentRowValuesToMergedRowValues(int, ColumnValueSelector)} to
   * all dimension column selectors of the given sourceRowIterator, using the given index number.
   */
  static TransformableRowIterator toMergedIndexRowIterator(
      TransformableRowIterator sourceRowIterator,
      int indexNumber,
      final List<DimensionMergerV9> mergers
  )
  {
    RowPointer sourceRowPointer = sourceRowIterator.getPointer();
    TimeAndDimsPointer markedSourceRowPointer = sourceRowIterator.getMarkedPointer();
    boolean anySelectorChanged = false;
    ColumnValueSelector[] convertedDimensionSelectors = new ColumnValueSelector[mergers.size()];
    ColumnValueSelector[] convertedMarkedDimensionSelectors = new ColumnValueSelector[mergers.size()];
    for (int i = 0; i < mergers.size(); i++) {
      ColumnValueSelector sourceDimensionSelector = sourceRowPointer.getDimensionSelector(i);
      ColumnValueSelector convertedDimensionSelector =
          mergers.get(i).convertSortedSegmentRowValuesToMergedRowValues(indexNumber, sourceDimensionSelector);
      convertedDimensionSelectors[i] = convertedDimensionSelector;
      // convertedDimensionSelector could be just the same object as sourceDimensionSelector, it means that this
      // type of column doesn't have any kind of special per-index encoding that needs to be converted to the "global"
      // encoding. E. g. it's always true for subclasses of NumericDimensionMergerV9.
      //noinspection ObjectEquality
      anySelectorChanged |= convertedDimensionSelector != sourceDimensionSelector;

      convertedMarkedDimensionSelectors[i] = mergers.get(i).convertSortedSegmentRowValuesToMergedRowValues(
          indexNumber,
          markedSourceRowPointer.getDimensionSelector(i)
      );
    }
    // If none dimensions are actually converted, don't need to transform the sourceRowIterator, adding extra
    // indirection layer. It could be just returned back from this method.
    if (!anySelectorChanged) {
      return sourceRowIterator;
    }
    return makeRowIteratorWithConvertedDimensionColumns(
        sourceRowIterator,
        convertedDimensionSelectors,
        convertedMarkedDimensionSelectors
    );
  }

  static TransformableRowIterator makeRowIteratorWithConvertedDimensionColumns(
      TransformableRowIterator sourceRowIterator,
      ColumnValueSelector[] convertedDimensionSelectors,
      ColumnValueSelector[] convertedMarkedDimensionSelectors
  )
  {
    RowPointer convertedRowPointer = sourceRowIterator.getPointer().withDimensionSelectors(convertedDimensionSelectors);
    TimeAndDimsPointer convertedMarkedRowPointer =
        sourceRowIterator.getMarkedPointer().withDimensionSelectors(convertedMarkedDimensionSelectors);
    return new ForwardingRowIterator(sourceRowIterator)
    {
      @Override
      public RowPointer getPointer()
      {
        return convertedRowPointer;
      }

      @Override
      public TimeAndDimsPointer getMarkedPointer()
      {
        return convertedMarkedRowPointer;
      }
    };
  }

  // TODO: maybe i can reuse this
  class DictionaryMergeIterator implements CloseableIterator<String>
  {
    /**
     * Don't replace this lambda with {@link Comparator#comparing} or {@link Comparators#naturalNullsFirst()} because
     * this comparator is hot, so we want to avoid extra indirection layers.
     */
    static final Comparator<Pair<Integer, PeekingIterator<String>>> NULLS_FIRST_PEEKING_COMPARATOR = (lhs, rhs) -> {
      String left = lhs.rhs.peek();
      String right = rhs.rhs.peek();
      if (left == null) {
        //noinspection VariableNotUsedInsideIf
        return right == null ? 0 : -1;
      } else if (right == null) {
        return 1;
      } else {
        return left.compareTo(right);
      }
    };

    protected final IntBuffer[] conversions;
    protected final List<Pair<ByteBuffer, Integer>> directBufferAllocations = new ArrayList<>();
    protected final PriorityQueue<Pair<Integer, PeekingIterator<String>>> pQueue;

    protected int counter;

    DictionaryMergeIterator(Indexed<String>[] dimValueLookups, boolean useDirect)
    {
      pQueue = new PriorityQueue<>(dimValueLookups.length, NULLS_FIRST_PEEKING_COMPARATOR);
      conversions = new IntBuffer[dimValueLookups.length];

      long mergeBufferTotalSize = 0;
      for (int i = 0; i < conversions.length; i++) {
        if (dimValueLookups[i] == null) {
          continue;
        }
        Indexed<String> indexed = dimValueLookups[i];
        if (useDirect) {
          int allocationSize = indexed.size() * Integer.BYTES;
          log.trace("Allocating dictionary merging direct buffer with size[%,d]", allocationSize);
          mergeBufferTotalSize += allocationSize;
          final ByteBuffer conversionDirectBuffer = ByteBuffer.allocateDirect(allocationSize);
          conversions[i] = conversionDirectBuffer.asIntBuffer();
          directBufferAllocations.add(new Pair<>(conversionDirectBuffer, allocationSize));
        } else {
          conversions[i] = IntBuffer.allocate(indexed.size());
          mergeBufferTotalSize += indexed.size();
        }

        final PeekingIterator<String> iter = Iterators.peekingIterator(
            Iterators.transform(
                indexed.iterator(),
                NullHandling::nullToEmptyIfNeeded
            )
        );
        if (iter.hasNext()) {
          pQueue.add(Pair.of(i, iter));
        }
      }
      log.debug("Allocated [%,d] bytes of dictionary merging direct buffers", mergeBufferTotalSize);
    }

    @Override
    public boolean hasNext()
    {
      return !pQueue.isEmpty();
    }

    @Override
    public String next()
    {
      Pair<Integer, PeekingIterator<String>> smallest = pQueue.remove();
      if (smallest == null) {
        throw new NoSuchElementException();
      }
      final String value = writeTranslate(smallest, counter);

      while (!pQueue.isEmpty() && Objects.equals(value, pQueue.peek().rhs.peek())) {
        writeTranslate(pQueue.remove(), counter);
      }
      counter++;

      return value;
    }

    boolean needConversion(int index)
    {
      IntBuffer readOnly = conversions[index].asReadOnlyBuffer();
      readOnly.rewind();
      int i = 0;
      while (readOnly.hasRemaining()) {
        if (i != readOnly.get()) {
          return true;
        }
        i++;
      }
      return false;
    }

    private String writeTranslate(Pair<Integer, PeekingIterator<String>> smallest, int counter)
    {
      final int index = smallest.lhs;
      final String value = smallest.rhs.next();

      conversions[index].put(counter);
      if (smallest.rhs.hasNext()) {
        pQueue.add(smallest);
      }
      return value;
    }

    @Override
    public void remove()
    {
      throw new UnsupportedOperationException("remove");
    }
    
    @Override
    public void close()
    {
      long mergeBufferTotalSize = 0;
      for (Pair<ByteBuffer, Integer> bufferAllocation : directBufferAllocations) {
        log.trace("Freeing dictionary merging direct buffer with size[%,d]", bufferAllocation.rhs);
        mergeBufferTotalSize += bufferAllocation.rhs;
        ByteBufferUtils.free(bufferAllocation.lhs);
      }
      log.debug("Freed [%,d] bytes of dictionary merging direct buffers", mergeBufferTotalSize);
    }
  }
}
