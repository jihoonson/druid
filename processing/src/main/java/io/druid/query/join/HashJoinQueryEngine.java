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

package io.druid.query.join;

import com.google.common.base.Function;
import com.google.common.base.Predicate;
import com.google.common.collect.Iterables;
import io.druid.collections.StupidPool;
import io.druid.common.guava.SettableSupplier;
import io.druid.data.input.Row;
import io.druid.java.util.common.guava.Accumulator;
import io.druid.java.util.common.guava.BaseSequence;
import io.druid.java.util.common.guava.BaseSequence.IteratorMaker;
import io.druid.java.util.common.guava.Sequence;
import io.druid.java.util.common.guava.Sequences;
import io.druid.query.dimension.ColumnSelectorStrategyFactory;
import io.druid.query.filter.ValueMatcher;
import io.druid.query.groupby.RowBasedColumnSelectorFactory;
import io.druid.segment.ColumnValueSelector;
import io.druid.segment.Cursor;
import io.druid.segment.DimensionSelector;
import io.druid.segment.Segment;
import io.druid.segment.StorageAdapter;
import io.druid.segment.column.ColumnCapabilities;
import io.druid.segment.data.IndexedInts;
import io.druid.segment.filter.Filters;
import org.joda.time.DateTime;

import javax.annotation.Nullable;
import java.io.Closeable;
import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

public class HashJoinQueryEngine implements JoinQueryEngine
{

  public static class JoinColumnSelectorStrategyFactory
      implements ColumnSelectorStrategyFactory<JoinColumnSelectorStrategy>
  {

    @Override
    public JoinColumnSelectorStrategy makeColumnSelectorStrategy(
        ColumnCapabilities capabilities, ColumnValueSelector selector
    )
    {
      // TODO
      return null;
    }
  }

  private static JoinHashTable buildHashTableFromSegment(final JoinQuery query, final Segment segment)
  {
    final JoinHashTable hashTable = new JoinHashTable();
    final StorageAdapter adapter = segment.asStorageAdapter();
    final Sequence<Cursor> cursors = adapter.makeCursors(
        Filters.toFilter(query.getFilter()), // TODO: push only pushable filters for this table
        query.getIntervals().get(0),
        query.getVirtualColumns(),
        query.getGranularity(),
        false
    );

    cursors.accumulate(
        hashTable,
        new Accumulator<JoinHashTable, Cursor>()
        {
          @Override
          public JoinHashTable accumulate(JoinHashTable accumulated, Cursor in)
          {
            // Retrieve column values from cursor
            // add to join hash table
            return accumulated;
          }
        }
    );

    return hashTable;
  }

  private static JoinHashTable buildHashTableFromSequence(final JoinQuery query, final Sequence<Row> sequence)
  {
    final JoinHashTable hashTable = new JoinHashTable();
    sequence.accumulate(
        hashTable,
        new Accumulator<JoinHashTable, Row>()
        {
          @Override
          public JoinHashTable accumulate(JoinHashTable accumulated, Row in)
          {
            // Retrieve column values from cursor
            // add to join hash table
            return accumulated;
          }
        }
    );

    return hashTable;
  }

  private static Sequence<Row> join(final JoinQuery query, Segment buildSide, Segment probeSide)
  {
    final JoinHashTable hashTable = buildHashTableFromSegment(query, buildSide);
    final StorageAdapter probeSideAdapter = probeSide.asStorageAdapter();
    final Sequence<Cursor> probeSideCursors = probeSideAdapter.makeCursors(
        Filters.toFilter(query.getFilter()),
        query.getIntervals().get(0),
        query.getVirtualColumns(),
        query.getGranularity(),
        false
    );

    return Sequences.concat(
        Sequences.map(
            probeSideCursors,
            new Function<Cursor, Sequence<Row>>()
            {
              @Override
              public Sequence<Row> apply(final Cursor cursor)
              {
                return new BaseSequence<>(
                    new IteratorMaker<Row, JoinResultIterator>()
                    {
                      @Override
                      public JoinResultIterator make()
                      {
                        // probe and collect result
                        return new JoinResultIterator(
                            query,
                            cursor,
                            hashTable,
                            null // TODO: fudge timestamp
                        );
                      }

                      @Override
                      public void cleanup(JoinResultIterator iterFromMake)
                      {
                        iterFromMake.close();
                      }
                    }
                );
              }
            }
        )
    );
  }

  private static Sequence<Row> join(final JoinQuery query, final Segment)
  {

  }

//  private static Sequence<Row> joinCursors(
//      final JoinQuery query,
//      final Sequence<Cursor> buildSideCursors,
//      final Sequence<Cursor> probeSideCursors
//  )
//  {
//
//  }

  private static Sequence<Row> joinRows(
      final JoinQuery query,
      final Sequence<Row> buildSideRows,
      final Sequence<Row> probeSideRows
  )
  {
    final JoinHashTable hashTable = buildHashTableFromSequence(query, buildSideRows);
    final SettableSupplier<Row> supplier = new SettableSupplier<>();
    final ValueMatcher matcher = query.getFilter().toFilter().makeMatcher(RowBasedColumnSelectorFactory.create(supplier, null));

    return Sequences.filter(
        Sequences.map(
            probeSideRows,
            new Function<Row, Row>()
            {
              @Nullable
              @Override
              public Row apply(final Row row)
              {
                return null;
              }
            }
        ),
        new Predicate<Row>()
        {
          @Override
          public boolean apply(@Nullable Row input)
          {
            if (input == null) {
              return false;
            } else {
              supplier.set(input);
              return matcher.matches();
            }
          }
        }
    );
  }

  public Sequence<Row> process(
      final JoinQuery query,
      final Segment segment,
      final List<Segment> broadcastSegments,
      final StupidPool<ByteBuffer> pool
  )
  {
    // TODO: runtime join ordering
    // - cached join results must appear first
    // - remaining segments are sorted by their size
    // - using priority queue might be a solution as in MergeSequence
    // - should consider cache hit of joined tables

    final List<Segment> segments;
    Sequence<Row> sequence = join(query, segments.get(0), segments.get(1));



//    final StorageAdapter storageAdapter = segment.asStorageAdapter();
//
//    if (storageAdapter == null) {
//      throw new ISE(
//          "Null storage adapter found. Probably trying to issue a query against a segment being memory unmapped."
//      );
//    }
//
//    // TODO: check this
//    final List<Interval> intervals = query.getQuerySegmentSpec().getIntervals();
//    if (intervals.size() != 1) {
//      throw new IAE("Should only have one interval, got[%s]", intervals);
//    }
//
//    final Sequence<Cursor> cursors = storageAdapter.makeCursors(
//        Filters.toFilter(query.getFilter()),
//        intervals.get(0),
//        query.getVirtualColumns(),
//        query.getGranularity(),
//        false
//    );
//
//    // probe in sequence
//    return Sequences.concat(
//        Sequences.map(
//            cursors,
//            new Function<Cursor, Sequence<Row>>()
//            {
//              @Override
//              public Sequence<Row> apply(Cursor input)
//              {
//                return new BaseSequence<>(
//                    new IteratorMaker<Row, JoinResultIterator>()
//                    {
//                      @Override
//                      public JoinResultIterator make()
//                      {
//                        return null;
//                      }
//
//                      @Override
//                      public void cleanup(JoinResultIterator iterFromMake)
//                      {
//                        iterFromMake.close();
//                      }
//                    }
//                );
//              }
//            }
//        )
//    );
  }

  private static class JoinResultIterator implements Iterator<Row>, Closeable
  {
    private final JoinQuery query; // TODO: to filter?
    private final Cursor cursor;
    private final JoinHashTable hashTable;
    private final DateTime timestamp;
    private Iterator<Row> matchedRightRowsIterator;
    private Iterator<Row> nextMatchedRightRowsIterator;
    private List<Object> matchLeftSide;

    public JoinResultIterator(JoinQuery query, Cursor cursor, JoinHashTable hashTable, DateTime timestamp)
    {
      this.query = query;
      this.cursor = cursor;
      this.hashTable = hashTable;
      this.timestamp = timestamp;
      this.nextMatchedRightRowsIterator = findNextMatch();
    }

    @Override
    public boolean hasNext()
    {
      return hasCurrentMatch() || hasNextMatch(); // || !cursor.isDone()
    }

    private boolean hasCurrentMatch()
    {
      return matchedRightRowsIterator != null && matchedRightRowsIterator.hasNext();
    }

    private boolean hasNextMatch()
    {
      return nextMatchedRightRowsIterator != null && nextMatchedRightRowsIterator.hasNext();
    }

    private Iterator<Row> findNextMatch()
    {
      final AnnotatedJoinSpec joinSpec = query.getAnnotatedJoinSpec();
      final JoinPredicate joinPredicate = joinSpec.getPredicate();

      // FIXME: assume equal predicate
      final EqualPredicate equalPredicate = (EqualPredicate) joinPredicate;
      // FIXME: assume dim extract predicate
      final DimExtractPredicate left = (DimExtractPredicate) equalPredicate.getLeft();
      final DimExtractPredicate right = (DimExtractPredicate) equalPredicate.getRight();

      final DimensionSelector dimensionSelector = cursor.makeDimensionSelector(left.getDimension());
      final IndexedInts vals = dimensionSelector.getRow();
      for (int i = 0; i < vals.size(); i++) {
        dimensionSelector.lookupName(vals.get(i));
      }

      joinSpec.getLeftInputColumnNames()

      cursor.advance();
      return null;
    }

    private Iterator<Row> filter(List<Row> matched)
    {
//      query.getFilter().toFilter().makeMatcher(RowBasedColumnSelectorFactory.create());
      Iterables.filter(matched, new Predicate<Row>()
      {
        @Override
        public boolean apply(Row input)
        {

          return false;
        }
      });
      return null;
    }

    @Override
    public Row next()
    {
      if (hasCurrentMatch()) {
        return buildResultRow(matchedRightRowsIterator.next());
      } else if (hasNextMatch()) {
        matchedRightRowsIterator = nextMatchedRightRowsIterator;
        nextMatchedRightRowsIterator = findNextMatch();
        return buildResultRow(matchedRightRowsIterator.next());
      } else {
        throw new NoSuchElementException();
      }

//      if (hasNextMatch()) {
//        Map<String, Object> theMap = Maps.newLinkedHashMap();
//        // build map
//
//        return new MapBasedRow(timestamp, theMap);
//      } else {
//        while (!cursor.isDone()) {
//          Object key;
//          cursor.advance();
//          List<Row> matched = hashTable.get(key);
//          if (matched != null) {
//            matchedRightRowsIterator = filter(matched);
//          }
//        }
//
//        if (hasNextMatch()) {
//          Map<String, Object> theMap = Maps.newLinkedHashMap();
//          // build map
//
//          return new MapBasedRow(timestamp, theMap);
//        } else {
//          return null;
//        }
//      }
    }

    private Row buildResultRow(Row right)
    {
      // build from matchLeftSide and right(Row type)
      return null;
    }

    @Override
    public void remove()
    {
      throw new UnsupportedOperationException();
    }

    @Override
    public void close()
    {

    }
  }
}
