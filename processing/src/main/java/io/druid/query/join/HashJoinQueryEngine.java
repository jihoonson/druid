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
import com.google.common.base.Supplier;
import com.google.common.collect.Iterables;
import io.druid.collections.StupidPool;
import io.druid.common.guava.SettableSupplier;
import io.druid.data.input.Row;
import io.druid.java.util.common.IAE;
import io.druid.java.util.common.ISE;
import io.druid.java.util.common.guava.Accumulator;
import io.druid.java.util.common.guava.BaseSequence;
import io.druid.java.util.common.guava.BaseSequence.IteratorMaker;
import io.druid.java.util.common.guava.Sequence;
import io.druid.java.util.common.guava.Sequences;
import io.druid.query.DataSourceWithSegmentSpec;
import io.druid.query.dimension.ColumnSelectorStrategyFactory;
import io.druid.query.dimension.DimensionSpec;
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
import org.joda.time.Interval;

import javax.annotation.Nullable;
import java.io.Closeable;
import java.nio.ByteBuffer;
import java.util.ArrayList;
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

//  private static JoinHashTable buildHashTableFromSegment(final JoinQuery query, final Segment segment)
//  {
//    final JoinHashTable hashTable = new JoinHashTable();
//    final StorageAdapter adapter = segment.asStorageAdapter();
//    final Sequence<Cursor> cursors = adapter.makeCursors(
//        Filters.toFilter(query.getFilter()), // TODO: push only pushable filters for this table
//        query.getIntervals().get(0),
//        query.getVirtualColumns(),
//        query.getGranularity(),
//        false
//    );
//
//    cursors.accumulate(
//        hashTable,
//        new Accumulator<JoinHashTable, Cursor>()
//        {
//          @Override
//          public JoinHashTable accumulate(JoinHashTable accumulated, Cursor in)
//          {
//            // Retrieve column values from cursor
//            // add to join hash table
//            return accumulated;
//          }
//        }
//    );
//
//    return hashTable;
//  }

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

//  private static Sequence<Row> join(final JoinQuery query, Segment buildSide, Segment probeSide)
//  {
//    final JoinHashTable hashTable = buildHashTableFromSegment(query, buildSide);
//    final StorageAdapter probeSideAdapter = probeSide.asStorageAdapter();
//    final Sequence<Cursor> probeSideCursors = probeSideAdapter.makeCursors(
//        Filters.toFilter(query.getFilter()),
//        query.getIntervals().get(0),
//        query.getVirtualColumns(),
//        query.getGranularity(),
//        false
//    );
//
//    return Sequences.concat(
//        Sequences.map(
//            probeSideCursors,
//            new Function<Cursor, Sequence<Row>>()
//            {
//              @Override
//              public Sequence<Row> apply(final Cursor cursor)
//              {
//                return new BaseSequence<>(
//                    new IteratorMaker<Row, JoinResultIterator>()
//                    {
//                      @Override
//                      public JoinResultIterator make()
//                      {
//                        // probe and collect result
//                        return new JoinResultIterator(
//                            query,
//                            cursor,
//                            hashTable,
//                            null // TODO: fudge timestamp
//                        );
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
//  }

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

  @Override
  public Sequence<Row> process(
      final JoinQuery query,
      final Segment segment,
      final Supplier<Sequence<Row>> supplier,
      final String nonBroadcastedSourceName,
      final StupidPool<ByteBuffer> pool
  )
  {
    // TODO: runtime join ordering??
    // - cached join results must appear first
    // - remaining segments are sorted by their size
    // - using priority queue might be a solution as in MergeSequence
    // - should consider cache hit of joined tables

//    final List<Segment> segments; // TODO: priority queue
//
//    while (segments.size() > 1) {
//      final JoinHashTable hashTable = buildHashTableFromSegment(query, segments.get(1));
//      final StorageAdapter storageAdapter = segments.get(0).asStorageAdapter();
//
//      if (storageAdapter == null) {
//        throw new ISE(
//            "Null storage adapter found. Probably trying to issue a query against a segment being memory unmapped."
//        );
//      }
//
//      // TODO: check this
//      final List<Interval> intervals = query.getQuerySegmentSpec().getIntervals();
//      if (intervals.size() != 1) {
//        throw new IAE("Should only have one interval, got[%s]", intervals);
//      }
//
//      final Sequence<Cursor> cursors = storageAdapter.makeCursors(
//          Filters.toFilter(query.getFilter()),
//          intervals.get(0),
//          query.getVirtualColumns(),
//          query.getGranularity(),
//          false
//      );
//
//      // probe in sequence
//      segments.add(Sequences.concat(
//          Sequences.map(
//              cursors,
//              new Function<Cursor, Sequence<Row>>()
//              {
//                @Override
//                public Sequence<Row> apply(final Cursor input)
//                {
//                  return new BaseSequence<>(
//                      new IteratorMaker<Row, JoinResultIterator>()
//                      {
//                        @Override
//                        public JoinResultIterator make()
//                        {
//                          return new JoinResultIterator(query, input, hashTable, null);
//                        }
//
//                        @Override
//                        public void cleanup(JoinResultIterator iterFromMake)
//                        {
//                          iterFromMake.close();
//                        }
//                      }
//                  );
//                }
//              }
//          )
//      ));
//    }
//
//    return segments.get(0);

    // TODO: join segment and supplier
    final JoinHashTable hashTable = buildHashTableFromSequence(query, supplier.get());
    final StorageAdapter storageAdapter = segment.asStorageAdapter();

    if (storageAdapter == null) {
      throw new ISE(
          "Null storage adapter found. Probably trying to issue a query against a segment being memory unmapped."
      );
    }

    // TODO: check this
    List<Interval> intervals = null;
    for (DataSourceWithSegmentSpec eachSource : query.getDataSources()) {
      if (Iterables.getOnlyElement(eachSource.getDataSource().getNames()).equals(nonBroadcastedSourceName)) {
        intervals = eachSource.getQuerySegmentSpec().getIntervals();
        if (intervals.size() != 1) {
          throw new IAE("Should only have one interval, got[%s]", intervals);
        }
      }
    }
    if (intervals == null) {
      // TODO
      throw new IAE("TODO");
    }

    final Sequence<Cursor> cursors = storageAdapter.makeCursors(
        Filters.toFilter(query.getFilter()),
        intervals.get(0),
        query.getVirtualColumns(),
        query.getGranularity(),
        false
    );

    // probe in sequence
    return Sequences.concat(
        Sequences.map(
            cursors,
            new Function<Cursor, Sequence<Row>>()
            {
              @Override
              public Sequence<Row> apply(final Cursor input)
              {
                return new BaseSequence<>(
                    new IteratorMaker<Row, JoinResultIterator>()
                    {
                      @Override
                      public JoinResultIterator make()
                      {
                        return new JoinResultIterator(query, input, hashTable, null);
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

  private static class JoinResultIterator implements Iterator<Row>, Closeable
  {
    private final Iterator<List<List<Object>>> baseIterator;
    private List<List<Object>> next;

    public JoinResultIterator(JoinQuery query, Cursor cursor, JoinHashTable hashTable, DateTime timestamp)
    {
      final HashJoin join = new HashJoin(query.getAnnotatedJoinSpec(), hashTable);

      final AnnotatedJoinSpec joinSpec = query.getAnnotatedJoinSpec();

      // TODO: the below code stores all join results in memory, and gets an iterator for the result.
      // Streaming join is also possible. Which approach is better?
      while (!cursor.isDone()) {
        List<List<Object>> keys = new ArrayList<>();

        for (DimensionSpec keySpec : joinSpec.getLeftKeyIndexes()) {
          final DimensionSelector dimensionSelector = cursor.makeDimensionSelector(keySpec);
          final IndexedInts vals = dimensionSelector.getRow();
          List<Object> key = new ArrayList<>(vals.size());
          for (int i = 0; i < vals.size(); i++) {
            key.add(dimensionSelector.lookupName(vals.get(i)));
          }
          keys.add(key);
        }

        List<List<Object>> allValues = new ArrayList<>();

        for (DimensionSpec payloadSpec : joinSpec.getLeftDimensions()) {
          final DimensionSelector dimensionSelector = cursor.makeDimensionSelector(payloadSpec);
          final IndexedInts vals = dimensionSelector.getRow();
          List<Object> outVal = new ArrayList<>(vals.size());
          for (int i = 0; i < vals.size(); i++) {
            outVal.add(dimensionSelector.lookupName(vals.get(i)));
          }
          allValues.add(outVal);
        }

        join.add(keys, allValues);

        cursor.advance();
      }

      baseIterator = join.getIterator();
      next = findNextMatch();
    }

    @Override
    public boolean hasNext()
    {
      return next != null;
    }

    private List<List<Object>> findNextMatch()
    {
      return null;
    }

    private List<List<Object>> filter(List<Row> matched)
    {
//      query.getFilter().toFilter().makeMatcher(RowBasedColumnSelectorFactory.create());
      return null;
    }

    @Override
    public Row next()
    {
      if (next == null) {
        throw new NoSuchElementException();
      }

      final List<List<Object>> ret = next;
      next = findNextMatch();
      return buildResultRow(ret);
    }

    private Row buildResultRow(List<List<Object>> row)
    {
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
