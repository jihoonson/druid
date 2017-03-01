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
import io.druid.data.input.Row;
import io.druid.java.util.common.IAE;
import io.druid.java.util.common.ISE;
import io.druid.java.util.common.guava.BaseSequence;
import io.druid.java.util.common.guava.BaseSequence.IteratorMaker;
import io.druid.java.util.common.guava.Sequence;
import io.druid.java.util.common.guava.Sequences;
import io.druid.query.dimension.ColumnSelectorStrategyFactory;
import io.druid.segment.ColumnValueSelector;
import io.druid.segment.Cursor;
import io.druid.segment.Segment;
import io.druid.segment.StorageAdapter;
import io.druid.segment.column.ColumnCapabilities;
import io.druid.segment.filter.Filters;
import org.joda.time.DateTime;
import org.joda.time.Interval;

import java.io.Closeable;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;

public class JoinQueryEngine
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

  public Sequence<Row> process(final JoinQuery query, final Map<Object, List<Row>> hashTable, final Segment segment)
  {
    final StorageAdapter storageAdapter = segment.asStorageAdapter();

    if (storageAdapter == null) {
      throw new ISE(
          "Null storage adapter found. Probably trying to issue a query against a segment being memory unmapped."
      );
    }

    // TODO: check this
    final List<Interval> intervals = query.getQuerySegmentSpec().getIntervals();
    if (intervals.size() != 1) {
      throw new IAE("Should only have one interval, got[%s]", intervals);
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
              public Sequence<Row> apply(Cursor input)
              {
                return new BaseSequence<>(
                    new IteratorMaker<Row, JoinEngineIterator>()
                    {
                      @Override
                      public JoinEngineIterator make()
                      {
                        return null;
                      }

                      @Override
                      public void cleanup(JoinEngineIterator iterFromMake)
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

  private static class JoinEngineIterator implements Iterator<Row>, Closeable
  {
    private final JoinQuery query;
    private final Cursor cursor;
    private final Map<Object, List<Row>> hashTable;
    private final DateTime timestamp;
    private Iterator<Row> matchedRightRowsIterator;
    private Iterator<Row> nextMatchedRightRowsIterator;
    private List<Object> matchLeftSide;

    public JoinEngineIterator(JoinQuery query, Cursor cursor, Map<Object, List<Row>> hashTable, DateTime timestamp)
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
