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

package org.apache.druid.indexing.firehose;

import com.google.common.base.Function;
import com.google.common.collect.Maps;
import org.apache.druid.data.input.InputEntity;
import org.apache.druid.data.input.InputEntity.CleanableFile;
import org.apache.druid.data.input.InputEntityReader;
import org.apache.druid.data.input.InputEntitySampler;
import org.apache.druid.data.input.InputRow;
import org.apache.druid.data.input.InputRowListPlusJson;
import org.apache.druid.data.input.MapBasedInputRow;
import org.apache.druid.data.input.impl.TimestampSpec;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.java.util.common.guava.Sequence;
import org.apache.druid.java.util.common.guava.Sequences;
import org.apache.druid.java.util.common.guava.Yielder;
import org.apache.druid.java.util.common.guava.Yielders;
import org.apache.druid.java.util.common.parsers.CloseableIterator;
import org.apache.druid.query.dimension.DefaultDimensionSpec;
import org.apache.druid.query.filter.DimFilter;
import org.apache.druid.segment.BaseLongColumnValueSelector;
import org.apache.druid.segment.BaseObjectColumnValueSelector;
import org.apache.druid.segment.Cursor;
import org.apache.druid.segment.DimensionSelector;
import org.apache.druid.segment.IndexIO;
import org.apache.druid.segment.QueryableIndexStorageAdapter;
import org.apache.druid.segment.VirtualColumns;
import org.apache.druid.segment.column.ColumnHolder;
import org.apache.druid.segment.data.IndexedInts;
import org.apache.druid.segment.filter.Filters;
import org.apache.druid.segment.realtime.firehose.WindowedStorageAdapter;

import javax.annotation.Nullable;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

public class DruidSegmentReader implements InputEntityReader, InputEntitySampler
{
  private final IndexIO indexIO;
  private final List<String> dimensions;
  private final List<String> metrics;
  private final DimFilter dimFilter;

  public DruidSegmentReader(
      IndexIO indexIO,
      List<String> dimensions,
      List<String> metrics,
      DimFilter dimFilter
  )
  {
    this.indexIO = indexIO;
    this.dimensions = dimensions;
    this.metrics = metrics;
    this.dimFilter = dimFilter;
  }

  @Override
  public CloseableIterator<InputRow> read(InputEntity<?> source, File temporaryDirectory) throws IOException
  {
    if (!(source instanceof DruidSegmentSource)) {
      throw new IAE("Cannot process [%s]", source.getClass().getName());
    }
    final CleanableFile segmentFile = source.fetch(temporaryDirectory, null);
    final WindowedStorageAdapter storageAdapter;
    try {
      storageAdapter = new WindowedStorageAdapter(
          new QueryableIndexStorageAdapter(
              indexIO.loadIndex(segmentFile.file())
          ),
          ((DruidSegmentSource) source).getIntervalFilter()
      );
    }
    catch (IOException e) {
      throw new RuntimeException(e);
    }
    final Sequence<InputRow> sequence = Sequences.concat(
        Sequences.map(
            storageAdapter.getAdapter().makeCursors(
                Filters.toFilter(dimFilter),
                storageAdapter.getInterval(),
                VirtualColumns.EMPTY,
                Granularities.ALL,
                false,
                null
            ), new Function<Cursor, Sequence<InputRow>>()
            {
              @Nullable
              @Override
              public Sequence<InputRow> apply(final Cursor cursor)
              {
                final BaseLongColumnValueSelector timestampColumnSelector =
                    cursor.getColumnSelectorFactory().makeColumnValueSelector(ColumnHolder.TIME_COLUMN_NAME);

                final Map<String, DimensionSelector> dimSelectors = new HashMap<>();
                for (String dim : dimensions) {
                  final DimensionSelector dimSelector = cursor
                      .getColumnSelectorFactory()
                      .makeDimensionSelector(new DefaultDimensionSpec(dim, dim));
                  // dimSelector is null if the dimension is not present
                  if (dimSelector != null) {
                    dimSelectors.put(dim, dimSelector);
                  }
                }

                final Map<String, BaseObjectColumnValueSelector> metSelectors = new HashMap<>();
                for (String metric : metrics) {
                  final BaseObjectColumnValueSelector metricSelector =
                      cursor.getColumnSelectorFactory().makeColumnValueSelector(metric);
                  metSelectors.put(metric, metricSelector);
                }

                return Sequences.simple(
                    new Iterable<InputRow>()
                    {
                      @Override
                      public Iterator<InputRow> iterator()
                      {
                        return new Iterator<InputRow>()
                        {
                          @Override
                          public boolean hasNext()
                          {
                            return !cursor.isDone();
                          }

                          @Override
                          public InputRow next()
                          {
                            final Map<String, Object> theEvent = Maps.newLinkedHashMap();
                            final long timestamp = timestampColumnSelector.getLong();
                            theEvent.put(TimestampSpec.DEFAULT_COLUMN, DateTimes.utc(timestamp));

                            for (Map.Entry<String, DimensionSelector> dimSelector :
                                dimSelectors.entrySet()) {
                              final String dim = dimSelector.getKey();
                              final DimensionSelector selector = dimSelector.getValue();
                              final IndexedInts vals = selector.getRow();

                              int valsSize = vals.size();
                              if (valsSize == 1) {
                                final String dimVal = selector.lookupName(vals.get(0));
                                theEvent.put(dim, dimVal);
                              } else if (valsSize > 1) {
                                List<String> dimVals = new ArrayList<>(valsSize);
                                for (int i = 0; i < valsSize; ++i) {
                                  dimVals.add(selector.lookupName(vals.get(i)));
                                }
                                theEvent.put(dim, dimVals);
                              }
                            }

                            for (Map.Entry<String, BaseObjectColumnValueSelector> metSelector :
                                metSelectors.entrySet()) {
                              final String metric = metSelector.getKey();
                              final BaseObjectColumnValueSelector selector = metSelector.getValue();
                              Object value = selector.getObject();
                              if (value != null) {
                                theEvent.put(metric, value);
                              }
                            }
                            cursor.advance();
                            return new MapBasedInputRow(timestamp, dimensions, theEvent);
                          }

                          @Override
                          public void remove()
                          {
                            throw new UnsupportedOperationException("Remove Not Supported");
                          }
                        };
                      }
                    }
                );
              }
            }
        )
    );
    return new CloseableIterator<InputRow>()
    {
      Yielder<InputRow> rowYielder = Yielders.each(sequence);

      @Override
      public boolean hasNext()
      {
        return !rowYielder.isDone();
      }

      @Override
      public InputRow next()
      {
        final InputRow inputRow = rowYielder.get();
        rowYielder = rowYielder.next(null);
        return inputRow;
      }

      @Override
      public void close() throws IOException
      {
        segmentFile.close();
      }
    };
  }

  @Override
  public CloseableIterator<InputRowListPlusJson> sample(InputEntity<?> source, File temporaryDirectory)
      throws IOException
  {
    return null;
  }
}
