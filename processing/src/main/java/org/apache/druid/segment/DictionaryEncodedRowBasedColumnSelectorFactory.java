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

import com.google.common.base.Predicate;
import org.apache.druid.common.config.NullHandling;
import org.apache.druid.query.dimension.DimensionSpec;
import org.apache.druid.query.extraction.ExtractionFn;
import org.apache.druid.query.filter.ValueMatcher;
import org.apache.druid.query.groupby.GroupByQuery;
import org.apache.druid.query.groupby.ResultRow;
import org.apache.druid.query.groupby.epinephelinae.MergedDictionary;
import org.apache.druid.query.monomorphicprocessing.RuntimeShapeInspector;
import org.apache.druid.segment.column.ColumnCapabilities;
import org.apache.druid.segment.column.ColumnCapabilitiesImpl;
import org.apache.druid.segment.column.ColumnHolder;
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.segment.column.ValueType;
import org.apache.druid.segment.data.IndexedInts;
import org.apache.druid.segment.data.SingleIndexedInt;

import javax.annotation.Nullable;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.function.ToLongFunction;

// TODO: should take care of non-string types too?
public class DictionaryEncodedRowBasedColumnSelectorFactory implements ColumnSelectorFactory
{
  private final Supplier<ResultRow> supplier;
  private final RowAdapter<ResultRow> adapter;
  private final Supplier<MergedDictionary[]> mergedDictionary;
  private final RowSignature rowSignature;
  private final RowBasedColumnSelectorFactory nonDimensionColumnSelectorFactory;
  private final GroupByQuery query;

  public DictionaryEncodedRowBasedColumnSelectorFactory(
      GroupByQuery query,
      Supplier<ResultRow> supplier,
      RowAdapter<ResultRow> adapter,
      Supplier<MergedDictionary[]> mergedDictionary,
      RowSignature rowSignature
  )
  {
    this.query = query;
    this.supplier = supplier;
    this.adapter = adapter;
    this.mergedDictionary = mergedDictionary;
    this.rowSignature = rowSignature;
    this.nonDimensionColumnSelectorFactory = RowBasedColumnSelectorFactory.create(
        adapter,
        supplier,
        rowSignature,
        false
    );
  }

  @Override
  public DimensionSelector makeDimensionSelector(DimensionSpec dimensionSpec)
  {
    return dimensionSpec.decorate(makeDimensionSelectorUndecorated(dimensionSpec));
  }

  private DimensionSelector makeDimensionSelectorUndecorated(DimensionSpec dimensionSpec)
  {
    final String dimension = dimensionSpec.getDimension();
    final ExtractionFn extractionFn = dimensionSpec.getExtractionFn();

    if (ColumnHolder.TIME_COLUMN_NAME.equals(dimension)) {
      if (extractionFn == null) {
        throw new UnsupportedOperationException("time dimension must provide an extraction function");
      }

      final ToLongFunction<ResultRow> timestampFunction = adapter.timestampFunction();

      return new BaseSingleValueDimensionSelector()
      {
        @Override
        protected String getValue()
        {
          return extractionFn.apply(timestampFunction.applyAsLong(supplier.get()));
        }

        @Override
        public void inspectRuntimeShape(RuntimeShapeInspector inspector)
        {
          inspector.visit("row", supplier);
          inspector.visit("extractionFn", extractionFn);
        }
      };
    } else if (query.getDimensions().stream().anyMatch(d -> d.getDimension().equals(dimension))) {
      // TODO: dimFunction returns a pair of (segmentId, dictionaryId)
      // need to modify below
      final Function<ResultRow, Object> dimFunction = adapter.columnFunction(dimension);
      @Nullable final MergedDictionary dictToUse;
      final int columnIndex = rowSignature.indexOf(dimension);
      if (columnIndex < 0) {
        // TODO: null?? or always return -1 when it's null?
        dictToUse = null;
      } else {
        dictToUse = mergedDictionary.get()[columnIndex];
      }

      return new DimensionSelector()
      {
        private final SingleIndexedInt indexedInt = new SingleIndexedInt();

        @Override
        public IndexedInts getRow()
        {
          // TODO: multi-values??
          // 1. get a resultRow from supplier
          // 2. retrieve column value from the resultRow. here, dictionary conversion is done.
          // 3. set newDictId in indexedInt.
          final int newDictId = (int) dimFunction.apply(supplier.get());
          indexedInt.setValue(newDictId);
          return indexedInt;
        }

        @Override
        public ValueMatcher makeValueMatcher(final @Nullable String value)
        {
          // TODO
          throw new UnsupportedOperationException();
//          if (extractionFn == null) {
//            return new ValueMatcher()
//            {
//              @Override
//              public boolean matches()
//              {
//                final List<String> dimensionValues = Rows.objectToStrings(dimFunction.apply(supplier.get()));
//                if (dimensionValues == null || dimensionValues.isEmpty()) {
//                  return value == null;
//                }
//
//                for (String dimensionValue : dimensionValues) {
//                  if (Objects.equals(NullHandling.emptyToNullIfNeeded(dimensionValue), value)) {
//                    return true;
//                  }
//                }
//                return false;
//              }
//
//              @Override
//              public void inspectRuntimeShape(RuntimeShapeInspector inspector)
//              {
//                inspector.visit("row", supplier);
//              }
//            };
//          } else {
//            return new ValueMatcher()
//            {
//              @Override
//              public boolean matches()
//              {
//                final List<String> dimensionValues = Rows.objectToStrings(dimFunction.apply(supplier.get()));
//                if (dimensionValues == null || dimensionValues.isEmpty()) {
//                  return value == null;
//                }
//
//                for (String dimensionValue : dimensionValues) {
//                  if (Objects.equals(extractionFn.apply(NullHandling.emptyToNullIfNeeded(dimensionValue)), value)) {
//                    return true;
//                  }
//                }
//                return false;
//              }
//
//              @Override
//              public void inspectRuntimeShape(RuntimeShapeInspector inspector)
//              {
//                inspector.visit("row", supplier);
//                inspector.visit("extractionFn", extractionFn);
//              }
//            };
//          }
        }

        @Override
        public ValueMatcher makeValueMatcher(final Predicate<String> predicate)
        {
          // TODO
          throw new UnsupportedOperationException();
//          final boolean matchNull = predicate.apply(null);
//          if (extractionFn == null) {
//            return new ValueMatcher()
//            {
//              @Override
//              public boolean matches()
//              {
//                final List<String> dimensionValues = Rows.objectToStrings(dimFunction.apply(supplier.get()));
//                if (dimensionValues == null || dimensionValues.isEmpty()) {
//                  return matchNull;
//                }
//
//                for (String dimensionValue : dimensionValues) {
//                  if (predicate.apply(NullHandling.emptyToNullIfNeeded(dimensionValue))) {
//                    return true;
//                  }
//                }
//                return false;
//              }
//
//              @Override
//              public void inspectRuntimeShape(RuntimeShapeInspector inspector)
//              {
//                inspector.visit("row", supplier);
//                inspector.visit("predicate", predicate);
//              }
//            };
//          } else {
//            return new ValueMatcher()
//            {
//              @Override
//              public boolean matches()
//              {
//                final List<String> dimensionValues = Rows.objectToStrings(dimFunction.apply(supplier.get()));
//                if (dimensionValues == null || dimensionValues.isEmpty()) {
//                  return matchNull;
//                }
//
//                for (String dimensionValue : dimensionValues) {
//                  if (predicate.apply(extractionFn.apply(NullHandling.emptyToNullIfNeeded(dimensionValue)))) {
//                    return true;
//                  }
//                }
//                return false;
//              }
//
//              @Override
//              public void inspectRuntimeShape(RuntimeShapeInspector inspector)
//              {
//                inspector.visit("row", supplier);
//                inspector.visit("predicate", predicate);
//              }
//            };
//          }
        }

        @Override
        public int getValueCardinality()
        {
          return dictToUse == null ? 0 : dictToUse.size();
        }

        @Override
        public String lookupName(int id)
        {
          final int newDictId = (int) dimFunction.apply(supplier.get());
          final String value;
          if (newDictId == DimensionDictionarySelector.CARDINALITY_UNKNOWN) {
            value = NullHandling.defaultStringValue();
          } else {
            value = dictToUse.lookup(newDictId);
          }

          return extractionFn == null ? value : extractionFn.apply(value);
        }

        @Override
        public boolean nameLookupPossibleInAdvance()
        {
          // TODO: verify this
          return true;
        }

        @Nullable
        @Override
        public IdLookup idLookup()
        {
          // TODO: is it useful?
          return null;
        }

        @Nullable
        @Override
        public Integer getObject()
        {
          return (int) dimFunction.apply(supplier.get());
        }

        @Override
        public Class<Integer> classOfObject()
        {
          return Integer.class;
        }

        @Override
        public void inspectRuntimeShape(RuntimeShapeInspector inspector)
        {
          // TODO: verify this
          inspector.visit("row", supplier);
          inspector.visit("extractionFn", extractionFn);
        }
      };
    } else {
      return nonDimensionColumnSelectorFactory.makeDimensionSelector(dimensionSpec);
    }
  }

  @Override
  public ColumnValueSelector makeColumnValueSelector(String columnName)
  {
    if (columnName.equals(ColumnHolder.TIME_COLUMN_NAME)) {
      final ToLongFunction<ResultRow> timestampFunction = adapter.timestampFunction();

      class TimeLongColumnSelector implements LongColumnSelector
      {
        @Override
        public long getLong()
        {
          return timestampFunction.applyAsLong(supplier.get());
        }

        @Override
        public boolean isNull()
        {
          // Time column never has null values
          return false;
        }

        @Override
        public void inspectRuntimeShape(RuntimeShapeInspector inspector)
        {
          inspector.visit("row", supplier);
        }
      }
      return new TimeLongColumnSelector();
    } else if (query.getDimensions().stream().anyMatch(dimensionSpec -> dimensionSpec.getDimension().equals(columnName))) {
      final Function<ResultRow, Object> columnFunction = adapter.columnFunction(columnName);
//      @Nullable final MergedDictionary dictToUse;
//      final int columnIndex = rowSignature.indexOf(columnName);
//      if (columnIndex < 0) {
//        // TODO: null?? or always return -1 when it's null?
//        dictToUse = null;
//      } else {
//        dictToUse = mergedDictionary.get()[columnIndex];
//      }

      return new ColumnValueSelector<Object>()
      {
        @Override
        public boolean isNull()
        {
          return !NullHandling.replaceWithDefault() && getCurrentDictId() == DimensionDictionarySelector.CARDINALITY_UNKNOWN;
        }

        @Override
        public double getDouble()
        {
          throw new UnsupportedOperationException();
        }

        @Override
        public float getFloat()
        {
          throw new UnsupportedOperationException();
        }

        @Override
        public long getLong()
        {
          // TODO: what if the dimension type is long?
          return getCurrentDictId();
        }

        @Nullable
        @Override
        public Object getObject()
        {
          // TODO: should i convert to new dict id?
          return getCurrentValue();
        }

        @Override
        public Class<Object> classOfObject()
        {
          return Object.class;
        }

        @Override
        public void inspectRuntimeShape(RuntimeShapeInspector inspector)
        {
          inspector.visit("row", supplier);
        }

        @Nullable
        private Object getCurrentValue()
        {
          return columnFunction.apply(supplier.get());
        }

        private int getCurrentDictId()
        {
          return (int) getCurrentValue();
        }
      };
    } else {
      return nonDimensionColumnSelectorFactory.makeColumnValueSelector(columnName);
    }
  }

  @Nullable
  @Override
  public ColumnCapabilities getColumnCapabilities(String column)
  {
    if (ColumnHolder.TIME_COLUMN_NAME.equals(column)) {
      // TIME_COLUMN_NAME is handled specially; override the provided rowSignature.
      return ColumnCapabilitiesImpl.createSimpleNumericColumnCapabilities(ValueType.LONG);
    } else if (query.getDimensions().stream().anyMatch(dimensionSpec -> dimensionSpec.getDimension().equals(column))) {
      final ValueType valueType = rowSignature.getColumnType(column).orElse(null);

      if (valueType != null) {
        if (valueType.isNumeric()) {
          return ColumnCapabilitiesImpl.createSimpleNumericColumnCapabilities(valueType);
        }

        if (valueType.isArray()) {
          return ColumnCapabilitiesImpl.createSimpleArrayColumnCapabilities(valueType);
        }

        // TODO: hasMultipleValues?
        // Do not set hasMultipleValues, because even though we might return multiple values, setting it affirmatively
        // causes expression selectors to always treat us as arrays, so leave as unknown
        return new ColumnCapabilitiesImpl()
            .setType(valueType)
            .setDictionaryEncoded(true)
            .setDictionaryValuesUnique(true)
            .setDictionaryValuesSorted(true);
      } else {
        return null;
      }
    } else {
      return nonDimensionColumnSelectorFactory.getColumnCapabilities(column);
    }
  }
}
