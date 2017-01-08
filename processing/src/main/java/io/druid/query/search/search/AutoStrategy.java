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

package io.druid.query.search.search;

import com.metamx.emitter.EmittingLogger;
import io.druid.collections.bitmap.ImmutableBitmap;
import io.druid.query.dimension.DimensionSpec;
import io.druid.query.filter.BitmapIndexSelector;
import io.druid.segment.ColumnSelectorBitmapIndexSelector;
import io.druid.segment.QueryableIndex;
import io.druid.segment.Segment;
import io.druid.segment.column.BitmapIndex;
import io.druid.segment.column.Column;
import io.druid.segment.filter.AndFilter;
import io.druid.segment.filter.InFilter;

import java.util.List;

public class AutoStrategy extends SearchStrategy
{
  public static final String NAME = "auto";

  private static final EmittingLogger log = new EmittingLogger(AutoStrategy.class);
  private static final double FILTER_SELECTIVITY = 0.1;

  public AutoStrategy(SearchQuery query)
  {
    super(query);
  }

  @Override
  public List<SearchQueryExecutor> getExecutionPlan(SearchQuery query, Segment segment)
  {
    final QueryableIndex index = segment.asQueryableIndex();

    if (index != null) {
      final BitmapIndexSelector selector = new ColumnSelectorBitmapIndexSelector(
          index.getBitmapFactoryForDimensions(),
          index
      );

      // TODO: add a comment for this and possible optimization
      if (filter == null || filter.supportsBitmapIndex(selector)) {
//        final ImmutableBitmap timeFilteredBitmap = UseIndexesStrategy.makeTimeFilteredBitmap(index,
//                                                                                             segment,
//                                                                                             filter,
//                                                                                             interval);
        final List<DimensionSpec> dimsToSearch = getDimsToSearch(index.getAvailableDimensions(),
                                                                 query.getDimensions());

        // Choose a search query execution strategy depending on the query.
        // TODO: explain cost model
        final SearchQueryDecisionHelper helper = getDecisionHelper(index);
        final double useIndexStrategyCost = helper.getBitmapIntersectCost() * computeTotalCard(index, dimsToSearch);
//        final double cursorOnlyStrategyCost = timeFilteredBitmap.size() * dimsToSearch.size();
//        final double cursorOnlyStrategyCost = ((AndFilter)filter).estimateSelectivity(selector, index.getNumRows()) * index.getNumRows() * dimsToSearch.size();
        final double cursorOnlyStrategyCost = FILTER_SELECTIVITY * index.getNumRows() * dimsToSearch.size();

//        log.info("useIndexStrategyCost: %f, cursorOnlyStrategyCost: %f, estimatedCost: %f", useIndexStrategyCost, cursorOnlyStrategyCost, estimatedCost);
        log.info("useIndexStrategyCost: %f, cursorOnlyStrategyCost: %f", useIndexStrategyCost, cursorOnlyStrategyCost);

        if (useIndexStrategyCost < cursorOnlyStrategyCost) {
          log.debug("Use-index execution strategy is selected, query id [%s]", query.getId());
//          return new UseIndexesStrategy(query, timeFilteredBitmap).getExecutionPlan(query, segment);
          return new UseIndexesStrategy(query, null).getExecutionPlan(query, segment);
        } else {
          log.debug("Cursor-only execution strategy is selected, query id [%s]", query.getId());
          return new CursorOnlyStrategy(query).getExecutionPlan(query, segment);
        }
      } else {
        log.debug("Filter doesn't support bitmap index. Fall back to cursor-only execution strategy, query id [%s]",
                  query.getId());
        return new CursorOnlyStrategy(query).getExecutionPlan(query, segment);
      }

    } else {
      log.debug("Index doesn't exist. Fall back to cursor-only execution strategy, query id [%s]", query.getId());
      return new CursorOnlyStrategy(query).getExecutionPlan(query, segment);
    }
  }

  private static long computeTotalCard(final QueryableIndex index, final Iterable<DimensionSpec> dimensionSpecs)
  {
    long totalCard = 0;
    for (DimensionSpec dimension : dimensionSpecs) {
      final Column column = index.getColumn(dimension.getDimension());
      if (column != null) {
        final BitmapIndex bitmapIndex = column.getBitmapIndex();
        if (bitmapIndex != null) {
          totalCard += bitmapIndex.getCardinality();
        }
      }
    }
    return totalCard;
  }
}
