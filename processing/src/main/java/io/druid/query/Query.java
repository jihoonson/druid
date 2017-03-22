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

package io.druid.query;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Ordering;
import io.druid.java.util.common.guava.Sequence;
import io.druid.query.datasourcemetadata.DataSourceMetadataQuery;
import io.druid.query.filter.DimFilter;
import io.druid.query.groupby.GroupByQuery;
import io.druid.query.join.JoinQuery;
import io.druid.query.metadata.metadata.SegmentMetadataQuery;
import io.druid.query.search.search.SearchQuery;
import io.druid.query.select.SelectQuery;
import io.druid.query.spec.QuerySegmentSpec;
import io.druid.query.timeboundary.TimeBoundaryQuery;
import io.druid.query.timeseries.TimeseriesQuery;
import io.druid.query.topn.TopNQuery;
import org.joda.time.Duration;

import java.util.Map;
import java.util.stream.StreamSupport;

@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "queryType")
@JsonSubTypes(value = {
    @JsonSubTypes.Type(name = Query.TIMESERIES, value = TimeseriesQuery.class),
    @JsonSubTypes.Type(name = Query.SEARCH, value = SearchQuery.class),
    @JsonSubTypes.Type(name = Query.TIME_BOUNDARY, value = TimeBoundaryQuery.class),
    @JsonSubTypes.Type(name = Query.GROUP_BY, value = GroupByQuery.class),
    @JsonSubTypes.Type(name = Query.SEGMENT_METADATA, value = SegmentMetadataQuery.class),
    @JsonSubTypes.Type(name = Query.SELECT, value = SelectQuery.class),
    @JsonSubTypes.Type(name = Query.TOPN, value = TopNQuery.class),
    @JsonSubTypes.Type(name = Query.JOIN, value = JoinQuery.class),
    @JsonSubTypes.Type(name = Query.DATASOURCE_METADATA, value = DataSourceMetadataQuery.class)

})
public interface Query<T>
{
  String TIMESERIES = "timeseries";
  String SEARCH = "search";
  String TIME_BOUNDARY = "timeBoundary";
  String GROUP_BY = "groupBy";
  String SEGMENT_METADATA = "segmentMetadata";
  String SELECT = "select";
  String TOPN = "topN";
  String DATASOURCE_METADATA = "dataSourceMetadata";
  String JOIN = "join";

  Iterable<DataSourceWithSegmentSpec> getDataSources();

//  @Deprecated
//  default DataSource getDataSource()
//  {
//    return Iterables.getOnlyElement(getDataSources()).getDataSource();
//  }

  boolean hasFilters();

  DimFilter getFilter();

  String getType();

  Sequence<T> run(QuerySegmentWalker walker, Map<String, Object> context);

  Sequence<T> run(QueryRunner<T> runner, Map<String, Object> context);

//  @Deprecated
//  default List<Interval> getIntervals()
//  {
//    return Iterables.getOnlyElement(getDataSources()).getQuerySegmentSpec().getIntervals();
//  }

  Duration getDuration(DataSource dataSource);

  default Duration getTotalDuration()
  {
    return StreamSupport.stream(getDataSources().spliterator(), false)
        .map(spec -> BaseQuery.initDuration(spec.getQuerySegmentSpec()))
        .reduce(new Duration(0), Duration::plus);
  }

//  @Deprecated
//  default Duration getDuration()
//  {
//    return getDuration(getDataSource());
//  }

  Map<String, Object> getContext();

  <ContextType> ContextType getContextValue(String key);

  <ContextType> ContextType getContextValue(String key, ContextType defaultValue);

  boolean getContextBoolean(String key, boolean defaultValue);

  boolean isDescending();

  Ordering<T> getResultOrdering();

  String getId();

  Query<T> withId(String id);

  default DataSourceWithSegmentSpec getDistributionTarget()
  {
    return getContextValue(QueryContextKeys.DIST_TARGET_SOURCE);
  }

  default Query<T> distributeBy(DataSourceWithSegmentSpec spec)
  {
    return withOverriddenContext(ImmutableMap.of(QueryContextKeys.DIST_TARGET_SOURCE, spec));
  }

  Query<T> withOverriddenContext(Map<String, Object> contextOverride);

  Query<T> replaceQuerySegmentSpecWith(DataSource dataSource, QuerySegmentSpec spec);

  Query<T> replaceQuerySegmentSpecWith(String dataSource, QuerySegmentSpec spec);

//  @Deprecated
//  default Query<T> withQuerySegmentSpec(QuerySegmentSpec spec)
//  {
//    return replaceQuerySegmentSpecWith(getDataSource(), spec);
//  }

  Query<T> replaceDataSourceWith(DataSource src, DataSource dst);

//  @Deprecated
//  default Query<T> withDataSource(DataSource dataSource)
//  {
//    return replaceDataSourceWith(getDataSource(), dataSource);
//  }
}
