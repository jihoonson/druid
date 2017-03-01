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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.druid.data.input.Row;
import io.druid.java.util.common.granularity.Granularity;
import io.druid.query.BaseQuery;
import io.druid.query.DataSource;
import io.druid.query.Query;
import io.druid.query.dimension.DimensionSpec;
import io.druid.query.filter.DimFilter;
import io.druid.query.spec.QuerySegmentSpec;
import io.druid.segment.VirtualColumns;

import java.util.List;
import java.util.Map;

// TODO: Row?
public class JoinQuery extends BaseQuery<Row>
{
  // join type
  private final JoinSpec joinSpec;
  private final Granularity granularity;
  private final List<DimensionSpec> dimensions; // out
  private final List<String> metrics; // out
  private final VirtualColumns virtualColumns;
  private final DimFilter filter;

  @JsonCreator
  public JoinQuery(
      @JsonProperty("dataSource") DataSource dataSource,
      @JsonProperty("join") JoinSpec joinSpec,
      @JsonProperty("granularity") Granularity granularity,
      @JsonProperty("dimensions") List<DimensionSpec> dimensions,
      @JsonProperty("metrics") List<String> metrics,
      @JsonProperty("intervals") QuerySegmentSpec querySegmentSpec,
      @JsonProperty("virtualColumns") VirtualColumns virtualColumns,
      @JsonProperty("filter") DimFilter filter,
      @JsonProperty("context") Map<String, Object> context
  )
  {
    super(dataSource, querySegmentSpec, false, context);
    this.joinSpec = joinSpec;
    this.granularity = granularity;
    this.dimensions = dimensions;
    this.metrics = metrics;
    this.virtualColumns = virtualColumns;
    this.filter = filter;
  }

  @Override
  public boolean hasFilters()
  {
    return filter != null;
  }

  @Override
  public DimFilter getFilter()
  {
    return filter;
  }

  @Override
  public String getType()
  {
    return Query.JOIN;
  }

  @JsonProperty
  public JoinSpec getJoinSpec()
  {
    return joinSpec;
  }

  @JsonProperty
  public Granularity getGranularity()
  {
    return granularity;
  }

  @JsonProperty
  public List<DimensionSpec> getDimensions()
  {
    return dimensions;
  }

  @JsonProperty
  public List<String> getMetrics()
  {
    return metrics;
  }

  @JsonProperty
  public VirtualColumns getVirtualColumns()
  {
    return virtualColumns;
  }

  @Override
  public Query<Row> withOverriddenContext(Map<String, Object> contextOverride)
  {
    return new JoinQuery(
        getDataSource(),
        joinSpec,
        granularity,
        dimensions,
        metrics,
        getQuerySegmentSpec(),
        virtualColumns,
        filter,
        computeOverridenContext(contextOverride)
    );
  }

  @Override
  public Query<Row> withQuerySegmentSpec(QuerySegmentSpec spec)
  {
    return new JoinQuery(
        getDataSource(),
        joinSpec,
        granularity,
        dimensions,
        metrics,
        spec,
        virtualColumns,
        filter,
        getContext()
    );
  }

  @Override
  public Query<Row> withDataSource(DataSource dataSource)
  {
    return new JoinQuery(
        dataSource,
        joinSpec,
        granularity,
        dimensions,
        metrics,
        getQuerySegmentSpec(),
        virtualColumns,
        filter,
        getContext()
    );
  }

  @Override
  public boolean equals(Object o)
  {
    // TODO
    return false;
  }

  @Override
  public int hashCode()
  {
    // TODO
    return 0;
  }
}
