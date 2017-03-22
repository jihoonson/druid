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

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import io.druid.java.util.common.guava.Sequence;
import io.druid.query.spec.QuerySegmentSpec;
import org.joda.time.Duration;
import org.joda.time.Interval;

import java.util.List;
import java.util.Map;
import java.util.Objects;

public abstract class SingleSourceBaseQuery<T extends Comparable<T>> extends BaseQuery<T>
{
//  private final DataSource dataSource;
//  private final QuerySegmentSpec querySegmentSpec;
  private final DataSourceWithSegmentSpec dataSource;
  private volatile Duration duration;

  public SingleSourceBaseQuery(
      DataSource dataSource,
      QuerySegmentSpec querySegmentSpec,
      boolean descending,
      Map<String, Object> context
  )
  {
    super(descending, context);
    Objects.requireNonNull(dataSource, "dataSource can't be null");
    Objects.requireNonNull(querySegmentSpec, "querySegmentSpec can't be null");

    this.dataSource = new DataSourceWithSegmentSpec(dataSource, querySegmentSpec);
  }

  @JsonProperty
  public DataSource getDataSource()
  {
    return dataSource.getDataSource();
  }

  @Override
  public Iterable<DataSourceWithSegmentSpec> getDataSources()
  {
    return ImmutableList.of(dataSource);
  }

  @JsonProperty("intervals")
  public QuerySegmentSpec getQuerySegmentSpec()
  {
    return dataSource.getQuerySegmentSpec();
  }

  @Override
  public Sequence<T> run(QuerySegmentWalker walker, Map<String, Object> context)
  {
    // TODO: check
    return run(
        dataSource.getQuerySegmentSpec().lookup(
            this,
            Iterables.getOnlyElement(dataSource.getDataSource().getNames()), walker),
        context
    );
  }

//  @Override
  public List<Interval> getIntervals()
  {
    return dataSource.getQuerySegmentSpec().getIntervals();
  }

//  @Override
  public Duration getDuration()
  {
    if (duration == null) {
      duration = initDuration(dataSource.getQuerySegmentSpec());
    }

    return duration;
  }

  @Override
  public boolean equals(Object o)
  {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    SingleSourceBaseQuery baseQuery = (SingleSourceBaseQuery) o;

    if (isDescending() != baseQuery.isDescending()) {
      return false;
    }
    if (getContext()!= null ? !getContext().equals(baseQuery.getContext()) : baseQuery.getContext() != null) {
      return false;
    }
    if (!dataSource.equals(baseQuery.dataSource)) {
      return false;
    }
    if (duration != null ? !duration.equals(baseQuery.duration) : baseQuery.duration != null) {
      return false;
    }

    return true;

//    return querySegmentSpec.equals(baseQuery.querySegmentSpec);
  }

  @Override
  public int hashCode()
  {
    int result = dataSource.hashCode();
    result = 31 * result + (isDescending() ? 1 : 0);
    result = 31 * result + (getContext() != null ? getContext().hashCode() : 0);
//    result = 31 * result + querySegmentSpec.hashCode();
    result = 31 * result + (duration != null ? duration.hashCode() : 0);
    return result;
  }
}
