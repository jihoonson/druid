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

package org.apache.druid.query.aggregation;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import org.apache.druid.segment.ColumnSelectorFactory;

import javax.annotation.Nullable;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;

public class PolyglotJsAggregatorFactory extends AggregatorFactory
{
  private final String name;
  private final List<String> fieldNames;
  private final String fnAggregate;
  private final String fnReset;

  public PolyglotJsAggregatorFactory(
      @JsonProperty("name") String name,
      @JsonProperty("fieldNames") final List<String> fieldNames,
      @JsonProperty("fnAggregate") final String fnAggregate,
      @JsonProperty("fnReset") final String fnReset
  )
  {
    Preconditions.checkNotNull(name, "Must have a valid, non-null aggregator name");
    Preconditions.checkNotNull(fieldNames, "Must have a valid, non-null fieldNames");
    Preconditions.checkNotNull(fnAggregate, "Must have a valid, non-null fnAggregate");
    Preconditions.checkNotNull(fnReset, "Must have a valid, non-null fnReset");

    this.name = name;
    this.fieldNames = fieldNames;

    this.fnAggregate = fnAggregate;
    this.fnReset = fnReset;
  }

  @Override
  public Aggregator factorize(ColumnSelectorFactory metricFactory)
  {
    return null;
  }

  @Override
  public BufferAggregator factorizeBuffered(ColumnSelectorFactory columnSelectorFactory)
  {
    return new PolyglotJsBufferAggregator(
        fieldNames.stream().map(columnSelectorFactory::makeColumnValueSelector).collect(Collectors.toList()),
        fnReset,
        fnAggregate
    );
  }

  @Override
  public Comparator getComparator()
  {
    return null;
  }

  @Nullable
  @Override
  public Object combine(@Nullable Object lhs, @Nullable Object rhs)
  {
    return null;
  }

  @Override
  public AggregatorFactory getCombiningFactory()
  {
    return null;
  }

  @Override
  public List<AggregatorFactory> getRequiredColumns()
  {
    return null;
  }

  @Override
  public Object deserialize(Object object)
  {
    return null;
  }

  @Nullable
  @Override
  public Object finalizeComputation(@Nullable Object object)
  {
    return null;
  }

  @Override
  public String getName()
  {
    return null;
  }

  @Override
  public List<String> requiredFields()
  {
    return null;
  }

  @Override
  public String getTypeName()
  {
    return null;
  }

  @Override
  public int getMaxIntermediateSize()
  {
    return 0;
  }

  @Override
  public byte[] getCacheKey()
  {
    return new byte[0];
  }
}
