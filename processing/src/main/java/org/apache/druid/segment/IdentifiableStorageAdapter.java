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

import org.apache.druid.java.util.common.granularity.Granularity;
import org.apache.druid.java.util.common.guava.Sequence;
import org.apache.druid.query.QueryMetrics;
import org.apache.druid.query.filter.Filter;
import org.apache.druid.segment.column.ColumnCapabilities;
import org.apache.druid.segment.data.Indexed;
import org.joda.time.DateTime;
import org.joda.time.Interval;

import javax.annotation.Nullable;

public class IdentifiableStorageAdapter implements StorageAdapter
{
  private final int id;
  private final StorageAdapter delegate;

  public IdentifiableStorageAdapter(int id, StorageAdapter delegate)
  {
    this.id = id;
    this.delegate = delegate;
  }

  public int getId()
  {
    return id;
  }

  @Override
  public Interval getInterval()
  {
    return delegate.getInterval();
  }

  @Override
  public Indexed<String> getAvailableDimensions()
  {
    return delegate.getAvailableDimensions();
  }

  @Override
  public Iterable<String> getAvailableMetrics()
  {
    return delegate.getAvailableMetrics();
  }

  @Override
  public int getDimensionCardinality(String column)
  {
    return delegate.getDimensionCardinality(column);
  }

  @Override
  public DateTime getMinTime()
  {
    return delegate.getMinTime();
  }

  @Override
  public DateTime getMaxTime()
  {
    return delegate.getMaxTime();
  }

  @Nullable
  @Override
  public Comparable getMinValue(String column)
  {
    return delegate.getMinValue(column);
  }

  @Nullable
  @Override
  public Comparable getMaxValue(String column)
  {
    return delegate.getMaxValue(column);
  }

  @Nullable
  @Override
  public ColumnCapabilities getColumnCapabilities(String column)
  {
    return delegate.getColumnCapabilities(column);
  }

  @Nullable
  @Override
  public String getColumnTypeName(String column)
  {
    return delegate.getColumnTypeName(column);
  }

  @Override
  public int getNumRows()
  {
    return delegate.getNumRows();
  }

  @Override
  public DateTime getMaxIngestedEventTime()
  {
    return delegate.getMaxIngestedEventTime();
  }

  @Override
  public Metadata getMetadata()
  {
    return delegate.getMetadata();
  }

  @Override
  public Sequence<Cursor> makeCursors(
      @Nullable Filter filter,
      Interval interval,
      VirtualColumns virtualColumns,
      Granularity gran,
      boolean descending,
      @Nullable QueryMetrics<?> queryMetrics
  )
  {
    return delegate.makeCursors(
        filter,
        interval,
        virtualColumns,
        gran,
        descending,
        queryMetrics
    );
  }
}
