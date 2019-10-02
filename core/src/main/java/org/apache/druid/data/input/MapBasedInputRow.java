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

package org.apache.druid.data.input;

import org.apache.druid.guice.annotations.PublicApi;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.ISE;
import org.joda.time.DateTime;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 *
 */
@PublicApi
public class MapBasedInputRow extends MapBasedRow implements InputRow
{
  private final List<String> dimensions;

  public MapBasedInputRow(
      long timestamp,
      List<String> dimensions,
      Map<String, Object> event
  )
  {
    super(timestamp, event);
    this.dimensions = dimensions;
  }

  public MapBasedInputRow(
      DateTime timestamp,
      List<String> dimensions,
      Map<String, Object> event
  )
  {
    super(timestamp, event);
    this.dimensions = dimensions;
  }

  @Override
  public List<String> getDimensions()
  {
    return dimensions;
  }

  @Override
  public String toString()
  {
    return "MapBasedInputRow{" +
           "timestamp=" + DateTimes.utc(getTimestampFromEpoch()) +
           ", event=" + getEvent() +
           ", dimensions=" + dimensions +
           '}';
  }

  public static class Builder
  {
    private final Map<String, Object> event = new HashMap<>();

    private final long timestamp;
    private final List<String> dimensions;

    public Builder(long timestamp, List<String> dimensions)
    {
      this.timestamp = timestamp;
      this.dimensions = dimensions;
    }

    public Builder addEvent(String column, Object value)
    {
      final Object prevValue = event.put(column, value);
      if (prevValue != null) {
        throw new ISE(
            "Duplicate column[%s] found. Previous value was [%s] and current value is [%s]",
            column,
            prevValue,
            value
        );
      }
      return this;
    }

    public MapBasedInputRow build()
    {
      return new MapBasedInputRow(timestamp, dimensions, event);
    }
  }
}
