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
package org.apache.druid.timeline;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;

import java.util.Objects;

public class IntRange
{
  private static final IntRange EMPTY = new IntRange(0, 0);

  public static IntRange empty()
  {
    return EMPTY;
  }

  private final int start; // TODO: var int?
  private final int end;

  @JsonCreator
  public IntRange(@JsonProperty("start") int start, @JsonProperty("end") int end)
  {
    this.start = start;
    this.end = end;
  }

  public void add(int i)
  {
    Preconditions.checkArgument(
        i == start || i == end,
        "Can't add a non-consecutive integer[%s] to range[%s, %s]",
        i,
        start,
        end
    );
  }

  @JsonProperty
  public int getStart()
  {
    return start;
  }

  @JsonProperty
  public int getEnd()
  {
    return end;
  }

  public boolean contains(int i)
  {
    return i >= start && i < end;
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
    IntRange shortRange = (IntRange) o;
    return start == shortRange.start &&
           end == shortRange.end;
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(start, end);
  }

  @Override
  public String toString()
  {
    return "IntRange{" +
           "start=" + start +
           ", end=" + end +
           '}';
  }
}
