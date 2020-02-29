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

package org.apache.druid.timeline.partition;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import org.apache.druid.java.util.common.StringUtils;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * Sorted array of range partition boundaries for a string dimension.
 * Note that
 */
public class PartitionBoundaries
{
  private final String[] boundaries;

  public static PartitionBoundaries empty()
  {
    return fromNonNullBoundaries(new String[]{});
  }

  public static PartitionBoundaries fromNonNullBoundaries(String[] boundaries)
  {
    if (boundaries.length == 0) {
      return new PartitionBoundaries(boundaries);
    }

    Preconditions.checkArgument(
        Arrays.stream(boundaries).noneMatch(Objects::isNull),
        "Input boundaries cannot contain nulls"
    );
    // Future improvement: Handle skewed partitions better (e.g., many values are repeated).
    List<String> dedupedBoundaries = Arrays.stream(boundaries)
                                           .distinct()
                                           .sorted()
                                           .collect(Collectors.toCollection(ArrayList::new));

    // First partition starts with null (see StringPartitionChunk.isStart())
    dedupedBoundaries.set(0, null);

    // Last partition ends with null (see StringPartitionChunk.isEnd())
    if (dedupedBoundaries.size() == 1) {
      dedupedBoundaries.add(null);
    } else {
      dedupedBoundaries.set(dedupedBoundaries.size() - 1, null);
    }
    return new PartitionBoundaries(dedupedBoundaries.toArray(new String[0]));
  }

  public static PartitionBoundaries fromShardSpecs(List<SingleDimensionShardSpec> sortedShardSpecs)
  {
    final List<String> boundaries = sortedShardSpecs
        .stream()
        .sorted(SingleDimensionShardSpec.COMPARATOR)
        .map(SingleDimensionShardSpec::getStart)
        .collect(Collectors.toList());
    boundaries.add(null);
    return new PartitionBoundaries(boundaries.toArray(new String[0]));
  }

  @JsonCreator
  private PartitionBoundaries(@JsonProperty("boundaries") String[] boundaries)
  {
    this.boundaries = Preconditions.checkNotNull(boundaries, "boundaries");
  }

  @JsonProperty
  public String[] getBoundaries()
  {
    return boundaries;
  }

  public String get(int i)
  {
    if (i < boundaries.length) {
      return boundaries[i];
    } else {
      throw new IndexOutOfBoundsException(StringUtils.format(
          "Index[%s] is requested for array of size[%s]",
          i,
          boundaries.length
      ));
    }
  }

  @JsonIgnore
  public int getNumBuckets()
  {
    return boundaries.length > 0 ? boundaries.length - 1 : 0;
  }

  public int size()
  {
    return boundaries.length;
  }

  @JsonIgnore
  public boolean isEmpty()
  {
    return boundaries.length == 0;
  }

  public int bucketFor(@Nullable String key)
  {
    Preconditions.checkState(boundaries.length > 0, "Cannot find index for key[%s] from empty boundaries", key);

    if (key == null) {
      return 0;
    }

    final int index = Arrays.binarySearch(boundaries, 1, boundaries.length - 1, key);
    if (index < 0) {
      return -(index + 1) - 1;
    } else {
      return index;
    }
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
    PartitionBoundaries that = (PartitionBoundaries) o;
    return Arrays.equals(boundaries, that.boundaries);
  }

  @Override
  public int hashCode()
  {
    return Arrays.hashCode(boundaries);
  }
}
