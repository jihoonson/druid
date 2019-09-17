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

package org.apache.druid.indexing.common.task;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.hash.HashFunction;
import com.google.common.hash.Hasher;
import com.google.common.hash.Hashing;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.java.util.common.JodaUtils;
import org.apache.druid.timeline.DataSegment;
import org.joda.time.Interval;

import javax.annotation.Nullable;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

public class CompactionIntervalSpec implements CompactionInputSpec
{
  public static final String TYPE = "interval";
  private static final HashFunction HASH_FUNCTION = Hashing.sha256();

  private final Interval interval;
  @Nullable
  private final String sha256OfSortedSegmentIds;

  @VisibleForTesting
  static byte[] hash(List<DataSegment> segments)
  {
    Collections.sort(segments);
    final Hasher hasher = HASH_FUNCTION.newHasher();
    segments.forEach(segment -> hasher.putString(segment.getId().toString(), StandardCharsets.UTF_8));
    return hasher.hash().asBytes();
  }

  @JsonCreator
  public CompactionIntervalSpec(
      @JsonProperty("interval") Interval interval,
      @JsonProperty("sha256OfSortedSegmentIds") @Nullable String sha256OfSortedSegmentIds
  )
  {
    if (interval != null && interval.toDurationMillis() == 0) {
      throw new IAE("Interval[%s] is empty, must specify a nonempty interval", interval);
    }
    this.interval = interval;
    this.sha256OfSortedSegmentIds = sha256OfSortedSegmentIds;
  }

  @JsonProperty
  public Interval getInterval()
  {
    return interval;
  }

  @Nullable
  @JsonProperty
  public String getSha256OfSortedSegmentIds()
  {
    return sha256OfSortedSegmentIds;
  }

  @Override
  public Interval findInterval(String dataSource)
  {
    return interval;
  }

  @Override
  public boolean validateSegments(List<DataSegment> segments)
  {
    final Interval segmentsInterval = JodaUtils.umbrellaInterval(
        segments.stream().map(DataSegment::getInterval).collect(Collectors.toList())
    );
    if (interval.overlaps(segmentsInterval)) {
      if (sha256OfSortedSegmentIds != null) {
        final byte[] hashOfThem = hash(segments);
        final byte[] hashOfOurs = sha256OfSortedSegmentIds.getBytes(StandardCharsets.UTF_8);
        return Arrays.equals(hashOfOurs, hashOfThem);
      } else {
        return true;
      }
    } else {
      return false;
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
    CompactionIntervalSpec that = (CompactionIntervalSpec) o;
    return Objects.equals(interval, that.interval) &&
           Objects.equals(sha256OfSortedSegmentIds, that.sha256OfSortedSegmentIds);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(interval, sha256OfSortedSegmentIds);
  }

  @Override
  public String toString()
  {
    return "CompactionIntervalSpec{" +
           "interval=" + interval +
           ", sha256OfSegmentIds='" + sha256OfSortedSegmentIds + '\'' +
           '}';
  }
}
