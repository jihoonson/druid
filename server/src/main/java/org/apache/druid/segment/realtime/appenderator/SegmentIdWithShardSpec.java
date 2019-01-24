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

package org.apache.druid.segment.realtime.appenderator;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.timeline.SegmentId;
import org.apache.druid.timeline.partition.ShardSpec;
import org.joda.time.Interval;

<<<<<<< HEAD:server/src/main/java/org/apache/druid/segment/realtime/appenderator/SegmentIdentifier.java
import javax.annotation.Nullable;
import java.util.Collections;
import java.util.Objects;
import java.util.Set;

public class SegmentIdentifier
=======
/**
 * {@link SegmentId} with additional {@link ShardSpec} info. {@link #equals}/{@link #hashCode} and {@link
 * #compareTo} don't consider that additinal info.
 *
 * This class is separate from {@link SegmentId} because in a lot of places segment ids are transmitted as "segment id
 * strings" that don't contain enough information to deconstruct the {@link ShardSpec}. Also, even a single extra field
 * in {@link SegmentId} is important, because it adds to the memory footprint considerably.
 */
public final class SegmentIdWithShardSpec implements Comparable<SegmentIdWithShardSpec>
>>>>>>> 66f64cd8bdf3a742d3d6a812b7560a9ffc0c28b8:server/src/main/java/org/apache/druid/segment/realtime/appenderator/SegmentIdWithShardSpec.java
{
  private final SegmentId id;
  private final ShardSpec shardSpec;
  private final String asString;
  private final Set<Integer> overshadowingSegments;

  public SegmentIdentifier(
      String dataSource,
      Interval interval,
      String version,
      ShardSpec shardSpec
  )
  {
    this(dataSource, interval, version, shardSpec, null);
  }

  @JsonCreator
  public SegmentIdWithShardSpec(
      @JsonProperty("dataSource") String dataSource,
      @JsonProperty("interval") Interval interval,
      @JsonProperty("version") String version,
      @JsonProperty("shardSpec") ShardSpec shardSpec,
      @JsonProperty("overshadowingSegments") @Nullable Set<Integer> overshadowingSegments
  )
  {
    this.id = SegmentId.of(dataSource, interval, version, shardSpec.getPartitionNum());
    this.shardSpec = Preconditions.checkNotNull(shardSpec, "shardSpec");
<<<<<<< HEAD:server/src/main/java/org/apache/druid/segment/realtime/appenderator/SegmentIdentifier.java
    this.overshadowingSegments = overshadowingSegments == null ? Collections.emptySet() : overshadowingSegments;
    this.asString = DataSegment.makeDataSegmentIdentifier(
        dataSource,
        interval.getStart(),
        interval.getEnd(),
        version,
        shardSpec
    );
=======
    this.asString = id.toString();
  }

  public SegmentId asSegmentId()
  {
    return id;
>>>>>>> 66f64cd8bdf3a742d3d6a812b7560a9ffc0c28b8:server/src/main/java/org/apache/druid/segment/realtime/appenderator/SegmentIdWithShardSpec.java
  }

  public SegmentIdentifier withShardSpec(ShardSpec shardSpec)
  {
    return new SegmentIdentifier(
        dataSource,
        interval,
        version,
        shardSpec,
        overshadowingSegments
    );
  }

  public SegmentIdentifier withOvershadowedGroup(Set<Integer> overshadowedGroup)
  {
    return new SegmentIdentifier(
        dataSource,
        interval,
        version,
        shardSpec,
        overshadowedGroup
    );
  }

  @JsonProperty
  public String getDataSource()
  {
    return id.getDataSource();
  }

  @JsonProperty
  public Interval getInterval()
  {
    return id.getInterval();
  }

  @JsonProperty
  public String getVersion()
  {
    return id.getVersion();
  }

  @JsonProperty
  public ShardSpec getShardSpec()
  {
    return shardSpec;
  }

<<<<<<< HEAD:server/src/main/java/org/apache/druid/segment/realtime/appenderator/SegmentIdentifier.java
  @JsonProperty
  public Set<Integer> getOvershadowingSegments()
  {
    return overshadowingSegments;
  }

  public String getIdentifierAsString()
  {
    return asString;
  }

=======
>>>>>>> 66f64cd8bdf3a742d3d6a812b7560a9ffc0c28b8:server/src/main/java/org/apache/druid/segment/realtime/appenderator/SegmentIdWithShardSpec.java
  @Override
  public boolean equals(Object o)
  {
    if (this == o) {
      return true;
    }
    if (!(o instanceof SegmentIdWithShardSpec)) {
      return false;
    }
    SegmentIdWithShardSpec that = (SegmentIdWithShardSpec) o;
    return id.equals(that.id);
  }

  @Override
  public int hashCode()
  {
    return id.hashCode();
  }

  @Override
  public int compareTo(SegmentIdWithShardSpec o)
  {
    return id.compareTo(o.id);
  }

  @Override
  public String toString()
  {
    return asString;
  }

  public static SegmentIdWithShardSpec fromDataSegment(final DataSegment segment)
  {
    return new SegmentIdWithShardSpec(
        segment.getDataSource(),
        segment.getInterval(),
        segment.getVersion(),
        segment.getShardSpec(),
        segment.getOvershadowedGroup()
    );
  }
}
