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

package org.apache.druid.server.coordinator.helper;

import com.google.common.base.Preconditions;
import org.apache.druid.timeline.DataSegment;
import org.joda.time.Interval;

import java.util.List;

/**
 * Util class used by {@link DruidCoordinatorSegmentCompactor} and {@link CompactionSegmentSearchPolicy}.
 */
class SegmentCompactorUtil
{
  private static final double ALLOWED_MARGIN_FOR_SEGMENT_SIZE = 0.1;

  static boolean matchTargetSegmentSizeBytes(long targetCompactionSizeBytes, List<DataSegment> segments)
  {
    final double minAllowedSize = targetCompactionSizeBytes * (1 - ALLOWED_MARGIN_FOR_SEGMENT_SIZE);
    final double maxAllowedSize = targetCompactionSizeBytes * (1 + ALLOWED_MARGIN_FOR_SEGMENT_SIZE);
    return segments
        .stream()
        .allMatch(segment -> segment.getSize() >= minAllowedSize && segment.getSize() <= maxAllowedSize);
  }

  /**
   * Removes {@code smallInterval} from {@code largeInterval}.  The end of both intervals should be same.
   *
   * @return an interval of {@code largeInterval} - {@code smallInterval}.
   */
  static Interval removeIntervalFromEnd(Interval largeInterval, Interval smallInterval)
  {
    Preconditions.checkArgument(
        largeInterval.getEnd().equals(smallInterval.getEnd()),
        "end should be same. largeInterval[%s] smallInterval[%s]",
        largeInterval,
        smallInterval
    );
    return new Interval(largeInterval.getStart(), smallInterval.getStart());
  }

  private SegmentCompactorUtil()
  {
  }
}
