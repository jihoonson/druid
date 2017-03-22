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

package io.druid.segment.realtime;

import com.google.common.base.Preconditions;
import io.druid.query.SegmentDescriptor;
import org.joda.time.Interval;

import java.util.Map;

public class QuerySegmentWalkers
{

  public static void checkSingleSourceIntervals(Map<String, Iterable<Interval>> intervals)
  {
    Preconditions.checkState(intervals.size() == 1, "Multi source queries are not allowed yet.");
  }

  public static void checkSingleSourceSegments(Map<String, Iterable<SegmentDescriptor>> specs)
  {
    Preconditions.checkState(specs.size() == 1, "Multi source queries are not allowed yet.");
  }

  private QuerySegmentWalkers() {}
}
