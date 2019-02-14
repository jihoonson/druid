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

import org.apache.druid.data.input.InputRow;
import org.apache.druid.indexing.appenderator.ActionBasedSegmentAllocator;
import org.apache.druid.indexing.common.TaskToolbox;
import org.apache.druid.indexing.common.actions.SegmentAllocateAction;
import org.apache.druid.segment.indexing.DataSchema;
import org.apache.druid.segment.realtime.appenderator.SegmentIdWithShardSpec;
import org.joda.time.Interval;

import java.io.IOException;
import java.util.Map;
import java.util.Set;

public class RemoteSegmentAllocator implements IndexTaskSegmentAllocator
{
  private final String taskId;
  private final ActionBasedSegmentAllocator internalAllocator;

  public RemoteSegmentAllocator(
      TaskToolbox toolbox,
      String taskId,
      DataSchema dataSchema,
      Map<Interval, Set<Integer>> inputPartitionIds
  )
  {
    this.taskId = taskId;
    this.internalAllocator = new ActionBasedSegmentAllocator(
        toolbox.getTaskActionClient(),
        dataSchema,
        (schema, row, sequenceName, previousSegmentId, skipSegmentLineageCheck) -> {
          final Set<Integer> overshadowingSegments = inputPartitionIds.get(
              schema.getGranularitySpec().bucketInterval(row.getTimestamp()).orNull()
          );
          return new SegmentAllocateAction(
              schema.getDataSource(),
              row.getTimestamp(),
              schema.getGranularitySpec().getQueryGranularity(),
              schema.getGranularitySpec().getSegmentGranularity(),
              sequenceName,
              previousSegmentId,
              skipSegmentLineageCheck,
              overshadowingSegments
          );
        }
    );
  }

  @Override
  public SegmentIdWithShardSpec allocate(
      InputRow row,
      String sequenceName,
      String previousSegmentId,
      boolean skipSegmentLineageCheck
  ) throws IOException
  {
    return internalAllocator.allocate(row, sequenceName, previousSegmentId, skipSegmentLineageCheck);
  }

  @Override
  public String getSequenceName(Interval interval, InputRow inputRow)
  {
    return taskId;
  }
}
