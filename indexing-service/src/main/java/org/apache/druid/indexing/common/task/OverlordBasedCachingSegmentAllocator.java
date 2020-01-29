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
import org.apache.druid.indexer.partitions.PartitionsSpec;
import org.apache.druid.indexing.common.TaskToolbox;
import org.apache.druid.indexing.common.actions.TaskAction;
import org.apache.druid.indexing.common.task.IndexTask.ShardSpecs;
import org.apache.druid.indexing.common.task.batch.parallel.SupervisorTaskAccess;
import org.apache.druid.segment.indexing.DataSchema;
import org.apache.druid.segment.realtime.appenderator.SegmentIdWithShardSpec;

import javax.annotation.Nullable;
import java.io.IOException;

public class OverlordBasedCachingSegmentAllocator implements CachingSegmentAllocator
{
  OverlordBasedCachingSegmentAllocator(
      final TaskToolbox toolbox,
      final @Nullable SupervisorTaskAccess supervisorTaskAccess,
      final DataSchema dataSchema,
      final TaskLockHelper taskLockHelper,
      final boolean appendToExisting,
      final PartitionsSpec partitionsSpec
  )
  {

  }

  @Override
  public ShardSpecs getShardSpecs()
  {
    return null;
  }

  @Nullable
  @Override
  public SegmentIdWithShardSpec allocate(
      InputRow row, String sequenceName, @Nullable String previousSegmentId, boolean skipSegmentLineageCheck
  ) throws IOException
  {
    return null;
  }
}
