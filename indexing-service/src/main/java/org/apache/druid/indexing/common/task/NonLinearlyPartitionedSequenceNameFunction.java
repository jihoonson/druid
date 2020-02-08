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
import org.apache.druid.indexing.common.task.batch.partition.PartitionAnalysis;
import org.joda.time.Interval;

/**
 * This sequence name function should be used for the hash or range partitioning. This function creates a proper
 * sequence name based on the partition information (time chunk + partition ID).
 *
 * Note that all segment IDs should be allocated upfront to use this function.
 *
 * @see org.apache.druid.indexer.partitions.SecondaryPartitionType
 */
public class NonLinearlyPartitionedSequenceNameFunction implements SequenceNameFunction
{
  private final String taskId;
  private final PartitionAnalysis<?, ?> partitionAnalysis;

  public NonLinearlyPartitionedSequenceNameFunction(String taskId, PartitionAnalysis<?, ?> partitionAnalysis)
  {
    this.taskId = taskId;
    this.partitionAnalysis = partitionAnalysis;
  }

  /**
   * Returns a sequence name based on the taskId, time chunk (primary partition), and the bucket ID of the
   * secondary partitioning.
   */
  @Override
  public String getSequenceName(Interval interval, InputRow inputRow)
  {
    final int bucketId = partitionAnalysis.getBucketAnalysis(interval)
                                          .lookupBucket(inputRow.getTimestampFromEpoch(), inputRow);
    return getSequenceName(taskId, interval, bucketId);
  }

  /**
   * Create a sequence name from the given shardSpec and interval.
   *
   * See {@link org.apache.druid.timeline.partition.HashBasedNumberedShardSpec} as an example of partitioning.
   */
  public static String getSequenceName(String taskId, Interval interval, int bucketId)
  {
    // Note: We do not use String format here since this can be called in a tight loop
    // and it's faster to add strings together than it is to use String#format
    return taskId + "_" + interval + "_" + bucketId;
  }
}
