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
import org.joda.time.Interval;

public class LinearlyPartitionedSequenceNameFunction implements SequenceNameFunction
{
  private final String taskId;

  public LinearlyPartitionedSequenceNameFunction(String taskId)
  {
    this.taskId = taskId;
  }

  @Override
  public String getSequenceName(Interval interval, InputRow inputRow)
  {
    // Segments are created as needed, using a single sequence name. They may be allocated from the overlord
    // (in append mode) or may be created on our own authority (in overwrite mode).
    return taskId;
  }
}
