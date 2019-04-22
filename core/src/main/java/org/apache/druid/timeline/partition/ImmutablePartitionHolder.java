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

import org.apache.druid.timeline.Overshadowable;

/**
 */
public class ImmutablePartitionHolder<T extends Overshadowable<T>> extends PartitionHolder<T>
{
  public ImmutablePartitionHolder(PartitionHolder<T> partitionHolder)
  {
    super(partitionHolder);
  }

  @Override
  public PartitionChunk<T> remove(PartitionChunk<T> tPartitionChunk)
  {
    throw new UnsupportedOperationException();
  }

  @Override
  public void add(PartitionChunk<T> tPartitionChunk)
  {
    throw new UnsupportedOperationException();
  }
}
