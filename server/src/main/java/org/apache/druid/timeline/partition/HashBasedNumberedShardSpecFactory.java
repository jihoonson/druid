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
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.druid.timeline.partition.HashBasedNumberedShardSpecFactory.HashBasedNumberedShardSpecContext;

import javax.annotation.Nullable;
import java.util.List;

public class HashBasedNumberedShardSpecFactory implements ShardSpecFactory<HashBasedNumberedShardSpecContext>
{
  @Nullable
  private final List<String> partitionDimensions;

  @JsonCreator
  public HashBasedNumberedShardSpecFactory(
      @JsonProperty("partitionDimensions") @Nullable List<String> partitionDimensions
  )
  {
    this.partitionDimensions = partitionDimensions;
  }

  @Nullable
  @JsonProperty
  public List<String> getPartitionDimensions()
  {
    return partitionDimensions;
  }

  @Override
  public ShardSpec create(
      ObjectMapper objectMapper,
      int partitionId,
      int numPartitions,
      HashBasedNumberedShardSpecContext context
  )
  {
    return new HashBasedNumberedShardSpec(
        partitionId,
        numPartitions,
        partitionDimensions,
        context.ordinal,
        objectMapper
    );
  }

  @Override
  public Class<? extends ShardSpec> getShardSpecClass()
  {
    return HashBasedNumberedShardSpec.class;
  }

  public static class HashBasedNumberedShardSpecContext implements ShardSpecFactory.Context
  {
    private final int ordinal;

    public HashBasedNumberedShardSpecContext(int ordinal)
    {
      this.ordinal = ordinal;
    }
  }
}
