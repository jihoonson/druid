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
import org.apache.druid.timeline.partition.SingleDimensionShardSpecFactory.SingleDimensionShardSpecContext;

public class SingleDimensionShardSpecFactory implements ShardSpecFactory<SingleDimensionShardSpecContext>
{
  private final String dimension;

  @JsonCreator
  public SingleDimensionShardSpecFactory(@JsonProperty("dimension") String dimension)
  {
    this.dimension = dimension;
  }

  @JsonProperty
  public String getDimension()
  {
    return dimension;
  }

  @Override
  public ShardSpec create(
      ObjectMapper objectMapper,
      int partitionId,
      int numPartitions,
      SingleDimensionShardSpecContext context
  )
  {
    return new SingleDimensionShardSpec(dimension, context.start, context.end, partitionId);
  }

  @Override
  public Class<? extends ShardSpec> getShardSpecClass()
  {
    return SingleDimensionShardSpec.class;
  }

  public static class SingleDimensionShardSpecContext implements ShardSpecFactory.Context
  {
    private final String start;
    private final String end;

    public SingleDimensionShardSpecContext(String start, String end)
    {
      this.start = start;
      this.end = end;
    }
  }
}
