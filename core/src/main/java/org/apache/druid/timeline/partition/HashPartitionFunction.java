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
import com.fasterxml.jackson.annotation.JsonValue;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.hash.Hashing;
import org.apache.druid.java.util.common.StringUtils;

import java.util.List;

public enum HashPartitionFunction
{
  // TODO: name?
  V1 {
    @Override
    public int hash(ObjectMapper jsonMapper, List<Object> partitionKeys, int numBuckets)
    {
      try {
        return Math.abs(
            Hashing.murmur3_32().hashBytes(jsonMapper.writeValueAsBytes(partitionKeys)).asInt() % numBuckets
        );
      }
      catch (JsonProcessingException e) {
        throw new RuntimeException(e);
      }
    }
  };

  // TODO: javadoc
  abstract public int hash(ObjectMapper jsonMapper, List<Object> partitionKeys, int numBuckets);

  @JsonCreator
  public static HashPartitionFunction fromString(String type)
  {
    return HashPartitionFunction.valueOf(StringUtils.toUpperCase(type));
  }

  @Override
  @JsonValue
  public String toString()
  {
    return StringUtils.toLowerCase(name());
  }
}
