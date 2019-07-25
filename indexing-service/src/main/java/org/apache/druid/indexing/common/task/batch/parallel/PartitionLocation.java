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

package org.apache.druid.indexing.common.task.batch.parallel;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.druid.java.util.common.StringUtils;
import org.joda.time.Interval;

import java.net.URI;

public class PartitionLocation
{
  private final String host;
  private final int port;
  private final Interval interval;
  private final int partitionId;

  @JsonCreator
  public PartitionLocation(
      @JsonProperty("host") String host,
      @JsonProperty("port") int port,
      @JsonProperty("interval") Interval interval,
      @JsonProperty("partitionId") int partitionId
  )
  {
    this.host = host;
    this.port = port;
    this.interval = interval;
    this.partitionId = partitionId;
  }

  @JsonProperty
  public String getHost()
  {
    return host;
  }

  @JsonProperty
  public int getPort()
  {
    return port;
  }

  @JsonProperty
  public Interval getInterval()
  {
    return interval;
  }

  @JsonProperty
  public int getPartitionId()
  {
    return partitionId;
  }

  public URI toIntermediaryDataServerURI(String supervisorTaskId)
  {
    //noinspection ConstantConditions
    return URI.create(
        StringUtils.urlEncode(
            StringUtils.format(
                "http://%s:%d/druid/worker/v1/shuffle/task/%s/partition?%s&%s&%d",
                host,
                port,
                supervisorTaskId,
                interval.getStart(),
                interval.getEnd(),
                partitionId
            )
        )
    );
  }

  @Override
  public String toString()
  {
    return "PartitionLocation{" +
           "host='" + host + '\'' +
           ", port=" + port +
           ", interval=" + interval +
           ", partitionId=" + partitionId +
           '}';
  }
}
