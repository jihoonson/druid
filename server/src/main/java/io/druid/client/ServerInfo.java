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

package io.druid.client;

import io.druid.server.coordination.DruidServerMetadata;
import io.druid.server.coordination.ServerType;
import io.druid.timeline.DataSegment;

import java.util.Collection;
import java.util.Map;

public interface ServerInfo<T>
{
  default String getName()
  {
    return getMetadata().getName();
  }

  DruidServerMetadata getMetadata();

  default String getHost()
  {
    return getMetadata().getHost();
  }

  long getCurrSize();

  default long getMaxSize()
  {
    return getMetadata().getMaxSize();
  }

  default ServerType getType()
  {
    return getMetadata().getType();
  }

  default String getTier()
  {
    return getMetadata().getTier();
  }

  default int getPriority()
  {
    return getMetadata().getPriority();
  }

  default DataSegment getSegment(String segmentName)
  {
    return getSegments().get(segmentName);
  }

  Collection<T> getDataSources();

  T getDataSource(String name);

  Map<String, DataSegment> getSegments();
}
