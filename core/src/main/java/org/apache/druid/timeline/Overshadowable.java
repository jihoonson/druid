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

package org.apache.druid.timeline;

public interface Overshadowable<T extends Overshadowable>
{
  default boolean isOvershadow(T other)
  {
    return containsRootPartition(other) && getMinorVersion() > other.getMinorVersion();
  }

  default boolean containsRootPartition(T other)
  {
    return getStartRootPartitionId() <= other.getStartRootPartitionId()
        && getEndRootPartitionId() >= other.getEndRootPartitionId();
  }

  int getStartRootPartitionId();

  int getEndRootPartitionId();

  default short getStartRootPartitionIdAsShort()
  {
    return (short) getStartRootPartitionId();
  }

  default short getEndRootPartitionIdAsShort()
  {
    return (short) getEndRootPartitionId();
  }

  short getMinorVersion();

  short getAtomicUpdateGroupSize();
}
