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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.ints.IntList;

import java.util.BitSet;
import java.util.Collections;
import java.util.Objects;
import java.util.function.IntConsumer;

public class AtomicUpdateGroup
{
  private static final BitSet SINGLE_TRUE;

  static {
    SINGLE_TRUE = new BitSet(1);
    SINGLE_TRUE.set(0);
  }

  private final int startPartitionId;
  private final BitSet partitions;

  public static AtomicUpdateGroup singleton(int partitionId)
  {
    return new AtomicUpdateGroup(partitionId, SINGLE_TRUE);
  }

  public static Builder builder()
  {
    return new Builder();
  }

  @JsonCreator
  public AtomicUpdateGroup(
      @JsonProperty("startPartitionId") int startPartitionId,
      @JsonProperty("partitions") BitSet partitions
  )
  {
    this.startPartitionId = startPartitionId;
    this.partitions = partitions;
  }

  @JsonProperty
  public int getStartPartitionId()
  {
    return startPartitionId;
  }

  @JsonProperty
  public BitSet getPartitions()
  {
    return partitions;
  }

  public boolean isEmpty()
  {
    return partitions.isEmpty();
  }

  public boolean contains(int i)
  {
    return partitions.get(i - startPartitionId);
  }

  @Override
  public boolean equals(Object o)
  {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    AtomicUpdateGroup that = (AtomicUpdateGroup) o;
    return startPartitionId == that.startPartitionId &&
           Objects.equals(partitions, that.partitions);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(startPartitionId, partitions);
  }

  @Override
  public String toString()
  {
    return "AtomicUpdateGroup{" +
           "startPartitionId=" + startPartitionId +
           ", partitions=" + partitions +
           '}';
  }

  public static class Builder
  {
    private final IntList ints = new IntArrayList();

    public Builder add(int i)
    {
      this.ints.add(i);
      return this;
    }

    public AtomicUpdateGroup build()
    {
      Collections.sort(ints);
      final int startPartitionId = ints.getInt(0);
      final BitSet partitions = new BitSet(ints.size());
      ints.forEach((IntConsumer) i -> partitions.set(i - startPartitionId));

      return new AtomicUpdateGroup(startPartitionId, partitions);
    }
  }
}
