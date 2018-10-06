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

import com.google.common.base.Preconditions;

import java.util.Collections;
import java.util.List;
import java.util.Objects;

public class OvershadowableInteger implements Overshadowable<OvershadowableInteger>
{
  private final int chunkNumber;
  private final int val;
  private final List<Integer> overshadowedGroup;
  private final List<Integer> atomicUpdateGroup;

  public OvershadowableInteger(int chunkNumber, int val)
  {
    this(chunkNumber, val, Collections.emptyList(), Collections.singletonList(chunkNumber));
  }

  public OvershadowableInteger(int chunkNumber, int val, List<Integer> overshadowedGroup, List<Integer> atomicUpdateGroup)
  {
    Preconditions.checkState(atomicUpdateGroup.contains(chunkNumber), "atomicUpdateGroup should contain the chunkNumer of itself");
    this.chunkNumber = chunkNumber;
    this.val = val;
    this.overshadowedGroup = overshadowedGroup;
    this.atomicUpdateGroup = atomicUpdateGroup;
  }

  @Override
  public List<Integer> getOvershadowedGroup()
  {
    return overshadowedGroup;
  }

  @Override
  public List<Integer> getAtomicUpdateGroup()
  {
    return atomicUpdateGroup;
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
    OvershadowableInteger that = (OvershadowableInteger) o;
    return chunkNumber == that.chunkNumber &&
           val == that.val &&
           Objects.equals(overshadowedGroup, that.overshadowedGroup) &&
           Objects.equals(atomicUpdateGroup, that.atomicUpdateGroup);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(chunkNumber, val, overshadowedGroup, atomicUpdateGroup);
  }

  @Override
  public String toString()
  {
    return "OvershadowableInteger{" +
           "chunkNumber=" + chunkNumber +
           ", val=" + val +
           ", overshadowedGroup=" + overshadowedGroup +
           ", atomicUpdateGroup=" + atomicUpdateGroup +
           '}';
  }
}
