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

package org.apache.druid.query.groupby;

import javax.annotation.Nullable;
import java.util.Arrays;

public class PerSegmentEncodedResultRow extends ResultRow
{
  private final int[] segmentIds;

  private PerSegmentEncodedResultRow(final int[] segmentIds, final Object[] row)
  {
    super(row);
    this.segmentIds = segmentIds;
    assert segmentIds.length == row.length;
  }

  public static PerSegmentEncodedResultRow create(final int size)
  {
    return new PerSegmentEncodedResultRow(new int[size], new Object[size]);
  }

  public int[] getSegmentIds()
  {
    return segmentIds;
  }

//  @Override
//  public void set(final int i, @Nullable final Object o)
//  {
//    throw new UnsupportedOperationException();
//  }

  @Override
  public int getInt(final int i)
  {
    // TODO Virtual column sometimes returns double....
    assert super.get(i) != null && (super.get(i) instanceof Number);
    return ((Number) super.get(i)).intValue();
  }

  public void set(final int i, final int segmentId, @Nullable final Object val)
  {
    this.segmentIds[i] = segmentId;
    super.set(i, val);
  }

  public int getSegmentId(final int i)
  {
    return segmentIds[i];
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
    if (!super.equals(o)) {
      return false;
    }
    PerSegmentEncodedResultRow that = (PerSegmentEncodedResultRow) o;
    return Arrays.equals(segmentIds, that.segmentIds);
  }

  @Override
  public int hashCode()
  {
    int result = super.hashCode();
    result = 31 * result + Arrays.hashCode(segmentIds);
    return result;
  }

  @Override
  public String toString()
  {
    return "PerSegmentResultRow{" +
           "segmentIds=" + Arrays.toString(segmentIds) +
           ", row=" + Arrays.toString(getArray()) +
           '}';
  }
}
