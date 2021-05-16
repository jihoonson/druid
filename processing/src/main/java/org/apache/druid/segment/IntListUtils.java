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

package org.apache.druid.segment;

import com.google.common.base.Preconditions;
import it.unimi.dsi.fastutil.ints.AbstractIntList;
import it.unimi.dsi.fastutil.ints.IntList;

import java.util.Random;
import java.util.RandomAccess;

public class IntListUtils
{
  private static final int SHUFFLE_THRESHOLD = 5;

  private IntListUtils()
  {
  }

  public static IntList fromTo(int from, int to)
  {
    Preconditions.checkArgument(from <= to);
    return new RangeIntList(from, to);
  }

  private static final class RangeIntList extends AbstractIntList
  {
    private final int start;
    private final int size;

    RangeIntList(int start, int end)
    {
      this.start = start;
      this.size = end - start;
    }

    @Override
    public int getInt(int index)
    {
      Preconditions.checkElementIndex(index, size);
      return start + index;
    }

    @Override
    public int size()
    {
      return size;
    }
  }

  public static void shuffle(IntList list, Random rnd)
  {
    int size = list.size();
    if (size < SHUFFLE_THRESHOLD || list instanceof RandomAccess) {
      for (int i = size; i > 1; i--) {
        list.set(i - 1, list.set(rnd.nextInt(i), list.getInt(i - 1)));
      }
    } else {
      int[] arr = list.toIntArray();

      // Shuffle array
      for (int i = size; i > 1; i--) {
        int j = rnd.nextInt(i);
        int tmp = arr[i - 1];
        arr[i - 1] = arr[j];
        arr[j] = tmp;
      }

      // Dump array back into list
      // instead of using a raw type here, it's possible to capture
      // the wildcard but it will require a call to a supplementary
      // private method
      for (int i = 0; i < arr.length; i++) {
        list.set(i, arr[i]);
      }
    }
  }
}
