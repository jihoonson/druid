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

package io.druid.query.groupby.epinephelinae;

import it.unimi.dsi.fastutil.ints.IntComparator;
import org.junit.Assert;
import org.junit.Test;

import java.util.concurrent.ThreadLocalRandom;

import static org.junit.Assert.*;

public class IntTimSortTest
{
  @Test
  public void testIndirectSort() throws Exception
  {
    final ThreadLocalRandom random = ThreadLocalRandom.current();
    final int numVals = 100_000;
    final int valueCardinality = 1000;

    final int[] indexes = new int[numVals];
    final int[] values = new int[numVals];

    for (int i = 0; i < numVals; i++) {
      indexes[i] = i;
      values[i] = random.nextInt(valueCardinality);
    }

    IntTimSort.sort(indexes, new IntComparator()
    {
      @Override
      public int compare(int k1, int k2)
      {
        return Integer.compare(values[k1], values[k2]);
      }

      @Override
      public int compare(Integer o1, Integer o2)
      {
        return compare(o1.intValue(), o2.intValue());
      }
    });

    for (int i = 0; i < numVals - 1; i++) {
      Assert.assertTrue(values[indexes[i]] <= values[indexes[i + 1]]);
    }
  }
}
