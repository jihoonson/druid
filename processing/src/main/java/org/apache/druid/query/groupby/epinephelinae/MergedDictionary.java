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

package org.apache.druid.query.groupby.epinephelinae;

import com.google.common.annotations.VisibleForTesting;
import it.unimi.dsi.fastutil.ints.Int2IntMap;

import javax.annotation.Nullable;
import java.util.List;

// TODO: size limit
public class MergedDictionary
{
  // segmentId -> original dict id -> new dict id
  private final int[][] dictIdConversion;
  private final String[] dictionary;

  public MergedDictionary(Int2IntMap[] dictIdConversion, List<String> dictionary)
  {
    this.dictIdConversion = new int[dictIdConversion.length][];
    for (int i = 0; i < dictIdConversion.length; i++) {
      this.dictIdConversion[i] = new int[dictIdConversion[i].size()];
      final int[] localVar = this.dictIdConversion[i];
      dictIdConversion[i].forEach((k, v) -> localVar[k] = v);
    }
    this.dictionary = dictionary.toArray(new String[0]);

//    for (int[] c : this.dictIdConversion) {
//      System.err.print(Arrays.toString(c));
//    }
//    System.err.println();
  }

  public int getNewDictId(int segmentId, int originalDictId)
  {
    return dictIdConversion[segmentId][originalDictId];
  }

  @Nullable
  public String lookup(int newDictId)
  {
//    System.err.println("dict len : " + dictionary.length + " new dict id: " + newDictId);
    return newDictId < 0 ? null : dictionary[newDictId];
  }

  public int size()
  {
    return dictionary.length;
  }

  @VisibleForTesting
  public String[] getDictionary()
  {
    return dictionary;
  }

  @VisibleForTesting
  public int[] getDictionaryConversion(int segmentId)
  {
    return dictIdConversion[segmentId];
  }
}
