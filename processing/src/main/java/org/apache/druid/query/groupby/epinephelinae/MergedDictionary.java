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

import it.unimi.dsi.fastutil.ints.Int2IntMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectMap;

public class MergedDictionary
{
  // segmentId -> original dict id -> new dict id
  private final int[][] dictIdConversion;
  private final String[] dictionary;

  public MergedDictionary(Int2IntMap[] dictIdConversion, Int2ObjectMap<String> dictionary, int maxDictId)
  {
    this.dictIdConversion = new int[dictIdConversion.length][];
    for (int i = 0; i < dictIdConversion.length; i++) {
      this.dictIdConversion[i] = new int[dictIdConversion[i].size()];
      final int[] localVar = this.dictIdConversion[i];
      dictIdConversion[i].forEach((k, v) -> localVar[k] = v);
    }
    this.dictionary = new String[maxDictId + 1];
    dictionary.forEach((k, v) -> this.dictionary[k] = v);
  }

  public int getNewDictId(int segmentId, int originalDictId)
  {
    return dictIdConversion[segmentId][originalDictId];
  }

  public String lookup(int newDictId)
  {
    return dictionary[newDictId];
  }

  public int size()
  {
    return dictionary.length;
  }
}
