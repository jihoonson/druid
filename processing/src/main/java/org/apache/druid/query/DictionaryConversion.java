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

package org.apache.druid.query;

// TODO: need this per dimension
public class DictionaryConversion
{
  private final String val;
  private final int segmentId;
  private final int oldDictionaryId;
  private final int newDictionaryId;

  public DictionaryConversion(String val, int segmentId, int oldDictionaryId, int newDictionaryId)
  {
    this.val = val;
    this.segmentId = segmentId;
    this.oldDictionaryId = oldDictionaryId;
    this.newDictionaryId = newDictionaryId;
  }

  public String getVal()
  {
    return val;
  }

  public int getSegmentId()
  {
    return segmentId;
  }

  public int getOldDictionaryId()
  {
    return oldDictionaryId;
  }

  public int getNewDictionaryId()
  {
    return newDictionaryId;
  }

  @Override
  public String toString()
  {
    return "DictionaryConversion{" +
           "val='" + val + '\'' +
           ", segmentId=" + segmentId +
           ", oldDictionaryId=" + oldDictionaryId +
           ", newDictionaryId=" + newDictionaryId +
           '}';
  }
}
