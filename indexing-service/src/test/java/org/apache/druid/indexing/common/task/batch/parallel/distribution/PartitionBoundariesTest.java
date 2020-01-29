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

package org.apache.druid.indexing.common.task.batch.parallel.distribution;

import org.apache.druid.segment.TestHelper;
import org.apache.druid.timeline.partition.PartitionBoundaries;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class PartitionBoundariesTest
{
  private PartitionBoundaries target;
  private String[] values;
  private String[] expected;

  @Before
  public void setup()
  {
    values = new String[]{"a", "dup", "dup", "z"};
    expected = new String[]{null, "dup", null};
    target = PartitionBoundaries.fromNonNullBoundaries(values);
  }

  @Test
  public void hasCorrectValues()
  {
    Assert.assertArrayEquals(expected, target.getBoundaries());
  }

  @Test
  public void cannotBeIndirectlyModified()
  {
    values[1] = "changed";
    Assert.assertArrayEquals(expected, target.getBoundaries());
  }

  @Test
  public void handlesNoValues()
  {
    Assert.assertArrayEquals(new String[0], PartitionBoundaries.empty().getBoundaries());
  }

  @Test
  public void handlesRepeatedValue()
  {
    Assert.assertArrayEquals(
        new String[]{null, null},
        PartitionBoundaries.fromNonNullBoundaries(new String[]{"a", "a", "a"}).getBoundaries()
    );
  }

  @Test
  public void serializesDeserializes()
  {
    TestHelper.testSerializesDeserializes(TestHelper.JSON_MAPPER, target);
  }
}
