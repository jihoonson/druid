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

package org.apache.druid.timeline.partition;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import nl.jqno.equalsverifier.EqualsVerifier;
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
    values = new String[]{"a", "dup", "dup", "k", "z"};
    expected = new String[]{null, "dup", "k", null};
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
  public void serializesDeserializes() throws JsonProcessingException
  {
    final ObjectMapper objectMapper = new ObjectMapper();
    String serialized = objectMapper.writeValueAsString(target);
    Object deserialized = objectMapper.readValue(serialized, target.getClass());
    Assert.assertEquals(serialized, objectMapper.writeValueAsString(deserialized));
  }

  @Test
  public void testGetNumBucketsOfNonEmptyPartitionBoundariesReturningCorrectSize()
  {
    Assert.assertEquals(3, target.getNumBuckets());
  }

  @Test
  public void testEqualsContract()
  {
    EqualsVerifier.forClass(PartitionBoundaries.class).withNonnullFields("boundaries").usingGetClass().verify();
  }

  @Test
  public void testBucketForNullKeyReturningFirstBucket()
  {
    Assert.assertEquals(0, target.bucketFor(null));
  }

  @Test
  public void testBucketForNonNullKeysReturningProperBuckets()
  {
    Assert.assertEquals(0, target.bucketFor("b"));
    Assert.assertEquals(0, target.bucketFor("bbb"));
    Assert.assertEquals(1, target.bucketFor("dup"));
    Assert.assertEquals(1, target.bucketFor("e"));
    Assert.assertEquals(2, target.bucketFor("k"));
    Assert.assertEquals(2, target.bucketFor("z"));
  }
}
