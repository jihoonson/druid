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

package io.druid.indexing.common.actions;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.druid.indexing.common.TaskLock;
import io.druid.indexing.common.TaskLockType;
import io.druid.jackson.DefaultObjectMapper;
import io.druid.java.util.common.Intervals;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;

public class SurrogateActionTest
{
  @Test
  public void testSerde() throws IOException
  {
    final ObjectMapper objectMapper = new DefaultObjectMapper();
    final SurrogateAction<TaskLock, LockTryAcquireAction> surrogateAction = new SurrogateAction<>(
        "testId", new LockTryAcquireAction(TaskLockType.EXCLUSIVE, Intervals.of("2018-01-01/2019-01-01"))
    );

    final String json = objectMapper.writeValueAsString(surrogateAction);
    Assert.assertEquals(surrogateAction.toString(), objectMapper.readValue(json, TaskAction.class).toString());
  }
}
