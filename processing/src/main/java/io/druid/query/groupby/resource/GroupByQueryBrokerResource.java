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

package io.druid.query.groupby.resource;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.metamx.emitter.EmittingLogger;
import io.druid.collections.ResourceHolder;

import java.io.Closeable;
import java.nio.ByteBuffer;
import java.util.List;

/**
 * This class contains all resources required by the Broker during executing a group-by query.
 * Currently, it contains only merge buffers, but any additional resources can be added in the future.
 */
public class GroupByQueryBrokerResource implements Closeable
{
  private static final EmittingLogger log = new EmittingLogger(GroupByQueryBrokerResource.class);

  private final ResourceHolder<List<ByteBuffer>> mergeBuffersHolder;
  private final List<ByteBuffer> mergeBuffers;

  public GroupByQueryBrokerResource(ResourceHolder<List<ByteBuffer>> mergeBuffersHolder)
  {
    this.mergeBuffersHolder = mergeBuffersHolder;
    this.mergeBuffers = Lists.newArrayList(mergeBuffersHolder.get());
  }

  public ResourceHolder<ByteBuffer> getMergeBuffer()
  {
    Preconditions.checkState(mergeBuffers.size() > 0);
    final ByteBuffer buffer = mergeBuffers.remove(mergeBuffers.size() - 1);
//    log.info("take " + System.identityHashCode(buffer));
    return new ResourceHolder<ByteBuffer>()
    {
      @Override
      public ByteBuffer get()
      {
        return buffer;
      }

      @Override
      public void close()
      {
        mergeBuffers.add(buffer);
//        log.info("close " + System.identityHashCode(buffer));
      }
    };
  }

  @Override
  public void close()
  {
    if (mergeBuffers.size() != mergeBuffersHolder.get().size()) {
      log.warn((mergeBuffersHolder.get().size() - mergeBuffers.size()) + " resources are not returned yet");
    }
    mergeBuffersHolder.close();
  }
}
