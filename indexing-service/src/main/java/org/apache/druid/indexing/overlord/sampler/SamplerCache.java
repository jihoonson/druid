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

package org.apache.druid.indexing.overlord.sampler;

import org.apache.druid.client.cache.Cache;
import org.apache.druid.data.input.ByteBufferInputRowParser;
import org.apache.druid.data.input.Firehose;
import org.apache.druid.data.input.FirehoseFactory;
import org.apache.druid.data.input.InputEntity;
import org.apache.druid.data.input.InputEntityReader;
import org.apache.druid.data.input.InputFormat;
import org.apache.druid.data.input.InputRow;
import org.apache.druid.data.input.InputRowListPlusJson;
import org.apache.druid.data.input.impl.InputRowParser;
import org.apache.druid.java.util.common.CloseableIterators;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.parsers.CloseableIterator;
import org.apache.druid.java.util.emitter.EmittingLogger;
import org.apache.druid.segment.indexing.DataSchema;

import javax.annotation.Nullable;
import javax.inject.Inject;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.Collection;

public class SamplerCache
{
  private static final EmittingLogger log = new EmittingLogger(SamplerCache.class);
  private static final String NAMESPACE = "sampler";

  private final Cache cache;

  @Inject
  public SamplerCache(Cache cache)
  {
    this.cache = cache;
  }

  @Nullable
  public String put(String key, Collection<String> values)
  {
    if (values == null) {
      return null;
    }

    try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
         ObjectOutputStream oos = new ObjectOutputStream(baos)) {
      oos.writeObject(new ArrayList<>(values));
      cache.put(new Cache.NamedKey(NAMESPACE, StringUtils.toUtf8(key)), baos.toByteArray());
      return key;
    }
    catch (IOException e) {
      log.warn(e, "Exception while serializing to sampler cache");
      return null;
    }
  }

  @Nullable
  public FirehoseFactory getAsFirehoseFactory(String key, InputRowParser parser)
  {
    if (!(parser instanceof ByteBufferInputRowParser)) {
      log.warn("SamplerCache expects a ByteBufferInputRowParser");
      return null;
    }

    Collection<String> data = get(key);
    if (data == null) {
      return null;
    }

    return new FirehoseFactory<ByteBufferInputRowParser>()
    {
      @Override
      public Firehose connect(ByteBufferInputRowParser parser, @Nullable File temporaryDirectory)
      {
        return new SamplerCacheFirehose(parser, data);
      }
    };
  }

  @Nullable
  public InputEntityReader createCacheReader(String key, DataSchema dataSchema, InputFormat inputFormat)
  {
    Collection<String> data = get(key);
    if (data == null) {
      return null;
    }

    return new InputEntityReader()
    {
      @Override
      public CloseableIterator<InputRow> read(InputEntity<?> source, File temporaryDirectory)
      {
        throw new UnsupportedOperationException();
      }

      @Override
      public CloseableIterator<InputRowListPlusJson> sample(InputEntity<?> source, File temporaryDirectory)
      {
        return CloseableIterators.withEmptyBaggage(data.iterator()).map(json -> {

        });
      }
    };
  }

  @Nullable
  private Collection<String> get(String key)
  {
    byte[] data = cache.get(new Cache.NamedKey(NAMESPACE, StringUtils.toUtf8(key)));
    if (data == null) {
      return null;
    }

    try (ByteArrayInputStream bais = new ByteArrayInputStream(data);
         ObjectInputStream ois = new ObjectInputStream(bais)) {
      return (ArrayList) ois.readObject();
    }
    catch (Exception e) {
      log.warn(e, "Exception while deserializing from sampler cache");
      return null;
    }
  }
}
