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

package org.apache.druid.segment.transform;

import org.apache.druid.data.input.InputRow;
import org.apache.druid.data.input.SplitReader;
import org.apache.druid.data.input.SplitSource;
import org.apache.druid.java.util.common.parsers.CloseableIterator;

import java.io.IOException;

public class TransformingSplitReader implements SplitReader
{
  private final SplitReader reader;
  private final Transformer transformer;

  TransformingSplitReader(SplitReader reader, Transformer transformer)
  {
    this.reader = reader;
    this.transformer = transformer;
  }

  @Override
  public CloseableIterator<InputRow> read(SplitSource source) throws IOException
  {
    return new CloseableIterator<InputRow>()
    {
      private final CloseableIterator<InputRow> delegate = reader.read(source);

      @Override
      public boolean hasNext()
      {
        return delegate.hasNext();
      }

      @Override
      public InputRow next()
      {
        return transformer.transform(delegate.next());
      }

      @Override
      public void close() throws IOException
      {
        delegate.close();
      }
    };
  }
}
