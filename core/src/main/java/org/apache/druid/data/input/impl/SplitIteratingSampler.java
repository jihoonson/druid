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

package org.apache.druid.data.input.impl;

import org.apache.druid.data.input.InputRowPlusRaw;
import org.apache.druid.data.input.InputSourceSampler;
import org.apache.druid.data.input.SplitSampler;
import org.apache.druid.data.input.SplitSource;
import org.apache.druid.java.util.common.parsers.CloseableIterator;

import java.io.File;
import java.io.IOException;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.stream.Stream;

public class SplitIteratingSampler<T> implements InputSourceSampler
{
  private final TimestampSpec timestampSpec;
  private final DimensionsSpec dimensionsSpec;
  private final InputFormat inputFormat;
  private final Iterator<SplitSource<T>> sourceIterator;
  private final File temporaryDirectory;

  public SplitIteratingSampler(
      TimestampSpec timestampSpec,
      DimensionsSpec dimensionsSpec,
      InputFormat inputFormat,
      Stream<SplitSource<T>> sourceStream,
      File temporaryDirectory
  )
  {
    this.timestampSpec = timestampSpec;
    this.dimensionsSpec = dimensionsSpec;
    this.inputFormat = inputFormat;
    this.sourceIterator = sourceStream.iterator();
    this.temporaryDirectory = temporaryDirectory;
  }

  @Override
  public CloseableIterator<InputRowPlusRaw> sample()
  {
    return new CloseableIterator<InputRowPlusRaw>()
    {
      CloseableIterator<InputRowPlusRaw> rowIterator = null;

      @Override
      public boolean hasNext()
      {
        checkRowIterator();
        return rowIterator != null && rowIterator.hasNext();
      }

      @Override
      public InputRowPlusRaw next()
      {
        if (!hasNext()) {
          throw new NoSuchElementException();
        }
        return rowIterator.next();
      }

      private void checkRowIterator()
      {
        if (rowIterator == null || !rowIterator.hasNext()) {
          try {
            if (rowIterator != null) {
              rowIterator.close();
            }
            if (sourceIterator.hasNext()) {
              // SplitSampler is stateful and so a new one should be created per split.
              final SplitSampler splitSampler = inputFormat.createSampler(timestampSpec, dimensionsSpec);
              rowIterator = splitSampler.sample(sourceIterator.next(), temporaryDirectory);
            }
          }
          catch (IOException e) {
            throw new RuntimeException(e);
          }
        }
      }

      @Override
      public void close() throws IOException
      {
        if (rowIterator != null) {

        }
      }
    };
  }
}
