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

package org.apache.druid.indexing.common.task;

import org.apache.druid.data.input.InputRow;
import org.apache.druid.data.input.InputRowListPlusRawValues;
import org.apache.druid.data.input.InputSourceReader;
import org.apache.druid.java.util.common.parsers.CloseableIterator;
import org.apache.druid.java.util.common.parsers.ParseException;
import org.apache.druid.segment.incremental.ParseExceptionHandler;

import java.io.IOException;
import java.util.NoSuchElementException;

public class ParseExceptionHandlingInputSourceReader implements InputSourceReader
{
  private final InputSourceReader delegateReader;
  private final ParseExceptionHandler parseExceptionHandler;

  public ParseExceptionHandlingInputSourceReader(
      InputSourceReader delegateReader,
      ParseExceptionHandler parseExceptionHandler
  )
  {
    this.delegateReader = delegateReader;
    this.parseExceptionHandler = parseExceptionHandler;
  }

  @Override
  public CloseableIterator<InputRow> read() throws IOException
  {
    final CloseableIterator<InputRow> delegate = delegateReader.read();
    return new CloseableIterator<InputRow>()
    {
      InputRow next;

      @Override
      public boolean hasNext()
      {
        while (next == null && delegate.hasNext()) {
          try {
            next = delegate.next();
          }
          catch (ParseException e) {
            parseExceptionHandler.handle(e);
          }
        }
        return next != null;
      }

      @Override
      public InputRow next()
      {
        if (!hasNext()) {
          throw new NoSuchElementException();
        }
        final InputRow row = next;
        next = null;
        return row;
      }

      @Override
      public void close() throws IOException
      {
        delegate.close();
      }
    };
  }

  @Override
  public CloseableIterator<InputRowListPlusRawValues> sample() throws IOException
  {
    final CloseableIterator<InputRowListPlusRawValues> delegate = delegateReader.sample();
    return new CloseableIterator<InputRowListPlusRawValues>()
    {
      InputRowListPlusRawValues next;

      @Override
      public boolean hasNext()
      {
        while (next == null && delegate.hasNext()) {
          try {
            next = delegate.next();
          }
          catch (ParseException e) {
            parseExceptionHandler.handle(e);
          }
        }
        return next != null;
      }

      @Override
      public InputRowListPlusRawValues next()
      {
        if (!hasNext()) {
          throw new NoSuchElementException();
        }
        final InputRowListPlusRawValues row = next;
        next = null;
        return row;
      }

      @Override
      public void close() throws IOException
      {
        delegate.close();
      }
    };
  }
}
