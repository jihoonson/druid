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

import org.apache.druid.data.input.InputRow;
import org.apache.druid.data.input.InputRowReader;
import org.apache.druid.data.input.NonTextFileReader;
import org.apache.druid.data.input.FirehoseV2;
import org.apache.druid.data.input.TextFileReader;
import org.apache.druid.java.util.common.parsers.CloseableIterator;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;

public class FileSource implements FirehoseV2
{
  private final File file;

  public FileSource(File file) throws FileNotFoundException
  {
    this.file = file;
  }

  @Override
  public CloseableIterator<InputRow> read(InputRowReader reader) throws IOException
  {
    if (reader.isTextFormat()) {
      return readText((TextFileReader) reader);
    } else {
      return ((NonTextFileReader) reader).read(file.getPath());
    }
  }

  private CloseableIterator<InputRow> readText(TextFileReader reader) throws IOException
  {
    final RandomAccessFile randomAccessFile = new RandomAccessFile(file, "r");
    return new CloseableIterator<InputRow>()
    {
      String next = randomAccessFile.readLine();

      @Override
      public boolean hasNext()
      {
        return next != null;
      }

      @Override
      public InputRow next()
      {
        try {
          final InputRow row = reader.read(next);
          next = randomAccessFile.readLine();
          return row;
        }
        catch (IOException e) {
          throw new RuntimeException(e);
        }
      }

      @Override
      public void close() throws IOException
      {
        randomAccessFile.close();
      }
    };
  }
}
