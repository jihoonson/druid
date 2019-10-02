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

import org.apache.druid.data.input.Firehose;
import org.apache.druid.data.input.InputRow;
import org.apache.druid.java.util.common.parsers.CloseableIterator;

import javax.annotation.Nullable;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

public class FileIteratingFirehose2 implements Firehose
{
  private final InputRowReader inputRowReader;
  private final Iterator<File> fileIterator;

  private CloseableIterator<InputRow> rowIterator;

  public FileIteratingFirehose2(ParseSpec parseSpec, List<File> files)
  {
    this.inputRowReader = parseSpec.createReader();
    this.fileIterator = files.iterator();
  }

  @Override
  public boolean hasMore()
  {
    return (rowIterator != null && rowIterator.hasNext()) || fileIterator.hasNext();
  }

  @Nullable
  @Override
  public InputRow nextRow() throws IOException
  {
    if (!hasMore()) {
      throw new NoSuchElementException();
    }
    if ((rowIterator == null || !rowIterator.hasNext()) && fileIterator.hasNext()) {
      if (rowIterator != null) {
        rowIterator.close();
      }
      rowIterator = inputRowReader.read(new FileInputStream(fileIterator.next()));
    }
    if (!rowIterator.hasNext()) {
      throw new NoSuchElementException();
    }
    return rowIterator.next();
  }

  @Override
  public Runnable commit()
  {
    return null;
  }

  @Override
  public void close() throws IOException
  {
    if (rowIterator != null) {
      rowIterator.close();
    }
  }
}
