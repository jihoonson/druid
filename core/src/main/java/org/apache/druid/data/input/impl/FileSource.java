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

import com.google.common.base.Predicate;
import com.google.common.base.Predicates;
import org.apache.druid.data.input.InputSplit;
import org.apache.druid.data.input.SplitSource;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.FileChannel;

public class FileSource implements SplitSource<File>
{
  private final InputSplit<File> split;
  private final RandomAccessFile file;
  private final FileChannel channel;

  public FileSource(InputSplit<File> split) throws FileNotFoundException
  {
    this.split = split;
    this.file = new RandomAccessFile(split.get(), "r");
    this.channel = this.file.getChannel();
  }

  public FileSource(File file, long start, long length) throws IOException
  {
    this.split = new InputSplit<>(file, start, length);
    this.file = new RandomAccessFile(file, "r");
    this.channel = this.file.getChannel();
    this.channel.position(start);
  }

  @Override
  public CleanableFile fetch(File temporaryDirectory, byte[] fetchBuffer)
  {
    return new CleanableFile()
    {
      @Override
      public File file()
      {
        return split.get();
      }

      @Override
      public void cleanup()
      {
        // do nothing
      }
    };
  }

  @Override
  public InputSplit<File> getSplit()
  {
    return split;
  }

  @Override
  public InputStream open() throws FileNotFoundException
  {
    return Channels.newInputStream(channel);
  }

  @Override
  public int read(ByteBuffer buffer, int offset, int length) throws IOException
  {
    buffer.position(0);
    buffer.limit(length);
    return channel.read(buffer, offset);
  }

  @Override
  public Predicate<Throwable> getRetryCondition()
  {
    return Predicates.alwaysFalse();
  }
}
