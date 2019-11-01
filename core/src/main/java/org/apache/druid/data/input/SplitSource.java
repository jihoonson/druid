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

package org.apache.druid.data.input;

import com.google.common.base.Predicate;
import org.apache.druid.java.util.common.FileUtils;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;

public interface SplitSource<T>
{
  int FETCH_BUFFER_SIZE = 4 * 1024;
  int MAX_FETCH_RETRY = 3;

  interface CleanableFile extends Closeable
  {
    File file();
  }

  default CleanableFile fetch(File temporaryDirectory, byte[] fetchBuffer) throws IOException
  {
    final File tempFile = File.createTempFile("druid-split", ".tmp", temporaryDirectory);
    final long copied = FileUtils.copyLarge(
        open(),
        tempFile,
        fetchBuffer,
        getRetryCondition(),
        MAX_FETCH_RETRY,
        "Failed to fetch"
    );
    // log copied?

    return new CleanableFile()
    {
      @Override
      public File file()
      {
        return tempFile;
      }

      @Override
      public void close()
      {
        // log on fail?
        tempFile.delete();
      }
    };
  }

  InputSplit<T> getSplit();

  /**
   * Basic way to read a split.
   */
  InputStream open() throws IOException;

  /**
   * This is for the future use case when we want to read a portion of a file.
   * Some storage types don't support nio, but they can just copy into the dst buffer.
   * Will not be included in the first implementation.
   */
  int read(ByteBuffer buffer, int offset, int length) throws IOException;

  Predicate<Throwable> getRetryCondition();
}
