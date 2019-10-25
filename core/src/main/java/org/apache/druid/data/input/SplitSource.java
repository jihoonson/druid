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

import com.google.common.io.ByteStreams;
import com.google.common.io.Files;
import org.apache.druid.java.util.common.FileUtils;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.Optional;

public interface SplitSource<T>
{
  interface CleanableFile
  {
    File file();

    void cleanup();
  }

  default CleanableFile fetch(File temporaryDirectory) throws IOException
  {
    // call open, write to temporaryDirectory, return it along with a cleanup method that deletes it

    final File tempFile = File.createTempFile("druid-split", ".tmp", temporaryDirectory);
    FileUtils.copyLarge(getSplit().get(), )

    return new CleanableFile()
    {
      @Override
      public File file()
      {
        return tempFile;
      }

      @Override
      public void cleanup()
      {
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
}
