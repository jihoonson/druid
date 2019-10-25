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

package org.apache.druid.firehose.s3;

import com.amazonaws.services.s3.model.AmazonS3Exception;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectInputStream;
import org.apache.druid.data.input.InputSplit;
import org.apache.druid.data.input.SplitSource;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.storage.s3.S3Utils;
import org.apache.druid.storage.s3.ServerSideEncryptingAmazonS3;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.nio.ByteBuffer;

public class S3Source implements SplitSource
{
  private final ServerSideEncryptingAmazonS3 s3Client;
  private final InputSplit<URI> uri;
  private final byte[] buf = new byte[256];
  private S3ObjectInputStream inputStream;

  public S3Source(ServerSideEncryptingAmazonS3 s3Client, URI uri)
  {
    this.s3Client = s3Client;
    this.uri = new InputSplit<>(uri);
  }

  @Override
  public InputSplit<URI> getSplit()
  {
    return uri;
  }

  @Override
  public InputStream open() throws IOException
  {
    try {
      // Get data of the given object and open an input stream
      final String bucket = uri.get().getAuthority();
      final String key = S3Utils.extractS3Key(uri.get());

      final S3Object s3Object = s3Client.getObject(bucket, key);
      if (s3Object == null) {
        throw new ISE("Failed to get an s3 object for bucket[%s] and key[%s]", bucket, key);
      }
      return s3Object.getObjectContent();
    }
    catch (AmazonS3Exception e) {
      throw new IOException(e);
    }
  }

  @Override
  public int read(ByteBuffer buffer, int offset, int length) throws IOException
  {
    if (inputStream == null) {
      inputStream = (S3ObjectInputStream) open();
    }
    int remaining = length;
    int totalReadBytes = 0;
    int readBytes = 0;
    while (remaining > 0 && readBytes != -1) {
      readBytes = inputStream.read(buf);
      if (readBytes > 0) {
        buffer.put(buf, totalReadBytes, readBytes);
        totalReadBytes += readBytes;
        remaining -= readBytes;
      }
    }
    return totalReadBytes;
  }
}
