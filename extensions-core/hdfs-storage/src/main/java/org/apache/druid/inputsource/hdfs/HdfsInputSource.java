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

package org.apache.druid.inputsource.hdfs;

import com.fasterxml.jackson.annotation.JacksonInject;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import org.apache.druid.data.input.AbstractInputSource;
import org.apache.druid.data.input.InputFormat;
import org.apache.druid.data.input.InputRowSchema;
import org.apache.druid.data.input.InputSourceReader;
import org.apache.druid.data.input.InputSplit;
import org.apache.druid.data.input.SplitHintSpec;
import org.apache.druid.data.input.impl.InputEntityIteratingReader;
import org.apache.druid.data.input.impl.SplittableInputSource;
import org.apache.druid.java.util.common.IAE;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;

import javax.annotation.Nullable;
import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

public class HdfsInputSource extends AbstractInputSource implements SplittableInputSource<Path>
{
  private final List<String> paths;
  private final Configuration conf;

  @JsonCreator
  public HdfsInputSource(
      @JsonProperty("paths") Object paths,
      @JacksonInject Configuration conf
  )
  {
    this.paths = parseJsonStringOrList(paths);
    this.conf = conf;
    Preconditions.checkArgument(!this.paths.isEmpty(), "Empty paths");
  }

  public static List<String> parseJsonStringOrList(Object jsonObject)
  {
    if (jsonObject instanceof String) {
      return Collections.singletonList((String) jsonObject);
    } else if (jsonObject instanceof List && ((List<?>) jsonObject).stream().allMatch(x -> x instanceof String)) {
      return ((List<?>) jsonObject).stream().map(x -> (String) x).collect(Collectors.toList());
    } else {
      throw new IAE("'inputPaths' must be a string or an array of strings");
    }
  }

  @JsonProperty
  public List<String> getPaths()
  {
    return paths;
  }

  @Override
  protected InputSourceReader formattableReader(
      InputRowSchema inputRowSchema,
      InputFormat inputFormat,
      @Nullable File temporaryDirectory
  ) throws IOException
  {
    return new InputEntityIteratingReader<>(
        inputRowSchema,
        inputFormat,
        createSplits(null, null).map(split -> new HdfsSource(conf, split.get())),
        temporaryDirectory
    );
  }

  @Override
  public Stream<InputSplit<Path>> createSplits(InputFormat inputFormat, @Nullable SplitHintSpec splitHintSpec)
      throws IOException
  {
    final FileSystem fs = new Path(paths.get(0)).getFileSystem(conf);
    return paths.stream()
                .flatMap(path -> {
                  try {
                    final RemoteIterator<FileStatus> delegate = fs.listStatusIterator(fs.makeQualified(new Path(path)));
                    final Iterator<FileStatus> fileIterator = new Iterator<FileStatus>()
                    {
                      @Override
                      public boolean hasNext()
                      {
                        try {
                          return delegate.hasNext();
                        }
                        catch (IOException e) {
                          throw new RuntimeException(e);
                        }
                      }

                      @Override
                      public FileStatus next()
                      {
                        try {
                          return delegate.next();
                        }
                        catch (IOException e) {
                          throw new RuntimeException(e);
                        }
                      }
                    };
                    return StreamSupport.stream(
                        Spliterators.spliteratorUnknownSize(fileIterator, Spliterator.DISTINCT & Spliterator.NONNULL),
                        false
                    );
                  }
                  catch (IOException e) {
                    throw new RuntimeException(e);
                  }
                })
                .map(fileStatus -> new InputSplit<>(fileStatus.getPath()));
  }

  @Override
  public int getNumSplits(InputFormat inputFormat, @Nullable SplitHintSpec splitHintSpec) throws IOException
  {
    final FileSystem fs = new Path(paths.get(0)).getFileSystem(conf);
    int numSplits = 0;
    for (String path : paths) {
      final RemoteIterator<FileStatus> iterator = fs.listStatusIterator(new Path(path));
      while (iterator.hasNext()) {
        iterator.next();
        numSplits++;
      }
    }

    return numSplits;
  }

  @Override
  public SplittableInputSource<Path> withSplit(InputSplit<Path> split)
  {
    return new HdfsInputSource(split.get().toString(), conf);
  }

  @Override
  public boolean needsFormat()
  {
    return true;
  }
}
