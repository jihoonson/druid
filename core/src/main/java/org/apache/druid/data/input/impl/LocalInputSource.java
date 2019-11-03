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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.filefilter.TrueFileFilter;
import org.apache.commons.io.filefilter.WildcardFileFilter;
import org.apache.druid.data.input.InputSourceReader;
import org.apache.druid.data.input.InputSourceSampler;
import org.apache.druid.data.input.InputSplit;
import org.apache.druid.data.input.SplitHintSpec;

import javax.annotation.Nullable;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Collection;
import java.util.stream.Stream;

public class LocalInputSource implements SplittableInputSource<File>
{
  private final File baseDir;
  private final String filter;

  private Collection<File> files;

  @JsonCreator
  public LocalInputSource(
      @JsonProperty("baseDir") File baseDir,
      @JsonProperty("filter") String filter
  )
  {
    this.baseDir = baseDir;
    this.filter = filter;
  }

  @JsonProperty
  public File getBaseDir()
  {
    return baseDir;
  }

  @JsonProperty
  public String getFilter()
  {
    return filter;
  }

  @Override
  public boolean isSplittable()
  {
    return true;
  }

  @Override
  public Stream<InputSplit<File>> getSplits(InputFormat inputFormat, @Nullable SplitHintSpec splitHintSpec)
  {
    checkFilesInitialized(inputFormat, splitHintSpec);
    return files.stream().map(InputSplit::new);
  }

  @Override
  public int getNumSplits(InputFormat inputFormat, @Nullable SplitHintSpec splitHintSpec) throws IOException
  {
    checkFilesInitialized(inputFormat, splitHintSpec);
    return files.size();
  }

  private void checkFilesInitialized(InputFormat inputFormat, @Nullable SplitHintSpec splitHintSpec)
  {
    if (files == null) {
      // TODO: we can check the file format is splittable in the future such as
      // if (inputFormat.isSplittable()) {
      //   files = ...
      // }
      // else {
      //   files = ...
      // }
      files = FileUtils.listFiles(
          Preconditions.checkNotNull(baseDir).getAbsoluteFile(),
          new WildcardFileFilter(filter),
          TrueFileFilter.INSTANCE
      );
    }
  }

  @Override
  public SplittableInputSource<File> withSplit(InputSplit<File> split)
  {
    // TODO: fix this
    final LocalInputSource newSource = new LocalInputSource(null, null);
    newSource.files = ImmutableList.of(split.get());
    return newSource;
  }

  @Override
  public InputSourceReader reader(
      TimestampSpec timestampSpec,
      DimensionsSpec dimensionsSpec,
      InputFormat inputFormat,
      @Nullable File temporaryDirectory
  )
  {
    return new SplitIteratingReader<>(
        timestampSpec,
        dimensionsSpec,
        inputFormat,
        getSplits(inputFormat, null).map(split -> {
          try {
            return new FileSource(split);
          }
          catch (FileNotFoundException e) {
            throw new RuntimeException(e);
          }
        }),
        temporaryDirectory
    );
  }

  @Override
  public InputSourceSampler sampler(
      TimestampSpec timestampSpec,
      DimensionsSpec dimensionsSpec,
      InputFormat inputFormat,
      @Nullable File temporaryDirectory
  )
  {
    return null;
  }
}
