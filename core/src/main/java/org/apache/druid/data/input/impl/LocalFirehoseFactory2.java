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

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.filefilter.TrueFileFilter;
import org.apache.commons.io.filefilter.WildcardFileFilter;
import org.apache.druid.data.input.FirehoseFactory2;
import org.apache.druid.data.input.FirehoseV2;
import org.apache.druid.data.input.InputSplit;
import org.apache.druid.data.input.SamplingFirehose;
import org.apache.druid.data.input.SplitHintSpec;
import org.apache.druid.java.util.common.parsers.ParseException;

import javax.annotation.Nullable;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Collection;
import java.util.stream.Stream;

public class LocalFirehoseFactory2 implements FirehoseFactory2<File>
{
  private final File baseDir;
  private final String filter;

  private Collection<File> files;

  public LocalFirehoseFactory2(
      @JsonProperty("baseDir") File baseDir,
      @JsonProperty("filter") String filter
  )
  {
    this.baseDir = baseDir;
    this.filter = filter;
  }

  @Override
  public boolean isSplittable()
  {
    return true;
  }

  @Override
  public Stream<InputSplit<File>> getSplits(@Nullable SplitHintSpec splitHintSpec)
  {
    checkFilesInitialized();
    return files.stream().map(InputSplit::new);
  }

  @Override
  public int getNumSplits(@Nullable SplitHintSpec splitHintSpec) throws IOException
  {
    checkFilesInitialized();
    return files.size();
  }

  private void checkFilesInitialized()
  {
    if (files == null) {
      // TODO: we can check the file format is splittable in the future such as
      // if (parseSpec.isSplittable()) {
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
  public FirehoseFactory2<File> withSplit(InputSplit<File> split)
  {
    final LocalFirehoseFactory2 newFactory = new LocalFirehoseFactory2(null, null);
    newFactory.files = ImmutableList.of(split.get());
    return newFactory;
  }

  @Override
  public FirehoseV2 connect(ParseSpec parseSpec, @Nullable File temporaryDirectory) throws ParseException
  {
    return new SplitIteratingFirehose<>(
        parseSpec,
        getSplits(null).map(split -> {
          try {
            return new FileSource(split);
          }
          catch (FileNotFoundException e) {
            throw new RuntimeException(e);
          }
        })
    );
  }

  @Override
  public SamplingFirehose sample(ParseSpec parseSpec, @Nullable File temporaryDirectory) throws IOException, ParseException
  {
    return null;
  }
}
