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
import org.apache.druid.data.input.InputSourceReader;
import org.apache.druid.data.input.InputSplit;
import org.apache.druid.data.input.SplitHintSpec;
import org.apache.druid.metadata.PasswordProvider;

import javax.annotation.Nullable;
import java.io.File;
import java.net.URI;
import java.util.Collections;
import java.util.List;
import java.util.stream.Stream;

public class HttpInputSource implements SplittableInputSource<URI>
{
  private final List<URI> uris;
  @Nullable
  private final String httpAuthenticationUsername;
  @Nullable
  private final PasswordProvider httpAuthenticationPasswordProvider;

  @JsonCreator
  public HttpInputSource(
      @JsonProperty("uris") List<URI> uris,
      @JsonProperty("httpAuthenticationUsername") @Nullable String httpAuthenticationUsername,
      @JsonProperty("httpAuthenticationPassword") @Nullable PasswordProvider httpAuthenticationPasswordProvider
  )
  {
    Preconditions.checkArgument(!uris.isEmpty(), "Empty URIs");
    this.uris = uris;
    this.httpAuthenticationUsername = httpAuthenticationUsername;
    this.httpAuthenticationPasswordProvider = httpAuthenticationPasswordProvider;
  }

  @Override
  public boolean isSplittable()
  {
    return true;
  }

  @Override
  public Stream<InputSplit<URI>> getSplits(InputFormat inputFormat, @Nullable SplitHintSpec splitHintSpec)
  {
    return uris.stream().map(InputSplit::new);
  }

  @Override
  public int getNumSplits(InputFormat inputFormat, @Nullable SplitHintSpec splitHintSpec)
  {
    return uris.size();
  }

  @Override
  public SplittableInputSource<URI> withSplit(InputSplit<URI> split)
  {
    return new HttpInputSource(
        Collections.singletonList(split.get()),
        httpAuthenticationUsername,
        httpAuthenticationPasswordProvider
    );
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
        getSplits(inputFormat, null).map(split -> new HttpSource(
            split,
            httpAuthenticationUsername,
            httpAuthenticationPasswordProvider
        )),
        temporaryDirectory
    );
  }
}
