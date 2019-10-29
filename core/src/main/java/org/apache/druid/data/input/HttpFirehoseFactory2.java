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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import org.apache.druid.data.input.impl.HttpSource;
import org.apache.druid.data.input.impl.ParseSpec;
import org.apache.druid.data.input.impl.SplitIteratingFirehose;
import org.apache.druid.java.util.common.parsers.ParseException;
import org.apache.druid.metadata.PasswordProvider;

import javax.annotation.Nullable;
import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.util.Collections;
import java.util.List;
import java.util.stream.Stream;

public class HttpFirehoseFactory2 implements FirehoseFactory2<URI>
{
  private final List<URI> uris;
  @Nullable
  private final String httpAuthenticationUsername;
  @Nullable
  private final PasswordProvider httpAuthenticationPasswordProvider;

  @JsonCreator
  public HttpFirehoseFactory2(
      @JsonProperty("uris") List<URI> uris,
      @JsonProperty("httpAuthenticationUsername") @Nullable String httpAuthenticationUsername,
      @JsonProperty("httpAuthenticationPassword") @Nullable PasswordProvider httpAuthenticationPasswordProvider
  )
  {
    Preconditions.checkArgument(uris.size() > 0, "Empty URIs");
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
  public Stream<InputSplit<URI>> getSplits(@Nullable SplitHintSpec splitHintSpec)
  {
    return uris.stream().map(InputSplit::new);
  }

  @Override
  public int getNumSplits(@Nullable SplitHintSpec splitHintSpec)
  {
    return uris.size();
  }

  @Override
  public FirehoseFactory2<URI> withSplit(InputSplit<URI> split)
  {
    return new HttpFirehoseFactory2(
        Collections.singletonList(split.get()),
        httpAuthenticationUsername,
        httpAuthenticationPasswordProvider
    );
  }

  @Override
  public FirehoseV2 connect(ParseSpec parseSpec, @Nullable File temporaryDirectory) throws IOException, ParseException
  {
    return new SplitIteratingFirehose<>(
        parseSpec,
        getSplits(null).map(split -> new HttpSource(split, httpAuthenticationUsername, httpAuthenticationPasswordProvider))
    );
  }

  @Override
  public SamplingFirehose sample(ParseSpec parseSpec, @Nullable File temporaryDirectory)
      throws IOException, ParseException
  {
    return null;
  }
}
