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

import org.apache.druid.data.input.impl.ParseSpec;
import org.apache.druid.java.util.common.parsers.ParseException;

import javax.annotation.Nullable;
import java.io.File;
import java.io.IOException;
import java.util.stream.Stream;

public interface FirehoseFactory2<T>
{
  boolean isSplittable();

  Stream<InputSplit<T>> getSplits(@Nullable SplitHintSpec splitHintSpec) throws IOException;

  int getNumSplits(@Nullable SplitHintSpec splitHintSpec) throws IOException;

  FirehoseFactory2<T> withSplit(InputSplit<T> split);

  FirehoseV2 connect(ParseSpec parseSpec, @Nullable File temporaryDirectory) throws IOException, ParseException;

  SamplingFirehose sample(ParseSpec parseSpec, @Nullable File temporaryDirectory) throws IOException, ParseException;
}
