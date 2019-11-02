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

import com.google.common.base.Preconditions;
import org.apache.druid.data.input.impl.DimensionsSpec;
import org.apache.druid.data.input.impl.FirehoseToInputSourceReaderAdaptor;
import org.apache.druid.data.input.impl.FirehoseToInputSourceSamplerAdaptor;
import org.apache.druid.data.input.impl.InputFormat;
import org.apache.druid.data.input.impl.InputRowParser;
import org.apache.druid.data.input.impl.TimestampSpec;
import org.apache.druid.java.util.common.parsers.ParseException;

import javax.annotation.Nullable;
import java.io.File;
import java.io.IOException;

public class FirehoseFactoryToInputSourceAdaptor implements InputSource
{
  private final FirehoseFactory firehoseFactory;
  private final InputRowParser inputRowParser;

  public FirehoseFactoryToInputSourceAdaptor(FirehoseFactory firehoseFactory, InputRowParser inputRowParser)
  {
    this.firehoseFactory = firehoseFactory;
    this.inputRowParser = Preconditions.checkNotNull(inputRowParser, "inputRowParser");
  }

  @Override
  public InputSourceReader reader(
      TimestampSpec timestampSpec,
      DimensionsSpec dimensionsSpec,
      InputFormat inputFormat,
      @Nullable File temporaryDirectory
  ) throws IOException, ParseException
  {
    return new FirehoseToInputSourceReaderAdaptor(firehoseFactory.connect(inputRowParser, temporaryDirectory));
  }

  @Override
  public InputSourceSampler sampler(
      TimestampSpec timestampSpec,
      DimensionsSpec dimensionsSpec,
      InputFormat inputFormat,
      @Nullable File temporaryDirectory
  ) throws IOException, ParseException
  {
    return new FirehoseToInputSourceSamplerAdaptor(
        firehoseFactory.connectForSampler(inputRowParser, temporaryDirectory)
    );
  }
}
