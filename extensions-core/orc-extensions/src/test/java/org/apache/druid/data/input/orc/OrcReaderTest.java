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

package org.apache.druid.data.input.orc;

import com.google.common.collect.ImmutableList;
import org.apache.druid.data.input.InputRow;
import org.apache.druid.data.input.InputRowSchema;
import org.apache.druid.data.input.impl.DimensionsSpec;
import org.apache.druid.data.input.impl.FileSource;
import org.apache.druid.data.input.impl.TimestampSpec;
import org.apache.druid.java.util.common.parsers.CloseableIterator;
import org.apache.druid.java.util.common.parsers.JSONPathSpec;
import org.apache.hadoop.conf.Configuration;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;
import java.util.Collections;

public class OrcReaderTest
{
  @Rule
  public TemporaryFolder temporaryFolder = new TemporaryFolder();

  @Test
  public void test() throws IOException
  {
    final Configuration conf = new Configuration();
    OrcReader reader = new OrcReader(
        conf,
        new InputRowSchema(
            new TimestampSpec("l_shipdate", "yyyy-MM-dd", null),
            new DimensionsSpec(
                DimensionsSpec.getDefaultSchemas(
                    ImmutableList.of(
                        "l_orderkey",
                        "l_partkey",
                        "l_suppkey",
                        "l_linenumber",
                        "l_returnflag",
                        "l_linestatus",
                        "l_shipdate",
                        "l_commitdate",
                        "l_receiptdate",
                        "l_shipinstruct",
                        "l_shipmode",
                        "l_comment"
                    )
                )
            ),
            Collections.emptyList()
        ),
        new JSONPathSpec(true, null),
        true
    );

    try (CloseableIterator<InputRow> iterator = reader.read(new FileSource(new File("/Users/jihoonson/Downloads/tpch-2.17.3/tables/lineitem-orc/lineitem-apache.orc"), 0, 83_886_080), temporaryFolder.newFolder())) {
      int n = 0;
      while (iterator.hasNext()) {
//        System.out.println(iterator.next());
        iterator.next();
        n++;
      }
      System.out.println("n: " + n);
    }

    reader = new OrcReader(
        conf,
        new InputRowSchema(
            new TimestampSpec("l_shipdate", "yyyy-MM-dd", null),
            new DimensionsSpec(
                DimensionsSpec.getDefaultSchemas(
                    ImmutableList.of(
                        "l_orderkey",
                        "l_partkey",
                        "l_suppkey",
                        "l_linenumber",
                        "l_returnflag",
                        "l_linestatus",
                        "l_shipdate",
                        "l_commitdate",
                        "l_receiptdate",
                        "l_shipinstruct",
                        "l_shipmode",
                        "l_comment"
                    )
                )
            ),
            Collections.emptyList()
        ),
        new JSONPathSpec(true, null),
        true
    );

    try (CloseableIterator<InputRow> iterator = reader.read(new FileSource(new File("/Users/jihoonson/Downloads/tpch-2.17.3/tables/lineitem-orc/lineitem-apache.orc"), 83_886_080, Long.MAX_VALUE), temporaryFolder.newFolder())) {
      int n = 0;
      while (iterator.hasNext()) {
//        System.out.println(iterator.next());
        iterator.next();
        n++;
      }
      System.out.println("n: " + n);
    }
  }
}
