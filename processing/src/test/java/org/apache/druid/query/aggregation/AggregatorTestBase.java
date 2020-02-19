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

package org.apache.druid.query.aggregation;

import org.apache.druid.data.gen.TestColumnSchema;
import org.apache.druid.data.gen.TestDataGenerator;
import org.apache.druid.data.input.impl.DimensionSchema.ValueType;

import java.util.ArrayList;
import java.util.List;

public class AggregatorTestBase
{
  // float, double, long, single-valued string, multi-valued string w/o nulls
  // custom column value selector for complex type

  // get as dimension selector
  // get as column value selector

  public AggregatorTestBase(int numRows, double nullRatio)
  {
    final List<TestColumnSchema> columnSchemas = new ArrayList<>();
    columnSchemas.add(
        TestColumnSchema.makeSequential(
            "floatsWithoutNulls",
            ValueType.FLOAT,
            false,
            1,
            nullRatio,
            -(numRows / 2),
            numRows / 2
        )
    );
  }
}
