/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Metamarkets licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.druid.query.join;

import io.druid.segment.column.ValueType;

import java.util.List;

public class AnnotatedJoinSpec extends JoinSpec
{
  private final List<String> leftInputColumnNames;
  private final List<ValueType> leftInputColumnTypes;
  private final List<String> rightInputColumnNames;
  private final List<ValueType> rightInputColumnTypes;
  private final List<String> outputColumnNames;
  private final List<ValueType> outputColumnTypes;

  public AnnotatedJoinSpec(
      JoinSpec baseSpec,
      List<String> leftInputColumnNames,
      List<ValueType> leftInputColumnTypes,
      List<String> rightInputColumnNames,
      List<ValueType> rightInputColumnTypes,
      List<String> outputColumnNames,
      List<ValueType> outputColumnTypes
  )
  {
    super(baseSpec.getJoinType(), baseSpec.getPredicate(), baseSpec.getLeft(), baseSpec.getRight());

    this.leftInputColumnNames = leftInputColumnNames;
    this.leftInputColumnTypes = leftInputColumnTypes;
    this.rightInputColumnNames = rightInputColumnNames;
    this.rightInputColumnTypes = rightInputColumnTypes;
    this.outputColumnNames = outputColumnNames;
    this.outputColumnTypes = outputColumnTypes;
  }

  public List<String> getLeftInputColumnNames()
  {
    return leftInputColumnNames;
  }

  public List<ValueType> getLeftInputColumnTypes()
  {
    return leftInputColumnTypes;
  }

  public List<String> getRightInputColumnNames()
  {
    return rightInputColumnNames;
  }

  public List<ValueType> getRightInputColumnTypes()
  {
    return rightInputColumnTypes;
  }

  public List<String> getOutputColumnNames()
  {
    return outputColumnNames;
  }

  public List<ValueType> getOutputColumnTypes()
  {
    return outputColumnTypes;
  }
}
