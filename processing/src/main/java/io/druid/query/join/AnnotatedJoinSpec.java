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

import io.druid.java.util.common.Pair;
import io.druid.query.dimension.DimensionSpec;
import it.unimi.dsi.fastutil.ints.IntList;

import java.util.ArrayList;
import java.util.List;

public class AnnotatedJoinSpec extends JoinSpec
{

  private final IntList leftKeyDimensions;
  private final IntList rightKeyDimensions;

  private final List<DimensionSpec> leftDimensions;
  private final List<DimensionSpec> rightDimensions;
  private final List<Pair<Boolean, Integer>> outputDimensions;

  public AnnotatedJoinSpec(
      JoinSpec baseSpec,
      List<DimensionSpec> leftDimensions,
      List<DimensionSpec> rightDimensions,
      IntList leftKeyDimensions,
      IntList rightKeyDimensions,
      List<Pair<Boolean, Integer>> outputDimensions
  )
  {
    super(baseSpec.getJoinType(), baseSpec.getPredicate(), baseSpec.getLeft(), baseSpec.getRight());

    this.leftDimensions = leftDimensions;
    this.rightDimensions = rightDimensions;
    this.leftKeyDimensions = leftKeyDimensions;
    this.rightKeyDimensions = rightKeyDimensions;
    this.outputDimensions = outputDimensions;
  }

  public List<Pair<Boolean, Integer>> getOutputDimensions()
  {
    return outputDimensions;
  }

  public List<DimensionSpec> getLeftDimensions()
  {
    return leftDimensions;
  }

  public List<DimensionSpec> getRightDimensions()
  {
    return rightDimensions;
  }

  public List<DimensionSpec> getLeftKeyDimensions()
  {
    final List<DimensionSpec> keyDimensions = new ArrayList<>(leftKeyDimensions.size());
    for (int i = 0; i < leftKeyDimensions.size(); i++) {
      keyDimensions.add(leftDimensions.get(leftKeyDimensions.get(i)));
    }
    return keyDimensions;
  }

  public List<DimensionSpec> getRightKeyDimensions()
  {
    final List<DimensionSpec> keyDimensions = new ArrayList<>(rightKeyDimensions.size());
    for (int i = 0; i < rightKeyDimensions.size(); i++) {
      keyDimensions.add(rightDimensions.get(rightKeyDimensions.get(i)));
    }
    return keyDimensions;
  }

//  public List<String> getLeftValueColumnNames()
//  {
//    return leftValueColumnNames;
//  }
//
//  public List<ValueType> getLeftValueColumnTypes()
//  {
//    return leftValueColumnTypes;
//  }
//
//  public List<String> getRightValueColumnNames()
//  {
//    return rightValueColumnNames;
//  }
//
//  public List<ValueType> getRightValueColumnTypes()
//  {
//    return rightValueColumnTypes;
//  }
//
//  public List<String> getOutputColumnNames()
//  {
//    return outputColumnNames;
//  }
//
//  public List<ValueType> getOutputColumnTypes()
//  {
//    return outputColumnTypes;
//  }
//
//  public List<String> getLeftKeyColumnNames()
//  {
//    return leftKeyColumnNames;
//  }
//
//  public List<ValueType> getLeftKeyColumnTypes()
//  {
//    return leftKeyColumnTypes;
//  }
//
//  public List<String> getRightKeyColumnNames()
//  {
//    return rightKeyColumnNames;
//  }
//
//  public List<ValueType> getRightKeyColumnTypes()
//  {
//    return rightKeyColumnTypes;
//  }
}
