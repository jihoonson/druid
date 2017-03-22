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

import io.druid.query.dimension.DimensionSpec;
import it.unimi.dsi.fastutil.ints.IntList;

import java.util.ArrayList;
import java.util.List;

public class AnnotatedJoinSpec
{
  // TODO: rename
  public enum InputDirection {
    LEFT,
    RIGHT
  }

  private final JoinType joinType;
  private final JoinPredicate predicate;
  private final JoinInputSpec left;
  private final JoinInputSpec right;

  // TODO: left and right must be aligned
  private final IntList leftKeyIndexes;
  private final IntList rightKeyIndexes;

  private final List<DimensionSpec> leftDimensions;
  private final List<DimensionSpec> rightDimensions;
  private final IntList outputDimIndexes;
  private final List<InputDirection> outputDirections;

  public AnnotatedJoinSpec(
      JoinType joinType,
      JoinPredicate predicate,
      JoinInputSpec left,
      JoinInputSpec right,
      List<DimensionSpec> leftDimensions,
      List<DimensionSpec> rightDimensions,
      IntList leftKeyIndexes,
      IntList rightKeyIndexes,
      IntList outputDimIndexes,
      List<InputDirection> outputDirections
  )
  {
    this.joinType = joinType;
    this.predicate = predicate;
    this.left = left;
    this.right = right;
    this.leftDimensions = leftDimensions;
    this.rightDimensions = rightDimensions;
    this.leftKeyIndexes = leftKeyIndexes;
    this.rightKeyIndexes = rightKeyIndexes;
    this.outputDimIndexes = outputDimIndexes;
    this.outputDirections = outputDirections;
  }

  public JoinType getJoinType()
  {
    return joinType;
  }

  public JoinPredicate getPredicate()
  {
    return predicate;
  }

  public JoinInputSpec getLeft()
  {
    return left;
  }

  public JoinInputSpec getRight()
  {
    return right;
  }

  public IntList getOutputDimIndexes()
  {
    return outputDimIndexes;
  }

  public List<InputDirection> getOutputDirections()
  {
    return outputDirections;
  }

  public List<DimensionSpec> getLeftDimensions()
  {
    return leftDimensions;
  }

  public List<DimensionSpec> getRightDimensions()
  {
    return rightDimensions;
  }

  public List<DimensionSpec> getLeftKeyIndexes()
  {
    final List<DimensionSpec> keyDimensions = new ArrayList<>(leftKeyIndexes.size());
    for (int i = 0; i < leftKeyIndexes.size(); i++) {
      keyDimensions.add(leftDimensions.get(leftKeyIndexes.get(i)));
    }
    return keyDimensions;
  }

  public List<DimensionSpec> getRightKeyIndexes()
  {
    final List<DimensionSpec> keyDimensions = new ArrayList<>(rightKeyIndexes.size());
    for (int i = 0; i < rightKeyIndexes.size(); i++) {
      keyDimensions.add(rightDimensions.get(rightKeyIndexes.get(i)));
    }
    return keyDimensions;
  }

  public AnnotatedJoinSpec swapLeftRightInputs()
  {
    // TODO
    return null;
  }
}
