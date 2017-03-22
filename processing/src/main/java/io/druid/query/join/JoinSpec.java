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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;

public class JoinSpec
{
  // left and right don't have to be aligned
  private final JoinType joinType;
  private final JoinPredicate predicate;
  private final JoinInputSpec left;
  private final JoinInputSpec right;

  @JsonCreator
  public JoinSpec(
      @JsonProperty("type") JoinType joinType,
      @JsonProperty("predicate") JoinPredicate predicate,
      @JsonProperty("left") JoinInputSpec left,
      @JsonProperty("right") JoinInputSpec right
  )
  {
    this.joinType = Preconditions.checkNotNull(joinType);
    this.predicate = Preconditions.checkNotNull(predicate, "%s join requires any predicate", joinType);
    this.left = Preconditions.checkNotNull(left);
    this.right = Preconditions.checkNotNull(right);

    Preconditions.checkArgument(JoinType.INNER == joinType, "%s join type is not supported yet", joinType);
    Preconditions.checkArgument(JoinPredicates.validatePredicate(predicate));
    Preconditions.checkArgument(JoinPredicates.isEquiJoin(predicate));
  }

  public boolean hasPredicate()
  {
    return predicate != null;
  }

  @JsonProperty
  public JoinPredicate getPredicate()
  {
    return predicate;
  }

  @JsonProperty
  public JoinType getJoinType()
  {
    return joinType;
  }

  public JoinInputSpec getLeft() {
    return left;
  }

  public JoinInputSpec getRight() {
    return right;
  }
}
