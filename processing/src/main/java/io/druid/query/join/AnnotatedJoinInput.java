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

import java.util.Objects;

public class AnnotatedJoinInput implements JoinInputSpec
{
//  private final String name;
  private final AnnotatedJoinSpec joinSpec;
  private boolean isBroadcast; // TODO: maybe move to DataInput

  public AnnotatedJoinInput(
//      String name,
      AnnotatedJoinSpec joinSpec
  )
  {
//    this.name = Objects.requireNonNull(name);
    this.joinSpec = joinSpec;
  }

//  @Override
//  public String getName()
//  {
//    return name;
//  }

  public AnnotatedJoinSpec getJoinSpec()
  {
    return joinSpec;
  }

  @Override
  public boolean equals(Object o)
  {
    if (o == this) {
      return true;
    }

    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    AnnotatedJoinInput that = (AnnotatedJoinInput) o;
//    if (!name.equals(that.name)) {
//      return false;
//    }

    // TODO: isbroadcast

    return joinSpec.equals(that.joinSpec);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(joinSpec, isBroadcast);
  }

  @Override
  public void accept(JoinSpecVisitor visitor)
  {

  }
}
