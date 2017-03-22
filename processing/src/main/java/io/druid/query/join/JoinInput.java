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

import java.util.Objects;

public class JoinInput implements JoinInputSpec
{
//  private final String name;
  private final JoinSpec joinSpec;

  @JsonCreator
  public JoinInput(
//      @JsonProperty("name") String name,
      @JsonProperty("joinSpec") JoinSpec joinSpec
  )
  {
//    this.name = Objects.requireNonNull(name);
    this.joinSpec = Objects.requireNonNull(joinSpec);
  }

//  @Override
//  @JsonProperty("name")
//  public String getName()
//  {
//    return name;
//  }

  @JsonProperty("joinSpec")
  public JoinSpec getJoinSpec()
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

    JoinInput that = (JoinInput) o;
//    if (!name.equals(that.name)) {
//      return false;
//    }

    return joinSpec.equals(that.joinSpec);
  }

  @Override
  public int hashCode()
  {
//    return Objects.hash(name, joinSpec);
    return Objects.hash(joinSpec);
  }

  @Override
  public void accept(JoinSpecVisitor visitor)
  {
    visitor.visit(this);
  }
}
