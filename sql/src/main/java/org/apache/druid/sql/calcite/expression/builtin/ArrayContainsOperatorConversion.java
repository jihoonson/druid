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

package org.apache.druid.sql.calcite.expression.builtin;

import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.type.SqlTypeFamily;
import org.apache.druid.math.expr.Expr;
import org.apache.druid.math.expr.Parser;
import org.apache.druid.query.filter.AndDimFilter;
import org.apache.druid.query.filter.DimFilter;
import org.apache.druid.sql.calcite.expression.DruidExpression;
import org.apache.druid.sql.calcite.expression.Expressions;
import org.apache.druid.sql.calcite.expression.OperatorConversions;
import org.apache.druid.sql.calcite.planner.PlannerContext;
import org.apache.druid.sql.calcite.rel.VirtualColumnRegistry;
import org.apache.druid.sql.calcite.table.RowSignature;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.List;

public class ArrayContainsOperatorConversion extends BaseExpressionDimFilterOperatorConversion
{
  private static final String EXPR_FUNCTION = "array_contains";

  private static final SqlFunction SQL_FUNCTION = OperatorConversions
      .operatorBuilder("ARRAY_CONTAINS")
      .operandTypeChecker(
          OperandTypes.sequence(
              "(array,array)",
              OperandTypes.or(
                  OperandTypes.family(SqlTypeFamily.ARRAY),
                  OperandTypes.family(SqlTypeFamily.STRING)
              ),
              OperandTypes.or(
                  OperandTypes.family(SqlTypeFamily.ARRAY),
                  OperandTypes.family(SqlTypeFamily.STRING)
              )
          )
      )
      .returnTypeInference(ReturnTypes.BOOLEAN)
      .build();

  public ArrayContainsOperatorConversion()
  {
    super(SQL_FUNCTION, EXPR_FUNCTION);
  }

  @Nullable
  @Override
  public DimFilter toDruidFilter(
      final PlannerContext plannerContext,
      RowSignature rowSignature,
      @Nullable VirtualColumnRegistry virtualColumnRegistry,
      final RexNode rexNode
  )
  {
    final List<RexNode> operands = ((RexCall) rexNode).getOperands();
    final List<DruidExpression> druidExpressions = Expressions.toDruidExpressions(
        plannerContext,
        rowSignature,
        operands
    );
    if (druidExpressions == null) {
      return null;
    }

    final DruidExpression leftExpr = druidExpressions.get(0);
    final DruidExpression rightExpr = druidExpressions.get(1);

    if (leftExpr.isSimpleExtraction()) {
      Expr expr = Parser.parse(rightExpr.getExpression(), plannerContext.getExprMacroTable());


      // if there is more than one elements
      // otherwise
//      new SelectorDimFilter(
//          leftExpr.getSimpleExtraction().getColumn(),
//          ,
//          leftExpr.getSimpleExtraction().getExtractionFn()
//      )
      final List<DimFilter> selectFilters = new ArrayList<>();

      return new AndDimFilter(selectFilters);
    } else {
      return toExpressionFilter(plannerContext, getDruidFunctionName(), druidExpressions);
    }
  }
}
