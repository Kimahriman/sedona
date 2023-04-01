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
package org.apache.spark.sql.catalyst.expressions

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.codegen.{CodegenContext, ExprCode, JavaCode}

trait SerdeAware {
  def evalWithoutSerialization(input: InternalRow): Any
}

trait SerdeAwareCodegen {
  this: Expression =>

  def internalDoGenCode(ctx: CodegenContext, ev: ExprCode, serializeValue: Boolean): ExprCode

  def genChildCode(
      ctx: CodegenContext,
      child: Expression,
      deserializedType: Option[Class[_]],
      deserializer: String => String): (ExprCode, String) = {
    child match {
      case serdeAware: SerdeAwareCodegen
          if deserializedType.isDefined && !ctx.subExprEliminationExprs.get(ExpressionEquals(this)).isDefined =>
        val isNull = ctx.freshName("isNull")
        val value = ctx.freshName("value")
        val exprCode = ExprCode(
          JavaCode.isNullVariable(isNull),
          JavaCode.variable(value, deserializedType.get))
        val eval = serdeAware.internalDoGenCode(ctx, exprCode, false)
        // reduceCodeSize(ctx, eval)
        if (eval.code.toString.nonEmpty) {
          // Add `this` in the comment.
          eval.copy(code = ctx.registerComment(this.toString) + eval.code)
        } else {
          eval
        }
        (eval, eval.value)
      case e =>
        val code = e.genCode(ctx)
        (code, deserializer(code.value))
    }
  }

  def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    internalDoGenCode(ctx, ev, true)
  }
}
