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

  @volatile var shouldSerialize = true

  def withDeserializedCodeGen[T](f: => T): T = {
    shouldSerialize = false
    try {
      f
    } finally {
      shouldSerialize = true
    }
  }

  def internalDoGenCode(ctx: CodegenContext, ev: ExprCode, serializeValue: Boolean): ExprCode

  def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    internalDoGenCode(ctx, ev, shouldSerialize)
  }
}
