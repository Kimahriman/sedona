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
package org.apache.spark.sql.sedona_sql.expressions

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.codegen._
import org.apache.spark.sql.catalyst.expressions.codegen.Block._
import org.apache.spark.sql.catalyst.util.ArrayData
import org.apache.spark.sql.sedona_sql.expressions.implicits._
import org.apache.spark.sql.sedona_sql.UDT.GeometryUDT
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String
import org.locationtech.jts.geom.Geometry

import scala.reflect.runtime.universe._

/**
  * Make expression foldable by constant folding optimizer. If all children
  * expressions are foldable, then the expression itself is foldable.
  */
trait FoldableExpression extends Expression {
  override def foldable: Boolean = children.forall(_.foldable)
}

abstract class UnaryGeometryExpression extends Expression with SerdeAware with ExpectsInputTypes {
  def inputExpressions: Seq[Expression]

  override def nullable: Boolean = true

  override def inputTypes: Seq[AbstractDataType] = Seq(GeometryUDT)

  override def eval(input: InternalRow): Any = {
    val result = evalWithoutSerialization(input)
    serializeResult(result)
  }

  override def evalWithoutSerialization(input: InternalRow): Any ={
    val inputExpression = inputExpressions.head
    val geometry = inputExpression match {
      case expr: SerdeAware => expr.evalWithoutSerialization(input)
      case expr: Any => expr.toGeometry(input)
    }

    (geometry) match {
      case (geometry: Geometry) => nullSafeEval(geometry)
      case _ => null
    }
  }

  protected def serializeResult(result: Any): Any = {
    result match {
      case geometry: Geometry => geometry.toGenericArrayData
      case _ => result
    }
  }

  protected def nullSafeEval(geometry: Geometry): Any


}

abstract class BinaryGeometryExpression extends Expression with SerdeAware with ExpectsInputTypes {
  def inputExpressions: Seq[Expression]

  override def nullable: Boolean = true

  override def inputTypes: Seq[AbstractDataType] = Seq(GeometryUDT, GeometryUDT)

  override def eval(input: InternalRow): Any = {
    val result = evalWithoutSerialization(input)
    serializeResult(result)
  }

  override def evalWithoutSerialization(input: InternalRow): Any = {
    val leftExpression = inputExpressions(0)
    val leftGeometry = leftExpression match {
      case expr: SerdeAware => expr.evalWithoutSerialization(input)
      case _ => leftExpression.toGeometry(input)
    }

    val rightExpression = inputExpressions(1)
    val rightGeometry = rightExpression match {
      case expr: SerdeAware => expr.evalWithoutSerialization(input)
      case _ => rightExpression.toGeometry(input)
    }

    (leftGeometry, rightGeometry) match {
      case (leftGeometry: Geometry, rightGeometry: Geometry) => nullSafeEval(leftGeometry, rightGeometry)
      case _ => null
    }
  }

  protected def serializeResult(result: Any): Any = {
    result match {
      case geometry: Geometry => geometry.toGenericArrayData
      case _ => result
    }
  }

  protected def nullSafeEval(leftGeometry: Geometry, rightGeometry: Geometry): Any
}

// This is a compile time type shield for the types we are able to infer. Anything
// other than these types will cause a compilation error. This is the Scala
// 2 way of making a union type.
sealed class InferrableType[T: TypeTag] extends Serializable
object InferrableType {
  implicit val geometryInstance: InferrableType[Geometry] =
    new InferrableType[Geometry] {}
  implicit val javaDoubleInstance: InferrableType[java.lang.Double] =
    new InferrableType[java.lang.Double] {}
  implicit val javaIntegerInstance: InferrableType[java.lang.Integer] =
    new InferrableType[java.lang.Integer] {}
  implicit val doubleInstance: InferrableType[Double] =
    new InferrableType[Double] {}
  implicit val booleanInstance: InferrableType[Boolean] =
    new InferrableType[Boolean] {}
  implicit val intInstance: InferrableType[Int] =
    new InferrableType[Int] {}
  implicit val stringInstance: InferrableType[String] =
    new InferrableType[String] {}
  implicit val binaryInstance: InferrableType[Array[Byte]] =
    new InferrableType[Array[Byte]] {}
  implicit val longArrayInstance: InferrableType[Array[java.lang.Long]] =
    new InferrableType[Array[java.lang.Long]] {}
}

object InferredTypes {
  def buildExtractor[T: TypeTag](expr: Expression): InternalRow => T = {
    if (typeOf[T] =:= typeOf[Geometry]) {
      input: InternalRow => expr.toGeometry(input).asInstanceOf[T]
    } else if (typeOf[T] =:= typeOf[String]) {
      input: InternalRow => expr.asString(input).asInstanceOf[T]
    } else {
      input: InternalRow => expr.eval(input).asInstanceOf[T]
    }
  }

  def buildCodegenExtractor[T: TypeTag]: String => String = {
    if (typeOf[T] =:= typeOf[Geometry]) {
      input: String => s"org.apache.sedona.common.geometrySerde.GeometrySerializer.deserialize((byte[])$input)"
    } else if (typeOf[T] =:= typeOf[String]) {
      input: String => s"$input.toString()"
    } else {
      input: String => input
    }
  }

  def buildSerializer[T: TypeTag]: T => Any = {
    if (typeOf[T] =:= typeOf[Geometry]) {
      output: T => if (output != null) {
        output.asInstanceOf[Geometry].toGenericArrayData
      } else {
        null
      }
    } else if (typeOf[T] =:= typeOf[String]) {
      output: T => if (output != null) {
        UTF8String.fromString(output.asInstanceOf[String])
      } else {
        null
      }
    } else if (typeOf[T] =:= typeOf[Array[java.lang.Long]]) {
      output: T =>
        if (output != null) {
          ArrayData.toArrayData(output)
        } else {
          null
        }
    } else {
      output: T => output
    }
  }

  def buildCodegenSerializer[T: TypeTag]: String => String = {
    if (typeOf[T] =:= typeOf[Geometry]) {
      output: String => s"org.apache.sedona.common.geometrySerde.GeometrySerializer.serialize((${classOf[Geometry].getCanonicalName()})$output)"
    } else if (typeOf[T] =:= typeOf[String]) {
      output: String => s"UTF8String.fromString((String)$output)"
    } else {
      output: String => output
    }
  }

  def getDeserializedType[T: TypeTag]: Option[Class[_]] = {
    if (typeOf[T] =:= typeOf[Geometry]) {
      Some(classOf[Geometry])
    } else if (typeOf[T] =:= typeOf[String]) {
      Some(classOf[String])
    } else {
      None
    }
  }

  def inferSparkType[T: TypeTag]: DataType = {
    if (typeOf[T] =:= typeOf[Geometry]) {
      GeometryUDT
    } else if (typeOf[T] =:= typeOf[java.lang.Double]) {
      DoubleType
    } else if (typeOf[T] =:= typeOf[java.lang.Integer]) {
      IntegerType
    } else if (typeOf[T] =:= typeOf[Double]) {
      DoubleType
    } else if (typeOf[T] =:= typeOf[Int]) {
      IntegerType
    } else if (typeOf[T] =:= typeOf[String]) {
      StringType
    } else if (typeOf[T] =:= typeOf[Array[Byte]]) {
      BinaryType
    } else if (typeOf[T] =:= typeOf[Array[java.lang.Long]]) {
      DataTypes.createArrayType(LongType)
    } else {
      BooleanType
    }
  }
}

abstract class InferredExpression[R: InferrableType]
    (implicit val rTag: TypeTag[R])
    extends Expression
    with ImplicitCastInputTypes
    with SerdeAware
    with SerdeAwareCodegen
    with Serializable {
  import InferredTypes._
  
  override def dataType = inferSparkType[R]

  lazy val serialize = buildSerializer[R]

  def deserializedType = getDeserializedType[R]

  override def eval(input: InternalRow): Any = {
    println("Eval with serialization", this)
    val res = evalWithoutSerialization(input).asInstanceOf[R]
    println(res)
    serialize(res)
  }

  def getCodegenSerializer(shouldSerialize: Boolean): String => String = {
    if (shouldSerialize || deserializedType.isEmpty) {
      val codegenSerialize = buildCodegenSerializer[R]
      val boxedType = CodeGenerator.boxedType(dataType)
      s: String => s"($boxedType)${codegenSerialize(s)}"
    } else {
      s: String => s"(${deserializedType.get.getCanonicalName})$s"
    }
  }

  def genResultCode(ctx: CodegenContext, ev: ExprCode, result: String, shouldSerialize: Boolean): String = {
    val resultSerializer = getCodegenSerializer(shouldSerialize)
    val intermediateVar = ctx.freshName("value")

    // We need to check if the result is null even if the child isn't null
    if (nullable) {
      s"""
        ${ev.isNull} = false;
        Object $intermediateVar = $result;
        if ($intermediateVar == null) {
          ${ev.isNull} = true;
        } else {
          ${ev.value} = ${resultSerializer(intermediateVar)};
        }
      """
    } else {
      s"${ev.value} = ${resultSerializer(result)};"
    }
  }

  def getExprCode(ev: ExprCode, shouldSerialize: Boolean): ExprCode = {
    ev.value match {
      case variable: VariableValue if !shouldSerialize && deserializedType.isDefined =>
        ev.copy(value = variable.copy(javaType = deserializedType.get))
      case _ => ev
    }
  }

  def getResultClass(shouldSerialize: Boolean): String = {
    if (!shouldSerialize && deserializedType.isDefined) {
      deserializedType.get.getCanonicalName()
    } else {
      CodeGenerator.javaType(dataType)
    }
  }

  def genChildCode[T: TypeTag](ctx: CodegenContext, child: Expression): (ExprCode, String) = {
    val deserializedType = getDeserializedType[T]
    val deserializer = buildCodegenExtractor[T]

    child match {
      case serdeAware: SerdeAwareCodegen if deserializedType.isDefined =>
        val code = serdeAware.withDeserializedCodeGen {
          serdeAware.genCode(ctx)
        }
        // We could still get a serialized result if the value is coming from a subexpression,
        // so we need to check if the returned type is correct
        if (code.value.javaType == deserializedType.get) {
          (code, code.value)
        } else {
          (code, deserializer(code.value))
        }
      case e =>
        val code = e.genCode(ctx)
        (code, deserializer(code.value))
    }
  }
}

/**
  * The implicit TypeTag's tell Scala to maintain generic type info at runtime. Normally type
  * erasure would remove any knowledge of what the passed in generic type is.
  */
abstract class InferredUnaryExpression[A1: InferrableType, R: InferrableType]
    (f: (A1) => R)
    (implicit val a1Tag: TypeTag[A1], implicit override val rTag: TypeTag[R])
    extends InferredExpression[R]
    with Serializable {
  import InferredTypes._

  def inputExpressions: Seq[Expression]

  override def children: Seq[Expression] = inputExpressions

  override def inputTypes: Seq[AbstractDataType] = Seq(inferSparkType[A1])

  override def nullable: Boolean = true

  lazy val extract = buildExtractor[A1](inputExpressions(0))

  override def evalWithoutSerialization(input: InternalRow): Any = {
    println("Eval without serialization", this, input)
    val value = extract(input)
    println("value", value)
    if (value != null) {
      f(value)
    } else {
      null
    }
  }

  override def internalDoGenCode(ctx: CodegenContext, ev: ExprCode, serializeResult: Boolean): ExprCode = {
    val child = inputExpressions(0)
    val (childGen, childValue) = genChildCode[A1](ctx, child)

    val func = ctx.addReferenceObj("func", f, s"scala.Function1")
    val resultCode = genResultCode(ctx, ev, s"$func.apply($childValue)", serializeResult)

    val updatedExprCode = getExprCode(ev, serializeResult)
    val javaType = getResultClass(serializeResult)

    if (nullable) {
      val nullSafeEval = ctx.nullSafeExec(child.nullable, childGen.isNull)(resultCode)
      updatedExprCode.copy(code = code"""
        ${childGen.code}
        boolean ${updatedExprCode.isNull} = true;
        $javaType ${updatedExprCode.value} = ${CodeGenerator.defaultValue(javaType, false)};
        $nullSafeEval""")
    } else {
      updatedExprCode.copy(code = code"""
        ${childGen.code}
        $javaType ${updatedExprCode.value} = ${CodeGenerator.defaultValue(javaType, false)};
        $resultCode""", isNull = FalseLiteral)
    }
  }
}

abstract class InferredBinaryExpression[A1: InferrableType, A2: InferrableType, R: InferrableType]
    (f: (A1, A2) => R)
    (implicit val a1Tag: TypeTag[A1], implicit val a2Tag: TypeTag[A2], implicit override val rTag: TypeTag[R])
    extends InferredExpression[R]
    with Serializable {
  import InferredTypes._

  def inputExpressions: Seq[Expression]

  override def children: Seq[Expression] = inputExpressions

  override def inputTypes: Seq[AbstractDataType] = Seq(inferSparkType[A1], inferSparkType[A2])

  override def nullable: Boolean = true

  override def dataType = inferSparkType[R]

  lazy val extractLeft = buildExtractor[A1](inputExpressions(0))
  lazy val extractRight = buildExtractor[A2](inputExpressions(1))

  override def evalWithoutSerialization(input: InternalRow): Any = {
    val left = extractLeft(input)
    val right = extractRight(input)
    if (left != null && right != null) {
        f(left, right)
    } else {
      null
    }
  }

  override def internalDoGenCode(ctx: CodegenContext, ev: ExprCode, serializeResult: Boolean): ExprCode = {
    val left = inputExpressions(0)
    val right = inputExpressions(1)

    val (leftGen, leftValue) = genChildCode[A1](ctx, left)
    val (rightGen, rightValue) = genChildCode[A2](ctx, right)

    val func = ctx.addReferenceObj("func", f, s"scala.Function2")
    val resultCode = genResultCode(ctx, ev, s"$func.apply($leftValue, $rightValue)", serializeResult)

    val updatedExprCode = getExprCode(ev, serializeResult)
    val javaType = getResultClass(serializeResult)

    if (nullable) {
      val nullSafeEval =
        leftGen.code + ctx.nullSafeExec(left.nullable, leftGen.isNull) {
          rightGen.code + ctx.nullSafeExec(right.nullable, rightGen.isNull) {
            resultCode
          }
      }

      updatedExprCode.copy(code = code"""
        boolean ${updatedExprCode.isNull} = true;
        $javaType ${updatedExprCode.value} = ${CodeGenerator.defaultValue(javaType, false)};
        $nullSafeEval""")
    } else {
      updatedExprCode.copy(code = code"""
        ${leftGen.code}
        ${rightGen.code}
        $javaType ${updatedExprCode.value} = ${CodeGenerator.defaultValue(javaType, false)};
        $resultCode""", isNull = FalseLiteral)
    }
  }
}

abstract class InferredTernaryExpression[A1: InferrableType, A2: InferrableType, A3: InferrableType, R: InferrableType]
    (f: (A1, A2, A3) => R)
    (implicit val a1Tag: TypeTag[A1], implicit val a2Tag: TypeTag[A2], implicit val a3Tag: TypeTag[A3], implicit override val rTag: TypeTag[R])
    extends InferredExpression[R]
    with Serializable {
  import InferredTypes._

  def inputExpressions: Seq[Expression]

  override def children: Seq[Expression] = inputExpressions

  override def inputTypes: Seq[AbstractDataType] = Seq(inferSparkType[A1], inferSparkType[A2], inferSparkType[A3])

  override def nullable: Boolean = true

  override def dataType = inferSparkType[R]

  lazy val extractFirst = buildExtractor[A1](inputExpressions(0))
  lazy val extractSecond = buildExtractor[A2](inputExpressions(1))
  lazy val extractThird = buildExtractor[A3](inputExpressions(2))

  override def evalWithoutSerialization(input: InternalRow): Any = {
    val first = extractFirst(input)
    val second = extractSecond(input)
    val third = extractThird(input)
    if (first != null && second != null && third != null) {
      f(first, second, third)
    } else {
      null
    }
  }

  override def internalDoGenCode(ctx: CodegenContext, ev: ExprCode, serializeResult: Boolean): ExprCode = {
    val first = inputExpressions(0)
    val second = inputExpressions(1)
    val third = inputExpressions(2)

    val (firstGen, firstValue) = genChildCode[A1](ctx, first)
    val (secondGen, secondValue) = genChildCode[A2](ctx, second)
    val (thirdGen, thirdValue) = genChildCode[A3](ctx, third)

    val func = ctx.addReferenceObj("func", f, s"scala.Function3")
    val resultCode = genResultCode(ctx, ev, s"$func.apply($firstValue, $secondValue, $thirdValue)", serializeResult)

    val updatedExprCode = getExprCode(ev, serializeResult)
    val javaType = getResultClass(serializeResult)

    if (nullable) {
      val nullSafeEval =
        firstGen.code + ctx.nullSafeExec(first.nullable, firstGen.isNull) {
          secondGen.code + ctx.nullSafeExec(second.nullable, secondGen.isNull) {
            thirdGen.code + ctx.nullSafeExec(third.nullable, thirdGen.isNull) {
              resultCode
            }
          }
        }

      updatedExprCode.copy(code = code"""
        boolean ${updatedExprCode.isNull} = true;
        $javaType ${updatedExprCode.value} = ${CodeGenerator.defaultValue(javaType, false)};
        $nullSafeEval""")
    } else {
      updatedExprCode.copy(code = code"""
        ${firstGen.code}
        ${secondGen.code}
        ${thirdGen.code}
        $javaType ${updatedExprCode.value} = ${CodeGenerator.defaultValue(javaType, false)};
        $resultCode""", isNull = FalseLiteral)
    }
  }
}

abstract class InferredQuarternaryExpression[A1: InferrableType, A2: InferrableType, A3: InferrableType, A4: InferrableType, R: InferrableType]
    (f: (A1, A2, A3, A4) => R)
    (implicit val a1Tag: TypeTag[A1], implicit val a2Tag: TypeTag[A2], implicit val a3Tag: TypeTag[A3], implicit val a4Tag: TypeTag[A4], implicit override val rTag: TypeTag[R])
    extends InferredExpression[R]
    with Serializable {
  import InferredTypes._

  def inputExpressions: Seq[Expression]

  override def children: Seq[Expression] = inputExpressions

  override def inputTypes: Seq[AbstractDataType] = Seq(inferSparkType[A1], inferSparkType[A2], inferSparkType[A3], inferSparkType[A4])

  override def nullable: Boolean = true

  override def dataType = inferSparkType[R]

  lazy val extractFirst = buildExtractor[A1](inputExpressions(0))
  lazy val extractSecond = buildExtractor[A2](inputExpressions(1))
  lazy val extractThird = buildExtractor[A3](inputExpressions(2))
  lazy val extractForth = buildExtractor[A4](inputExpressions(3))

  override def evalWithoutSerialization(input: InternalRow): Any = {
    val first = extractFirst(input)
    val second = extractSecond(input)
    val third = extractThird(input)
    val forth = extractForth(input)
    if (first != null && second != null && third != null && forth != null) {
      f(first, second, third, forth)
    } else {
      null
    }
  }

  override def internalDoGenCode(ctx: CodegenContext, ev: ExprCode, serializeResult: Boolean): ExprCode = {
    val first = inputExpressions(0)
    val second = inputExpressions(1)
    val third = inputExpressions(2)
    val fourth = inputExpressions(3)

    val (firstGen, firstValue) = genChildCode[A1](ctx, first)
    val (secondGen, secondValue) = genChildCode[A2](ctx, second)
    val (thirdGen, thirdValue) = genChildCode[A3](ctx, third)
    val (fourthGen, fourthValue) = genChildCode[A4](ctx, fourth)

    val func = ctx.addReferenceObj("func", f, s"scala.Function4")
    val resultCode = genResultCode(ctx, ev, s"$func.apply($firstValue, $secondValue, $thirdValue, $fourthValue)", serializeResult)

    val updatedExprCode = getExprCode(ev, serializeResult)
    val javaType = getResultClass(serializeResult)

    if (nullable) {
      val nullSafeEval =
        firstGen.code + ctx.nullSafeExec(first.nullable, firstGen.isNull) {
          secondGen.code + ctx.nullSafeExec(second.nullable, secondGen.isNull) {
            thirdGen.code + ctx.nullSafeExec(third.nullable, thirdGen.isNull) {
              fourthGen.code + ctx.nullSafeExec(fourth.nullable, fourthGen.isNull) {
                resultCode
              }
            }
          }
        }

      updatedExprCode.copy(code = code"""
        boolean ${updatedExprCode.isNull} = true;
        $javaType ${updatedExprCode.value} = ${CodeGenerator.defaultValue(javaType, false)};
        $nullSafeEval""")
    } else {
      updatedExprCode.copy(code = code"""
        ${firstGen.code}
        ${secondGen.code}
        ${thirdGen.code}
        ${fourthGen.code}
        $javaType ${updatedExprCode.value} = ${CodeGenerator.defaultValue(javaType, false)};
        $resultCode""", isNull = FalseLiteral)
    }
  }
}
