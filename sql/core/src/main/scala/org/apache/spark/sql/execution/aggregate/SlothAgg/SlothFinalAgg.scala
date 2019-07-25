/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.execution.aggregate.SlothAgg

import scala.collection.mutable.ArrayBuffer

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.aggregate._
import org.apache.spark.sql.execution.streaming.{SlothRuntime, SlothRuntimeCache, SlothRuntimeOpId}

class SlothFinalAgg(
    partIndex: Int,
    groupingExpressions: Seq[NamedExpression],
    originalInputAttributes: Seq[Attribute],
    aggregateExpressions: Seq[AggregateExpression],
    aggregateAttributes: Seq[Attribute],
    initialInputBufferOffset: Int,
    resultExpressions: Seq[NamedExpression],
    newMutableProjection: (Seq[Expression], Seq[Attribute]) => MutableProjection,
    opRtId: SlothRuntimeOpId) {

  // Initialize all AggregateFunctions by binding references if necessary,
  // and set inputBufferOffset and mutableBufferOffset.
  protected def initializeAggregateFunctions(
      expressions: Seq[AggregateExpression],
      startingInputBufferOffset: Int): Array[AggregateFunction] = {
    var mutableBufferOffset = 0
    var inputBufferOffset: Int = startingInputBufferOffset
    val expressionsLength = expressions.length
    val functions = new Array[AggregateFunction](expressionsLength)
    var i = 0
    while (i < expressionsLength) {
      val func = expressions(i).aggregateFunction
      val funcWithBoundReferences: AggregateFunction = expressions(i).mode match {
        case Partial | Complete if func.isInstanceOf[ImperativeAggregate] =>
          // We need to create BoundReferences if the function is not an
          // expression-based aggregate function (it does not support code-gen) and the mode of
          // this function is Partial or Complete because we will call eval of this
          // function's children in the update method of this aggregate function.
          // Those eval calls require BoundReferences to work.
          BindReferences.bindReference(func, originalInputAttributes)
        case _ =>
          // We only need to set inputBufferOffset for aggregate functions with mode
          // PartialMerge and Final.
          val updatedFunc = func match {
            case function: ImperativeAggregate =>
              function.withNewInputAggBufferOffset(inputBufferOffset)
            case function => function
          }
          inputBufferOffset += func.aggBufferSchema.length
          updatedFunc
      }
      val funcWithUpdatedAggBufferOffset = funcWithBoundReferences match {
        case function: ImperativeAggregate =>
          // Set mutableBufferOffset for this function. It is important that setting
          // mutableBufferOffset happens after all potential bindReference operations
          // because bindReference will create a new instance of the function.
          function.withNewMutableAggBufferOffset(mutableBufferOffset)
        case function => function
      }
      mutableBufferOffset += funcWithUpdatedAggBufferOffset.aggBufferSchema.length
      functions(i) = funcWithUpdatedAggBufferOffset
      i += 1
    }
    functions
  }

  protected val aggregateFunctions: Array[AggregateFunction] =
    initializeAggregateFunctions(aggregateExpressions, initialInputBufferOffset)

  protected val groupingAttributes = groupingExpressions.map(_.toAttribute)

  // Positions of those imperative aggregate functions in allAggregateFunctions.
  // For example, we have func1, func2, func3, func4 in aggregateFunctions, and
  // func2 and func3 are imperative aggregate functions.
  // ImperativeAggregateFunctionPositions will be [1, 2].
  protected[this] val allImperativeAggregateFunctionPositions: Array[Int] = {
    val positions = new ArrayBuffer[Int]()
    var i = 0
    while (i < aggregateFunctions.length) {
      aggregateFunctions(i) match {
        case agg: DeclarativeAggregate =>
        case _ => positions += i
      }
      i += 1
    }
    positions.toArray
  }

  // All imperative AggregateFunctions.
  protected[this] val allImperativeAggregateFunctions: Array[ImperativeAggregate] =
    allImperativeAggregateFunctionPositions
      .map(aggregateFunctions)
      .map(_.asInstanceOf[ImperativeAggregate])

  // Initializing the function used to generate the output row.
  private def generateResultProjection(): (UnsafeRow, InternalRow) => UnsafeRow = {
    val joinedRow = new JoinedRow
    val modes = aggregateExpressions.map(_.mode).distinct
    val bufferAttributes = aggregateFunctions.flatMap(_.aggBufferAttributes)
    val evalExpressions = aggregateFunctions.map {
      case ae: DeclarativeAggregate => ae.evaluateExpression
      case agg: AggregateFunction => NoOp
    }
    val aggregateResult = new SpecificInternalRow(aggregateAttributes.map(_.dataType))
    val expressionAggEvalProjection = newMutableProjection(evalExpressions, bufferAttributes)
    expressionAggEvalProjection.target(aggregateResult)

    val resultProjection =
      UnsafeProjection.create(resultExpressions, groupingAttributes ++ aggregateAttributes)
    resultProjection.initialize(partIndex)

    (currentGroupingKey: UnsafeRow, currentBuffer: InternalRow) => {
      // Generate results for all expression-based aggregate functions.
      expressionAggEvalProjection(currentBuffer)
      // Generate results for all imperative aggregate functions.
      var i = 0
      while (i < allImperativeAggregateFunctions.length) {
        aggregateResult.update(
          allImperativeAggregateFunctionPositions(i),
          allImperativeAggregateFunctions(i).eval(currentBuffer))
        i += 1
      }
      resultProjection(joinedRow(currentGroupingKey, aggregateResult))
    }
  }

  private val curRt = SlothRuntimeCache.get(opRtId)
  private var finalRt =
    if (curRt != null) {curRt.asInstanceOf[SlothFinalAggRuntime]}
    else null

  private val generateOutput: (UnsafeRow, InternalRow) => UnsafeRow =
    if (finalRt != null) finalRt.generateOutput
    else generateResultProjection()

  private val groupingProjection: UnsafeProjection =
    if (finalRt != null) finalRt.groupingProjection
    else UnsafeProjection.create(groupingExpressions, originalInputAttributes)

  private val aggBufferExpression =
    aggregateExpressions.flatMap(_.aggregateFunction.inputAggBufferAttributes)

  private val bufferProjection: UnsafeProjection =
    if (finalRt != null) finalRt.bufferProjection
    else UnsafeProjection.create(aggBufferExpression, originalInputAttributes)

  val processFinalRow = (input: InternalRow) => {
    val groupingKey = if (groupingExpressions.isEmpty) {groupingProjection.apply(null)}
                      else {groupingProjection.apply(input)}
    val output = generateOutput(groupingKey, bufferProjection.apply(input))
    output.setInsert(input.isInsert)
    output.setUpdate(input.isUpdate)

    output
  }: InternalRow


  def onCompletion(): Unit = {
    if (finalRt == null) {
      finalRt = new SlothFinalAggRuntime(
        generateOutput,
        groupingProjection,
        bufferProjection)
    }

    SlothRuntimeCache.put(opRtId, finalRt)
  }

}

case class SlothFinalAggRuntime (
  generateOutput: (UnsafeRow, InternalRow) => UnsafeRow,
  groupingProjection: UnsafeProjection,
  bufferProjection: UnsafeProjection) extends SlothRuntime {}
