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

package org.apache.spark.sql.execution.aggregate

import java.util.UUID

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.errors._
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.expressions.aggregate._
import org.apache.spark.sql.catalyst.plans.physical._
import org.apache.spark.sql.execution._
import org.apache.spark.sql.execution.aggregate.SlothAgg.SlothFinalAgg
import org.apache.spark.sql.execution.streaming.SlothRuntimeOpId
import org.apache.spark.util.{CompletionIterator, Utils}

case class SlothFinalAggExec (
    requiredChildDistributionExpressions: Option[Seq[Expression]],
    groupingExpressions: Seq[NamedExpression],
    aggregateExpressions: Seq[AggregateExpression],
    aggregateAttributes: Seq[Attribute],
    initialInputBufferOffset: Int,
    resultExpressions: Seq[NamedExpression],
    child: SparkPlan)
  extends UnaryExecNode {

  private[this] val aggregateBufferAttributes = {
    aggregateExpressions.flatMap(_.aggregateFunction.aggBufferAttributes)
  }

  require(HashAggregateExec.supportsAggregate(aggregateBufferAttributes))

  override lazy val allAttributes: AttributeSeq =
    child.output ++ aggregateBufferAttributes ++ aggregateAttributes ++
      aggregateExpressions.flatMap(_.aggregateFunction.inputAggBufferAttributes)

  override def output: Seq[Attribute] = resultExpressions.map(_.toAttribute)

  override def outputPartitioning: Partitioning = child.outputPartitioning

  override def producedAttributes: AttributeSet =
    AttributeSet(aggregateAttributes) ++
    AttributeSet(resultExpressions.diff(groupingExpressions).map(_.toAttribute)) ++
    AttributeSet(aggregateBufferAttributes)

  override def requiredChildDistribution: List[Distribution] = {
    requiredChildDistributionExpressions match {
      case Some(exprs) if exprs.isEmpty => AllTuples :: Nil
      case Some(exprs) if exprs.nonEmpty => ClusteredDistribution(exprs) :: Nil
      case None => UnspecifiedDistribution :: Nil
    }
  }

  private var opId: Long = _
  private var runId: UUID = _

  def setId(opId: Long, runId: UUID): Unit = {
    this.opId = opId
    this.runId = runId
  }

  protected override def doExecute(): RDD[InternalRow] = attachTree(this, "execute") {

    child.execute().mapPartitionsWithIndex { (partIndex, iter) => {
      if (iter.isEmpty) Iterator.empty
      else {
        val finalAgg = new SlothFinalAgg(
          partIndex,
          groupingExpressions,
          child.output,
          aggregateExpressions,
          aggregateAttributes,
          initialInputBufferOffset,
          resultExpressions,
          (expressions, inputSchema) =>
            newMutableProjection(expressions, inputSchema, subexpressionEliminationEnabled),
          new SlothRuntimeOpId(opId, runId))

        CompletionIterator[InternalRow, Iterator[InternalRow]](
          iter.map(finalAgg.processFinalRow),
          finalAgg.onCompletion())
      }

    }}
  }

  def isAggregateExpr(expr: NamedExpression): Boolean = {
    val attr = expr match {
      case alias: Alias =>
        if (alias.child.isInstanceOf[AttributeReference]) {
          alias.child.asInstanceOf[AttributeReference]
        } else {
          return true
        }
      case attrRef: AttributeReference =>
        attrRef
      case _ =>
        return true
    }

    !groupingExpressions.exists(namedExpr => namedExpr.toAttribute.semanticEquals(attr))
  }

  def findUpdateAttributes(): Seq[Attribute] = {
    resultExpressions.filter(isAggregateExpr).map(_.toAttribute)
  }

  override def verboseString: String = toString(verbose = true)

  override def simpleString: String = toString(verbose = false)

  override def setUpdateOutput(updateOutput: Boolean): Unit = child.setUpdateOutput(updateOutput)

  private def toString(verbose: Boolean): String = {
    val allAggregateExpressions = aggregateExpressions

    val keyString = Utils.truncatedString(groupingExpressions, "[", ", ", "]")
    val functionString = Utils.truncatedString(allAggregateExpressions, "[", ", ", "]")
    val outputString = Utils.truncatedString(output, "[", ", ", "]")
    if (verbose) {
      s"SlothFinalHashAggregate(keys=$keyString, functions=$functionString, output=$outputString)"
    } else {
      s"SlothFinalHashAggregate(keys=$keyString, functions=$functionString)"
    }
  }
}
