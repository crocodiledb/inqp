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

import org.apache.hadoop.conf.Configuration

import org.apache.spark.TaskContext
import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.aggregate._
import org.apache.spark.sql.catalyst.expressions.codegen.{GenerateUnsafeRowJoiner, Predicate}
import org.apache.spark.sql.execution.SlothAggResultMap
import org.apache.spark.sql.execution.metric.SQLMetric
import org.apache.spark.sql.execution.streaming.{SlothRuntime, SlothRuntimeCache, SlothRuntimeOpId, StatefulOperatorStateInfo}
import org.apache.spark.sql.execution.streaming.state._
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.KVIterator

class SlothAggregationIterator (
    partIndex: Int,
    groupingExpressions: Seq[NamedExpression],
    aggregateExpressions: Seq[AggregateExpression],
    aggregateAttributes: Seq[Attribute],
    resultExpressions: Seq[NamedExpression],
    newMutableProjection: (Seq[Expression], Seq[Attribute]) => MutableProjection,
    originalInputAttributes: Seq[Attribute],
    inputIter: Iterator[InternalRow],
    numOutputRows: SQLMetric,
    stateMemory: SQLMetric,
    stateInfo: Option[StatefulOperatorStateInfo],
    storeConf: StateStoreConf,
    hadoopConf: Configuration,
    watermarkForKey: Option[Predicate],
    watermarkForData: Option[Predicate],
    deltaOutput: Boolean,
    updateOuput: Boolean)
extends Iterator[InternalRow] with Logging {

  private val opRtId = new SlothRuntimeOpId(stateInfo.get.operatorId, stateInfo.get.queryRunId)
  private val retRT = SlothRuntimeCache.get(opRtId)
  private var aggRT =
    if (retRT != null) retRT.asInstanceOf[SlothAggIterRuntime]
    else null

  // Initialize all AggregateFunctions by binding references if necessary,
  // and mutableBufferOffset.
  // Note that we do not set inputBufferOffset here since we assume Partial mode here
  private def initializeAggregateFunctions(
      expressions: Seq[AggregateExpression]): Array[AggregateFunction] = {
    var mutableBufferOffset = 0
    val expressionsLength = expressions.length
    val functions = new Array[AggregateFunction](expressionsLength)
    var i = 0
    while (i < expressionsLength) {
      val func = expressions(i).aggregateFunction
      val funcWithUpdatedAggBufferOffset: AggregateFunction = func match {
        case function: ImperativeAggregate =>
          BindReferences.bindReference(function, originalInputAttributes).
            withNewMutableAggBufferOffset(mutableBufferOffset)
        case function => function
      }
      mutableBufferOffset += funcWithUpdatedAggBufferOffset.aggBufferSchema.length
      functions(i) = funcWithUpdatedAggBufferOffset
      i += 1
    }
    functions
  }

  private val aggregateFunctions: Array[AggregateFunction] =
    initializeAggregateFunctions(aggregateExpressions)

  // The projection used to initialize buffer values for all expression-based aggregates.
  private[this] val expressionAggInitialProjection = {
    val initExpressions = aggregateFunctions.flatMap {
      case ae: DeclarativeAggregate => ae.initialValues
      // For the positions corresponding to imperative aggregate functions, we'll use special
      // no-op expressions which are ignored during projection code-generation.
      case i: ImperativeAggregate => Seq.fill(i.aggBufferAttributes.length)(NoOp)
    }
    newMutableProjection(initExpressions, Nil)
  }

  private val groupingProjection: UnsafeProjection =
    if (aggRT != null) aggRT.groupingProjection
    else UnsafeProjection.create(groupingExpressions, originalInputAttributes)

  private def createNewAggregationBuffer(): UnsafeRow = {
    val bufferSchema = aggregateFunctions.flatMap(_.aggBufferAttributes)
    val buffer: UnsafeRow = UnsafeProjection.create(bufferSchema.map(_.dataType))
      .apply(new GenericInternalRow(bufferSchema.length))
    // Initialize declarative aggregates' buffer values
    expressionAggInitialProjection.target(buffer)(EmptyRow)
    // Initialize imperative aggregates' buffer values
    aggregateFunctions.collect { case f: ImperativeAggregate => f }.foreach(_.initialize(buffer))
    buffer
  }

  private[this] val initialAggregationBuffer: UnsafeRow =
    if (aggRT != null) aggRT.initialAggregationBuffer
    else createNewAggregationBuffer()

  private[this] val hashMapforResult = if (aggRT != null) {
    aggRT.hashMapforResult.reInit(
      TaskContext.get(),
      1024*16,
      TaskContext.get().taskMemoryManager().pageSizeBytes())
    aggRT.hashMapforResult
  } else {
    new SlothAggResultMap (
    initialAggregationBuffer,
    StructType.fromAttributes(aggregateFunctions.flatMap(_.aggBufferAttributes)),
    StructType.fromAttributes(groupingExpressions.map(_.toAttribute)),
    TaskContext.get(),
    1024 * 16, // initial capacity
    TaskContext.get().taskMemoryManager().pageSizeBytes)
  }

  private def genNonIncMetaData(aggregateExpressions: Seq[AggregateExpression],
                                inputAttributes: Seq[Attribute]): Seq[NonIncMetaPerExpr] = {
    var nonIncMetaSeq = Seq.empty[NonIncMetaPerExpr]

    if (aggregateExpressions.exists((expr: AggregateExpression) => {
      val aggFunc = expr.aggregateFunction
      aggFunc.isInstanceOf[DeclarativeAggregate] &&
        (!aggFunc.isInstanceOf[Max] && !aggFunc.isInstanceOf[Min])
    })) {
      throw new IllegalArgumentException(s"Only support max/min")
    }

    var bufOffset = 0
    var rowOffset = 0
    var attr: AttributeReference = null
    for (aggExpr <- aggregateExpressions) {
      val aggFunc = aggExpr.aggregateFunction
      if (aggFunc.isInstanceOf[Max] || aggFunc.isInstanceOf[Min]) {
        // TODO: we assume max/min applies on an attribute rather than an expression
        if (aggFunc.isInstanceOf[Max]) {
          attr = aggFunc.asInstanceOf[Max].child.asInstanceOf[AttributeReference]
        } else {
          attr = aggFunc.asInstanceOf[Min].child.asInstanceOf[AttributeReference]
        }
        rowOffset = inputAttributes.indexWhere((inputAttr: Attribute) => {
            inputAttr.asInstanceOf[AttributeReference].semanticEquals(attr)})
        val nonIncMetaPerExpr = NonIncMetaPerExpr(
          aggExpr, bufOffset, rowOffset, aggFunc.dataType)
        nonIncMetaSeq = nonIncMetaSeq :+ nonIncMetaPerExpr
      }
      bufOffset += aggFunc.aggBufferAttributes.length
    }

    nonIncMetaSeq
  }

  private[this] val nonIncMetaData: Seq[NonIncMetaPerExpr] =
    genNonIncMetaData(aggregateExpressions, originalInputAttributes)

  private def checkChangeValue(buffer: UnsafeRow, row: UnsafeRow,
                               nonIncMeta: NonIncMetaPerExpr): Boolean = {
    val bufOffset = nonIncMeta.bufOffset
    val rowOffset = nonIncMeta.rowOffset

    nonIncMeta.dataType match {
      case DoubleType =>
        buffer.getDouble(bufOffset) == row.getDouble(rowOffset)
      case LongType =>
        buffer.getLong(bufOffset) == row.getLong(rowOffset)
      case IntegerType =>
        buffer.getInt(bufOffset) == row.getInt(rowOffset)
      case _ =>
        throw new IllegalArgumentException(s"Unsupported ${nonIncMeta.dataType} for max/min")
    }
  }

  private def resetBufferValue(buffer: UnsafeRow, nonIncMeta: NonIncMetaPerExpr): Unit = {
    val bufOffset = nonIncMeta.bufOffset

     nonIncMeta.dataType match {
      case DoubleType =>
        if (nonIncMeta.aggExpr.aggregateFunction.isInstanceOf[Max]) buffer.setDouble(bufOffset, 0.0)
        else buffer.setDouble(bufOffset, Double.MaxValue)
      case LongType =>
        if (nonIncMeta.aggExpr.aggregateFunction.isInstanceOf[Max]) buffer.setLong(bufOffset, 0L)
        else buffer.setLong(bufOffset, Long.MaxValue)
      case IntegerType =>
        if (nonIncMeta.aggExpr.aggregateFunction.isInstanceOf[Max]) buffer.setInt(bufOffset, 0)
        else buffer.setInt(bufOffset, Int.MaxValue)
      case _ =>
        throw new IllegalArgumentException(s"Unsupported ${nonIncMeta.dataType} for max/min")
    }
  }

  private[this] val hashMapforMetaData = if (aggRT != null) {
    aggRT.hashMapforMetaData.reInit(stateInfo, storeConf, hadoopConf)
    aggRT.hashMapforMetaData
  } else {
    new SlothAggMetaMap(groupingExpressions,
      nonIncMetaData.length, stateInfo, storeConf, hadoopConf, watermarkForKey)
  }


  private[this] val hashMapforFullData = if (nonIncMetaData.nonEmpty) {
    if (aggRT != null) {
      aggRT.hashMapforFullData.reInit(stateInfo, storeConf, hadoopConf)
      aggRT.hashMapforFullData
    } else {
      new SlothAggFullMap(groupingExpressions,
        originalInputAttributes, stateInfo, storeConf, hadoopConf, watermarkForData)
    }
  } else { null }

  // private[this] val hashMapforFullData: SlothAggFullMap = null

  private[this] val stateStoreforResult =
    if (aggRT != null) {
      aggRT.stateStoreforResult.reInit(stateInfo, storeConf, hadoopConf)
      aggRT.stateStoreforResult
    } else {
      new SlothAggResultStore(
        groupingExpressions, aggregateFunctions.flatMap(_.aggBufferAttributes),
        stateInfo, storeConf, hadoopConf, watermarkForKey)
    }

  // Initializing functions used to process a row.
  protected def generateProcessRow(
      expressions: Seq[AggregateExpression],
      functions: Seq[AggregateFunction],
      inputAttributes: Seq[Attribute],
      isDeclarative: Boolean,
      isInsert: Boolean): (InternalRow, InternalRow) => Unit = {
    val joinedRow = new JoinedRow
    if (expressions.nonEmpty) {
      if (isDeclarative) {
        val declarativeExpressions = functions.flatMap {
          case (ae: DeclarativeAggregate) =>
            ae.updateExpressions
          case (agg: AggregateFunction) =>
            Seq.fill(agg.aggBufferAttributes.length)(NoOp)
        }

        // This projection is used to merge buffer values for all expression-based aggregates.
        val aggregationBufferSchema = functions.flatMap(_.aggBufferAttributes)
        val declarativeProjection =
          newMutableProjection(declarativeExpressions, aggregationBufferSchema ++ inputAttributes)

        (currentBuffer: InternalRow, row: InternalRow) => {
          // Process all expression-based aggregate functions.
          declarativeProjection.target(currentBuffer)(joinedRow(currentBuffer, row))
        }

      } else {
        val imperativeFunctions = functions.collect {
          case (ae: ImperativeAggregate) =>
            if (isInsert) {
              (buffer: InternalRow, row: InternalRow) => ae.update(buffer, row)
            } else {
              (buffer: InternalRow, row: InternalRow) => ae.delete(buffer, row)
            }
        }.toArray

        (currentBuffer: InternalRow, row: InternalRow) => {
          // Process all imperative aggregate functions.
          var i = 0
          while (i < imperativeFunctions.length) {
            imperativeFunctions(i)(currentBuffer, row)
            i += 1
          }
        }
      }
    } else {
      // Grouping only.
      (currentBuffer: InternalRow, row: InternalRow) => {}
    }
  }

  private val processDeclarative: (UnsafeRow, InternalRow) => Unit =
    generateProcessRow(aggregateExpressions, aggregateFunctions, originalInputAttributes,
      true, true)

  private val processImperativeInsert: (UnsafeRow, InternalRow) => Unit =
    generateProcessRow(aggregateExpressions, aggregateFunctions, originalInputAttributes,
      false, true)

  private val processImperativeDelete: (UnsafeRow, InternalRow) => Unit =
    generateProcessRow(aggregateExpressions, aggregateFunctions, originalInputAttributes,
      false, false)

  private def processRow(groupKey: UnsafeRow, buffer: UnsafeRow,
                         input: InternalRow, isNewGroup: Boolean): Unit = {

    val unsafeInput = input.asInstanceOf[UnsafeRow]
    val isInsert = unsafeInput.isInsert
    unsafeInput.cleanStates()

    // Load state into all hashmaps
    if (isNewGroup) {
      hashMapforMetaData.newEntry(groupKey)

      val storeBuffer = stateStoreforResult.get(groupKey)
      if (storeBuffer != null) {
        buffer.copyFrom(storeBuffer)
      }
    }

    // Update state of hashMapForMetaData and hashMapForFullData
    if (isInsert) {
      hashMapforMetaData.incCounter(groupKey)

      if (hashMapforFullData != null) {
        val newID = hashMapforMetaData.allocID(groupKey)
        hashMapforFullData.put(groupKey, newID, true, unsafeInput)
      }
    } else {
      hashMapforMetaData.decCounter(groupKey)

      if (hashMapforFullData != null) {
        val newID = hashMapforMetaData.allocID(groupKey)
        hashMapforFullData.put(groupKey, newID, false, unsafeInput)
        // hashMapforFullData.remove(groupKey,
        // hashMapforMetaData.getOldMaxID(groupKey), unsafeInput)
      }
    }

    // Process the input, i.e. update hashMapforResult
    if (isInsert) {
      processDeclarative(buffer, unsafeInput)
      processImperativeInsert(buffer, unsafeInput)
    } else {
      processImperativeDelete(buffer, unsafeInput)

      // Check whether this delete will change the max/min value for this group
      for (exprIndex <- nonIncMetaData.indices) {
        if (!hashMapforMetaData.getHasChange(groupKey, exprIndex) &&
            checkChangeValue(buffer, unsafeInput, nonIncMetaData(exprIndex))) {
          hashMapforMetaData.setHasChange(groupKey, exprIndex)
        }
      }
    }
  }

  private def processInputs(): Unit = {
    if (groupingExpressions.isEmpty && inputIter.hasNext) {
      // If there is no grouping expressions, we can just reuse the same buffer over and over again.
      // Note that it would be better to eliminate the hash map entirely in the future.
      val groupingKey = groupingProjection.apply(null)
      val buffer: UnsafeRow = hashMapforResult.getAggregationBufferFromUnsafeRow(groupingKey)
      var isNewGroup = true
      while (inputIter.hasNext) {
        val newInput = inputIter.next()
        processRow(groupingKey, buffer, newInput, isNewGroup)
        isNewGroup = false
      }
    } else {
      while (inputIter.hasNext) {
        val newInput = inputIter.next()
        val groupingKey = groupingProjection.apply(newInput)
        val buffer: UnsafeRow = hashMapforResult.getAggregationBufferFromUnsafeRow(groupingKey)
        val isNewGroup = hashMapforResult.getIsNewGroup
        processRow(groupingKey, buffer, newInput, isNewGroup)
      }
    }
  }

  // Recompute NonInc functions (i.e. max/min)
  private def computeNonInc(): Unit = {
    if (hashMapforFullData != null) {
      val start = System.nanoTime()
      val numKeys = hashMapforFullData.getNumKeys
      for ((nonIncMetaPerExpr, exprIndex) <- nonIncMetaData.zipWithIndex) {
        val iter = hashMapforFullData.getGroupIteratorbyExpr(
          nonIncMetaPerExpr, exprIndex, hashMapforMetaData)
        iter.foreach((p: UnsafeRowPair) => {
          val groupkey = p.key
          val row = p.value
          val buffer = hashMapforResult.getAggregationBufferFromUnsafeRow(groupkey)
          resetBufferValue(buffer, nonIncMetaPerExpr)
          processDeclarative(buffer, row)
        })
      }
      print(s"Sort Time ${(System.nanoTime() - start)/1000000} ms, ${numKeys} keys\n")
    }
  }

  private def generateResultProjection(): (UnsafeRow, UnsafeRow) => UnsafeRow = {
    val groupingAttributes = groupingExpressions.map(_.toAttribute)
    val bufferAttributes = aggregateFunctions.flatMap(_.aggBufferAttributes)
    val groupingKeySchema = StructType.fromAttributes(groupingAttributes)
    val bufferSchema = StructType.fromAttributes(bufferAttributes)
    val unsafeRowJoiner = GenerateUnsafeRowJoiner.create(groupingKeySchema, bufferSchema)

    (currentGroupingKey: UnsafeRow, currentBuffer: UnsafeRow) => {
      unsafeRowJoiner.join(currentGroupingKey, currentBuffer)
    }
  }

  private val generateOutput: (UnsafeRow, UnsafeRow) => UnsafeRow =
    if (aggRT != null) {
      aggRT.generateOutput
    } else {
      generateResultProjection()
    }

  private[this] var resultIter: KVIterator[UnsafeRow, UnsafeRow] = _
  private[this] var resultIterHasNext: Boolean = _
  private[this] var groupKey: UnsafeRow = _
  private[this] var oldGroupValue: UnsafeRow = _
  private[this] var newGroupValue: UnsafeRow = _

  processInputs()
  computeNonInc()

  stateStoreforResult.scan()
  resultIter = hashMapforResult.iterator()
  resultIterHasNext = resultIter.next()

  // // Pre-load the first key-value pair from the aggregationBufferMapIterator.
  // resultIterHasNext = resultIter.next()
  // // If the map is empty, we just free it.
  // if (!resultIterHasNext) {
  //   hashMapforResult.free()
  // } else {
  //   // OK, maybe we need a delete here
  //   val groupkey = resultIter.getKey
  //   if (!hashMapforMetaData.isNewGroup(groupkey) && deltaOutput) {
  //     oldGroupValue = stateStoreforResult.get(groupkey)
  //   }
  // }

  TaskContext.get().addTaskCompletionListener[Unit](_ => {
    // At the end of the task, update the task's peak memory usage. Since we destroy
    // the map to create the sorter, their memory usages should not overlap, so it is safe
    // to just use the max of the two.
    val memoryConsumption = hashMapforResult.getPeakMemoryUsedBytes +
      hashMapforMetaData.getMemoryConsumption() +
      {
        if (hashMapforFullData != null) { hashMapforFullData.getMemoryConsumption }
        else { 0L }
      }
    stateMemory.set(memoryConsumption)

    val metrics = TaskContext.get().taskMetrics()
    metrics.incPeakExecutionMemory(memoryConsumption)

    // Updating average hashmap probe
    // avgHashProbe.set(hashMapforResult.getAverageProbesPerLookup())

    // val numKeys = stateStoreforResult.getNumKeys() +
    //   hashMapforMetaData.getNumKeys() +
    //   {
    //     if (hashMapforFullData != null) { hashMapforFullData.getNumKeys }
    //     else { 0L }
    //   }
    // numTotalStateRows.set(numKeys)

  })

  override final def hasNext: Boolean = {
    if (oldGroupValue != null || newGroupValue != null) return true

    // Load the next group having data
    while (resultIterHasNext && oldGroupValue == null && newGroupValue == null) {
      groupKey = resultIter.getKey
      if (!hashMapforMetaData.isNewGroup(groupKey) && deltaOutput) {
        oldGroupValue = stateStoreforResult.get(groupKey)
      }
      if (hashMapforMetaData.getCounter(groupKey) != 0) {
        newGroupValue = resultIter.getValue
      }

      resultIterHasNext = resultIter.next()
    }

    if (oldGroupValue != null || newGroupValue != null) return true
    else return false
  }

  override final def next(): UnsafeRow = {
    if (hasNext) {
      var ret: UnsafeRow = null

      // This is an update
      if (oldGroupValue != null && newGroupValue != null) {
        ret = generateOutput(groupKey, oldGroupValue)
        ret.setInsert(false)
        ret.setUpdate(true && updateOuput)
        oldGroupValue = null

        // This is a delete
      } else if (oldGroupValue != null && newGroupValue == null) {
        ret = generateOutput(groupKey, oldGroupValue)
        ret.setInsert(false)
        ret.setUpdate(false)
        oldGroupValue = null

        // This is an insert
      } else if (oldGroupValue == null && newGroupValue != null) {
        stateStoreforResult.put(groupKey, newGroupValue)
        ret = generateOutput(groupKey, newGroupValue)
        ret.setInsert(true)
        ret.setUpdate(false)
        newGroupValue = null

      } else {
        throw new IllegalArgumentException
      }
      numOutputRows += 1
      ret
    } else {
      throw new IllegalArgumentException
    }
  }

  // override final def hasNext: Boolean = {
  //   resultIterHasNext || oldGroupValue != null
  // }

  // override final def next(): UnsafeRow = {
  //   if (hasNext) {
  //     var ret: UnsafeRow = null
  //     val groupkey = resultIter.getKey

  //     if (oldGroupValue != null) {
  //       ret = generateOutput(groupkey, oldGroupValue)
  //       ret.setInsert(false)
  //       ret.setUpdate(isUpdate)
  //       oldGroupValue = null
  //     } else {
  //       val groupval = resultIter.getValue

  //       // Save the key/value into state store
  //       stateStoreforResult.put(groupkey, groupval)

  //       val result = generateOutput(groupkey, groupval)
  //       resultIterHasNext = resultIter.next()

  //       if (!resultIterHasNext) {
  //         val resultCopy = result.copy()
  //         hashMapforResult.free()
  //         ret = resultCopy
  //       } else {
  //         ret = result

  //         // Check whether we need a delete for the next groupkey
  //         val nextGroupKey = resultIter.getKey
  //         if (!hashMapforMetaData.isNewGroup(nextGroupKey) && deltaOutput) {
  //           oldGroupValue = stateStoreforResult.get(nextGroupKey)
  //         }
  //       }

  //       ret.setInsert(true)
  //     }

  //     numOutputRows += 1
  //     ret
  //   } else {
  //     throw new IllegalArgumentException
  //   }
  // }

    /**
   * Generate an output row when there is no input and there is no grouping expression.
   */
  def outputForEmptyGroupingKeyWithoutInput(): UnsafeRow = {
    if (groupingExpressions.isEmpty) {
      // We create an output row and copy it. So, we can free the map.
      val resultCopy =
        generateOutput(UnsafeRow.createFromByteArray(0, 0), initialAggregationBuffer).copy()
      hashMapforResult.free()
      resultCopy.setInsert(true)
      resultCopy
    } else {
      throw new IllegalStateException(
        "This method should not be called when groupingExpressions is not empty.")
    }
  }

  def onCompletion(): Unit = {
    hashMapforResult.free()

    hashMapforMetaData.saveToStateStore()
    hashMapforMetaData.commit()

    if (hashMapforFullData != null) {
      hashMapforFullData.commit()
    }

    stateStoreforResult.commit()

    hashMapforMetaData.purge()
    if (hashMapforFullData != null) hashMapforFullData.purge()
    stateStoreforResult.purge()
    if (aggRT == null) {
      aggRT = new SlothAggIterRuntime(
        groupingProjection,
        initialAggregationBuffer,
        generateOutput,
        hashMapforResult,
        hashMapforMetaData,
        hashMapforFullData,
        stateStoreforResult)
    }
    SlothRuntimeCache.put(opRtId, aggRT)
  }
}

case class SlothAggIterRuntime (
  groupingProjection: UnsafeProjection,
  initialAggregationBuffer: UnsafeRow,
  generateOutput: (UnsafeRow, UnsafeRow) => UnsafeRow,
  hashMapforResult: SlothAggResultMap,
  hashMapforMetaData: SlothAggMetaMap,
  hashMapforFullData: SlothAggFullMap,
  stateStoreforResult: SlothAggResultStore) extends SlothRuntime {}
