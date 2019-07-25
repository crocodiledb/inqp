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
import util.control.Breaks._

import org.apache.spark.TaskContext
import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.aggregate._
import org.apache.spark.sql.catalyst.expressions.codegen.Predicate
import org.apache.spark.sql.execution.streaming.StatefulOperatorStateInfo
import org.apache.spark.sql.execution.streaming.state._
import org.apache.spark.sql.types._

class SlothAggFullMap(
    groupExpressions: Seq[NamedExpression],
    inputAttributes: Seq[Attribute],
    var stateInfo: Option[StatefulOperatorStateInfo],
    var storeConf: StateStoreConf,
    var hadoopConf: Configuration,
    watermarkForData: Option[Predicate]) extends Logging {

  private val keySchema = StructType(groupExpressions.zipWithIndex.map {
      case (k, i) => StructField(s"field$i", k.dataType, k.nullable)})
  private val keyAttributes = keySchema.toAttributes

  private val keyWithIndexSchema = keySchema.add("index", "long")
  private val keyWithIndexExprs = keyAttributes :+ Literal(1L)

  private val keyWithIndexRowGenerator = UnsafeProjection.create(keyWithIndexExprs, keyAttributes)
  private val indexOrdinal = keyAttributes.size
  private val keyWithoutIndexGenerator = UnsafeProjection
    .create(groupExpressions, inputAttributes)

  private val valSchema = inputAttributes.toStructType

  private val storeName = "GroupKeywithIndexToFullDataStore"

  private var stateStore = getStateStore(keyWithIndexSchema, valSchema)

  def reInit(stateInfo: Option[StatefulOperatorStateInfo],
             storeConf: StateStoreConf,
             hadoopConf: Configuration): Unit = {
    this.stateInfo = stateInfo
    this.storeConf = storeConf
    this.hadoopConf = hadoopConf
    stateStore = getStateStore(keyWithIndexSchema, valSchema)
  }

  def purge(): Unit = {

  }

  /** Generated a row using the key and index */
  private def keyWithMetaRow(key: UnsafeRow, valueIndex: Long): UnsafeRow = {
    val row = keyWithIndexRowGenerator(key)
    row.setLong(indexOrdinal, valueIndex)
    row
  }

  def put(groupKey: UnsafeRow, id: Long, isInsert: Boolean, row: UnsafeRow): Unit = {
    row.setInsert(isInsert)
    stateStore.put(keyWithMetaRow(groupKey, id), row)
    row.cleanStates()
  }

  // def remove(groupKey: UnsafeRow, maxID: Long, oldRow: UnsafeRow): Unit = {
  //   breakable(
  //     for (id <- 0L to maxID) {
  //       if (id == maxID) {
  //         logError(s"Not found the row in fullmap")
  //         break
  //       }

  //       val tmpKey = keyWithIndexRow(groupKey, id)
  //       val tmpRow = stateStore.get(tmpKey)
  //       if (tmpRow != null && tmpRow.equals(oldRow)) {
  //         stateStore.remove(tmpKey)
  //         break
  //       }
  //     }
  //   )
  // }

  def getNumKeys: Long = {
    stateStore.metrics.numKeys
  }

  def getMemoryConsumption: Long = {
    stateStore.metrics.memoryUsedBytes
  }

  def commit(): Unit = {
    // We need to remove late data first
    if (watermarkForData.isDefined) {
      stateStore.getRange(None, None)
        .foreach(rowPair =>
          if (watermarkForData.get.eval(rowPair.value)) {
            stateStore.remove(rowPair.key)
          })
    }

    stateStore.commit()
    logDebug("Committed, metrics = " + stateStore.metrics)
  }

  def abortIfNeeded(): Unit = {
    if (!stateStore.hasCommitted) {
      logInfo(s"Aborted store ${stateStore.id}")
      stateStore.abort()
    }
  }

  def metrics: StateStoreMetrics = stateStore.metrics

  /** Get the StateStore with the given schema */
  private def getStateStore(keySchema: StructType, valueSchema: StructType): StateStore = {
    val storeProviderId = StateStoreProviderId(
      stateInfo.get, TaskContext.getPartitionId(), storeName)
    val store = StateStore.get(
      storeProviderId, keySchema, valueSchema, None,
      stateInfo.get.storeVersion, storeConf, hadoopConf)
    logInfo(s"Loaded store ${store.id}")
    store
  }

  def getGroupIteratorbyExpr(nonIncMetaPerExpr: NonIncMetaPerExpr,
                             exprIndex: Int,
                             hashMapforMetaData: SlothAggMetaMap): Iterator[UnsafeRowPair] = {
    val aggFunc = nonIncMetaPerExpr.aggExpr.aggregateFunction
    val rowOffset = nonIncMetaPerExpr.rowOffset
    val dataType = nonIncMetaPerExpr.dataType

    // Check whether two groups are the same
    val eqGroupFunc = (groupA: UnsafeRow, groupB: UnsafeRow) => {
      val groupFields = keySchema.size
      val aID = groupA.getLong(groupFields)
      val bID = groupB.getLong(groupFields)
      groupA.setLong(groupFields, 0)
      groupB.setLong(groupFields, 0)

      val result = groupA.equals(groupB)

      groupA.setLong(groupFields, aID)
      groupB.setLong(groupFields, bID)

      result
    }: Boolean

    /**
     * Compare two groups
     * 0  -> equal
     * -1 -> less than
     * +1 -> larger than
     */
    val compGroupFunc = (groupA: UnsafeRow, groupB: UnsafeRow) => {
      var result: Int = 0
      if (groupA.getSizeInBytes < groupB.getSizeInBytes) {
        result = -1
      } else if (groupA.getSizeInBytes > groupB.getSizeInBytes) {
        result = 1
      } else {
        val groupFields = keySchema.size
        val aID = groupA.getLong(groupFields)
        val bID = groupB.getLong(groupFields)
        groupA.setLong(groupFields, 0)
        groupB.setLong(groupFields, 0)

        val aBytes = groupA.getBytes
        val bBytes = groupB.getBytes

        result = 0

        breakable(
          for (i <- 0 until aBytes.length) {
            if (aBytes(i) < bBytes(i)) {
              result = -1
              break
            } else if (aBytes(i) > bBytes(i)) {
              result = 1
              break
            }
          }
        )

        groupA.setLong(groupFields, aID)
        groupB.setLong(groupFields, bID)
      }

      result
    }: Int

    val insertCompFunc = (rowA: UnsafeRow, rowB: UnsafeRow) => {
      val isInsertA = rowA.isInsert
      val isInsertB = rowB.isInsert

      // Delete comes first
      if (!isInsertA && isInsertB) true
      else false
    }: Boolean

    // -1 less than
    // 0 equal
    // 1 larger than
    val compRowFunc = dataType match {
      case DoubleType =>
        (rowA: UnsafeRow, rowB: UnsafeRow) => {
          val valueA = rowA.getDouble(rowOffset)
          val valueB = rowB.getDouble(rowOffset)
          if (valueA < valueB) -1
          else if (valueA == valueB) 0
          else 1
        }: Int
      case IntegerType =>
         (rowA: UnsafeRow, rowB: UnsafeRow) => {
          val valueA = rowA.getInt(rowOffset)
          val valueB = rowB.getInt(rowOffset)
          if (valueA < valueB) -1
          else if (valueA == valueB) 0
          else 1
        }: Int
      case LongType =>
        (rowA: UnsafeRow, rowB: UnsafeRow) => {
          val valueA = rowA.getLong(rowOffset)
          val valueB = rowB.getLong(rowOffset)
          if (valueA < valueB) -1
          else if (valueA == valueB) 0
          else 1
        }: Int
      case _ =>
        throw new IllegalArgumentException(s"Not supported ${dataType}")
    }

    val wrapCompRowFunc = aggFunc match {
      case _: Min =>
        (rowA: UnsafeRow, rowB: UnsafeRow) => {
          val rowResult = compRowFunc(rowA, rowB)
          if (rowResult == -1) true
          else if (rowResult == 0) insertCompFunc(rowA, rowB)
          else false
        }
      // Max
      case _ =>
        (rowA: UnsafeRow, rowB: UnsafeRow) => {
          val rowResult = compRowFunc(rowA, rowB)
          if (rowResult == -1) false
          else if (rowResult == 0) insertCompFunc(rowA, rowB)
          else true
        }
    }

    val sortFunc = (pairA: UnsafeRowPair, pairB: UnsafeRowPair) => {
      val ret = compGroupFunc(pairA.key, pairB.key)
      if (ret < 0) {
        true
      } else if (ret > 0) {
        false
      } else {
        wrapCompRowFunc(pairA.value, pairB.value)
      }
    }: Boolean


    val pairSeq = stateStore.iterator().map(p => new UnsafeRowPair(p.key, p.value))
      .filter((p: UnsafeRowPair) => {
        val groupKey =
          if (groupExpressions.isEmpty) {
            keyWithoutIndexGenerator.apply(null)
          } else {
            keyWithoutIndexGenerator(p.value)
          }
        hashMapforMetaData.getHasChange(groupKey, exprIndex)
      })
      .toSeq

    var deleteCounter = 0
    val sortedWithoutDeletes = pairSeq.sortWith(sortFunc)
      .filter(pair => {
        val key = pair.key
        val value = pair.value
        if (value.isInsert && deleteCounter == 0) true
        else {
          stateStore.remove(key)
          if (value.isInsert) deleteCounter -= 1
          else deleteCounter += 1
          false
        }
      })

    new SortedIterator(sortedWithoutDeletes, eqGroupFunc)
  }

  private class SortedIterator (sortedSeq: Seq[UnsafeRowPair],
                                eqGroupFunc: (UnsafeRow, UnsafeRow) => Boolean)
    extends Iterator[UnsafeRowPair] {

    var index = 0
    val len = sortedSeq.length
    val retWithoutIndex = new UnsafeRowPair

    override def hasNext(): Boolean = {
      index != len
    }

    override def next(): UnsafeRowPair = {
      val ret = sortedSeq(index)
      index += 1
      while (index != len && eqGroupFunc(ret.key, sortedSeq(index).key)) {
        index += 1
      }
      retWithoutIndex.key = if (groupExpressions.isEmpty) keyWithoutIndexGenerator.apply(null)
                            else keyWithoutIndexGenerator(ret.value)
      retWithoutIndex.value = ret.value
      retWithoutIndex
    }
  }
}
