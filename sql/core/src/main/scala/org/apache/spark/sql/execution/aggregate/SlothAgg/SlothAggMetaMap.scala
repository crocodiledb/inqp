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

import scala.collection.mutable.HashMap

import org.apache.hadoop.conf.Configuration

import org.apache.spark.TaskContext
import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.aggregate._
import org.apache.spark.sql.catalyst.expressions.codegen.Predicate
import org.apache.spark.sql.execution.streaming.StatefulOperatorStateInfo
import org.apache.spark.sql.execution.streaming.state._
import org.apache.spark.sql.types._

case class NonIncMetaPerExpr(aggExpr: AggregateExpression,
                             bufOffset: Int,
                             rowOffset: Int,
                             dataType: DataType)

case class AggMetaData (var counter: Int, val oldMaxID: Long,
                   var newMaxID: Long, val hasChange: Array[Boolean])

class SlothAggMetaMap (
    groupExpressions: Seq[NamedExpression],
    nonIncExprNum: Int,
    stateInfo: Option[StatefulOperatorStateInfo],
    storeConf: StateStoreConf,
    hadoopConf: Configuration,
    watermarkForKey: Option[Predicate],
    repairMode: Boolean) {

  private var hashMap = HashMap.empty[UnsafeRow, AggMetaData]
  private val metaStore = new GroupKeytoMetaStore(groupExpressions,
    stateInfo, storeConf, hadoopConf, watermarkForKey, repairMode)
  private val counterIndex = 0
  private val maxIdIndex = 1
  private val hasChangeIndex = 2

  def reInit(stateInfo: Option[StatefulOperatorStateInfo],
             storeConf: StateStoreConf,
             hadoopConf: Configuration,
             repairMode: Boolean): Unit = {
    hashMap = HashMap.empty[UnsafeRow, AggMetaData]
    metaStore.reInit(stateInfo, storeConf, hadoopConf, repairMode)
  }

  def purge(): Unit = {
    hashMap = null
  }

  def newEntry(groupkey: UnsafeRow): Unit = {
    if (repairMode) hashMap += (groupkey.copy() -> metaStore.get(groupkey))
  }

  def incCounter(groupkey: UnsafeRow): Unit = {
    if (repairMode) hashMap(groupkey).counter += 1
    else {
      val rawVal = metaStore.getRaw(groupkey)
      val counter = rawVal.getInt(counterIndex)
      rawVal.setInt(counterIndex, counter + 1)
    }
  }

  def decCounter(groupkey: UnsafeRow): Unit = {
    if (repairMode) {
      val metaData = hashMap(groupkey)
      metaData.counter -= 1
      assert(metaData.counter >= 0,
        "AGG Counter should never be less than 0")
    } else {
      val rawVal = metaStore.getRaw(groupkey)
      val counter = rawVal.getInt(counterIndex)
      rawVal.setInt(counterIndex, counter - 1)
      assert(counter >= 1,
        "AGG Counter should never be less than 0")
    }
  }

  def getCounter(groupkey: UnsafeRow): Int = {
    if (repairMode) hashMap(groupkey).counter
    else metaStore.getRaw(groupkey).getInt(counterIndex)
  }

  def allocID(groupkey: UnsafeRow): Long = {
    if (repairMode) {
      val newMaxID = hashMap(groupkey).newMaxID
      hashMap(groupkey).newMaxID = newMaxID + 1
      newMaxID
    } else {
      val rawVal = metaStore.getRaw(groupkey)
      val newMaxID = rawVal.getLong(maxIdIndex)
      rawVal.setLong(maxIdIndex, newMaxID + 1)
      newMaxID
    }
  }

  def getOldMaxID(groupkey: UnsafeRow): Long = {
    if (repairMode) hashMap(groupkey).oldMaxID
    else throw new IllegalArgumentException("Repair mode does not support getOldMaxID")
  }

  def getNewMaxID(groupkey: UnsafeRow): Long = {
    if (repairMode) hashMap(groupkey).newMaxID
    else metaStore.getRaw(groupkey).getLong(maxIdIndex)
  }

  def setHasChange(groupkey: UnsafeRow, exprIndex: Int): Unit = {
    if (repairMode) hashMap(groupkey).hasChange(exprIndex) = true
    else {
      val rawValue = metaStore.getRaw(groupkey)
      rawValue.setBoolean(hasChangeIndex, true)
    }
  }

  def getHasChange(groupkey: UnsafeRow, exprIndex: Int): Boolean = {
    if (repairMode) {
      val metaData = hashMap.get(groupkey)
      if (metaData.isDefined) {
        metaData.get.hasChange(exprIndex)
      }
      else false
    } else {
      val rawValue = metaStore.getRaw(groupkey)
      rawValue.getBoolean(hasChangeIndex)
    }
  }

  // def isNewGroup(groupkey: UnsafeRow): Boolean = {
  //   hashMap(groupkey).isNewGroup
  // }

  def commit(): Unit = {
    metaStore.commit()
  }

  def abort(): Unit = {
    metaStore.abortIfNeeded()
  }

  def getNumKeys: Long = {
    metaStore.metrics.numKeys
  }

  def getMemoryConsumption: Long = {
    metaStore.metrics.memoryUsedBytes
  }

  def saveToStateStore(): Unit = {
    if (repairMode) {
      hashMap.iterator.foreach(pair => {
        val groupKey = pair._1
        val aggMetaData = pair._2
        if (aggMetaData.counter != 0 ) {
          metaStore.put(groupKey, aggMetaData)
        } else {
          // counter equals 0 and is not a new group
          metaStore.remove(groupKey)
        }
      })
    }
  }

  private class GroupKeytoMetaStore (
    groupExpression: Seq[NamedExpression],
    var stateInfo: Option[StatefulOperatorStateInfo],
    var storeConf: StateStoreConf,
    var hadoopConf: Configuration,
    watermarkForKey: Option[Predicate],
    repairMode: Boolean) extends Logging {

    private val storeName = "GroupKeytoMetaStore"

    private val keySchema = StructType(
      groupExpression.zipWithIndex
        .map{case (k, i) => StructField(s"field$i", k.dataType, k.nullable)})

    private val valSchema = new StructType()
      .add("counter", "int")
      .add("maxid", "long")
      .add("hasChange", "boolean")
    private val valueProj = UnsafeProjection.create(valSchema)
    private val valueRow = valueProj(new SpecificInternalRow(valSchema))

    private var stateStore = getStateStore(keySchema, valSchema)

    def reInit(stateInfo: Option[StatefulOperatorStateInfo],
               storeConf: StateStoreConf,
               hadoopConf: Configuration,
               repairMode: Boolean): Unit = {
      this.stateInfo = stateInfo
      this.storeConf = storeConf
      this.hadoopConf = hadoopConf
      stateStore = getStateStore(keySchema, valSchema)
    }

    /** Get the meta data for a group key */
    def get(key: UnsafeRow): AggMetaData = {
      // False is the default value for Boolean
      val nonIncHasChange = new Array[Boolean](nonIncExprNum)
      val tmpValRow = stateStore.get(key)
      if (tmpValRow == null) {
        new AggMetaData(0, 0L, 0L, nonIncHasChange)
      } else {
        if (tmpValRow.getBoolean(hasChangeIndex)) {
          nonIncHasChange.zipWithIndex.foreach(pair => nonIncHasChange(pair._2) = true)
        }
        new AggMetaData(tmpValRow.getInt(counterIndex), tmpValRow.getLong(maxIdIndex),
          tmpValRow.getLong(maxIdIndex), nonIncHasChange)
      }
    }

    def getRaw(key: UnsafeRow): UnsafeRow = {
      val tmpValRow = stateStore.get(key)
      if (tmpValRow == null) {
        valueRow.setInt(counterIndex, 0)
        valueRow.setLong(maxIdIndex, 0L)
        valueRow.setBoolean(hasChangeIndex, false)
        stateStore.put(key, valueRow)
      }
      stateStore.get(key)
    }

    /** Set the meta info for a group key */
    def put(key: UnsafeRow, value: AggMetaData): Unit = {
      require(value.counter > 0,
        s"group counter should be larger than 0 when it is written into store")
      valueRow.setInt(counterIndex, value.counter)
      valueRow.setLong(maxIdIndex, value.newMaxID)
      valueRow.setBoolean(hasChangeIndex, value.hasChange.exists(_ == true))
      stateStore.put(key, valueRow)
    }

    def remove(key: UnsafeRow): Unit = {
      stateStore.remove(key)
    }

    def commit(): Unit = {
      // We need to remove late data first
      if (watermarkForKey.isDefined) {
        stateStore.getRange(None, None)
          .foreach(rowPair =>
            if (watermarkForKey.get.eval(rowPair.key)) {
              stateStore.remove(rowPair.key)
            })
      }

      // Now, commit
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
  }
}


