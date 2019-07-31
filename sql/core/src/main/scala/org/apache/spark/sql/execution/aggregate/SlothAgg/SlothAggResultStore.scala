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
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.codegen.Predicate
import org.apache.spark.sql.execution.streaming.StatefulOperatorStateInfo
import org.apache.spark.sql.execution.streaming.state._
import org.apache.spark.sql.types._

class SlothAggResultStore(
    groupExpressions: Seq[NamedExpression],
    aggBufferAttributes: Seq[Attribute],
    var stateInfo: Option[StatefulOperatorStateInfo],
    var storeConf: StateStoreConf,
    var hadoopConf: Configuration,
    watermarkForKey: Option[Predicate]) extends Logging {

  private val keySchema = StructType(groupExpressions.zipWithIndex.map {
      case (k, i) => StructField(s"field$i", k.dataType, k.nullable)})
  private val keySchemaWithFlag = keySchema.add("is new", "boolean")

  private val keyAttributes = keySchema.toAttributes
  private val keyAttributesWithFlag = keyAttributes :+ Literal(true)

  private val groupKeyWithFlagProj =
    UnsafeProjection.create(keyAttributesWithFlag, keyAttributes)
  private val groupKeyProj = UnsafeProjection
    .create(keyAttributes, keyAttributes :+ AttributeReference("index", BooleanType)())

  private val flagIndex = keySchema.size
  private val OLD = false
  private val NEW = true

  private val valSchema = aggBufferAttributes.toStructType

  private val storeName = "GroupKeyToResultDataStore"

  private var stateStore = getStateStore(keySchemaWithFlag, valSchema)

  def reInit(stateInfo: Option[StatefulOperatorStateInfo],
             storeConf: StateStoreConf,
             hadoopConf: Configuration): Unit = {
    this.stateInfo = stateInfo
    this.storeConf = storeConf
    this.hadoopConf = hadoopConf
    stateStore = getStateStore(keySchemaWithFlag, valSchema)
  }

  def purge(): Unit = {

  }

  def getOld(groupKey: UnsafeRow): UnsafeRow = {
    val keyWithFlag = groupKeyWithFlagProj(groupKey)
    keyWithFlag.setBoolean(flagIndex, OLD)
    stateStore.get(keyWithFlag)
  }

  def getNew(groupKey: UnsafeRow): UnsafeRow = {
    val keyWithFlag = groupKeyWithFlagProj(groupKey)
    keyWithFlag.setBoolean(flagIndex, NEW)
    stateStore.get(keyWithFlag)
  }

  def putOld(groupKey: UnsafeRow, buffer: UnsafeRow): Unit = {
    val keyWithFlag = groupKeyWithFlagProj(groupKey)
    keyWithFlag.setBoolean(flagIndex, OLD)
    stateStore.put(keyWithFlag, buffer)
  }

  def putNew(groupKey: UnsafeRow, buffer: UnsafeRow): Unit = {
    val keyWithFlag = groupKeyWithFlagProj(groupKey)
    keyWithFlag.setBoolean(flagIndex, NEW)
    stateStore.put(keyWithFlag, buffer)
  }

  def remove(groupKey: UnsafeRow): Unit = {
    val keyWithFlag = groupKeyWithFlagProj(groupKey)

    keyWithFlag.setBoolean(flagIndex, OLD)
    stateStore.remove(keyWithFlag)

    keyWithFlag.setBoolean(flagIndex, NEW)
    stateStore.remove(keyWithFlag)
  }

  def iterator(): Iterator[UnsafeRowPair] = {
    val rowPair = new UnsafeRowPair()
    stateStore.getRange(None, None)
      .filter(pair => {
        val keyWithFlag = pair.key
        keyWithFlag.getBoolean(flagIndex) == NEW
      })
      .map(pair => {
        val groupKey = groupKeyProj(pair.key)
        rowPair.withRows(groupKey, pair.value)
      })
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

    stateStore.commit()
    logDebug("Committed, metrics = " + stateStore.metrics)
  }

  def abortIfNeeded(): Unit = {
    if (!stateStore.hasCommitted) {
      logInfo(s"Aborted store ${stateStore.id}")
      stateStore.abort()
    }
  }

  def getNumKeys: Long = {
    metrics.numKeys
  }

  def getMemoryConsumption: Long = {
    metrics.memoryUsedBytes
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
