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

package org.apache.spark.sql.execution.streaming.state

import scala.collection.mutable.HashMap

import org.apache.hadoop.conf.Configuration

import org.apache.spark.TaskContext
import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.expressions.{Expression, SpecificInternalRow, UnsafeProjection, UnsafeRow}
import org.apache.spark.sql.catalyst.expressions.codegen.Predicate
import org.apache.spark.sql.execution.streaming.StatefulOperatorStateInfo
import org.apache.spark.sql.types.{StructField, StructType}

private case class DedupMetaData (var counter: Int, val isNewGroup: Boolean)

class SlothDeduplicateStateManager(
                                    keyExpressions: Seq[Expression],
                                    stateInfo: Option[StatefulOperatorStateInfo],
                                    storeConf: StateStoreConf,
                                    hadoopConf: Configuration) extends Logging {

  private val hashMap = HashMap.empty[UnsafeRow, DedupMetaData]
  private val dedupStore = new DedupStore(keyExpressions,
    stateInfo, storeConf, hadoopConf)

  private def get(key: UnsafeRow): DedupMetaData = {
    hashMap.get(key) match {
      case optionMeta: Option[DedupMetaData] if optionMeta.isDefined =>
        optionMeta.get
      case _ =>
        val newMeta = dedupStore.get(key)
        hashMap += (key.copy() -> newMeta)
        newMeta
    }
  }

  def insert(key: UnsafeRow): Unit = {
    val dedupMeta = get(key)
    dedupMeta.counter += 1
  }

  def delete(key: UnsafeRow): Unit = {
    val dedupMeta = get(key)
    dedupMeta.counter -= 1
  }

  def distinctIterAndWriteStore(): Iterator[UnsafeRow] = {
    hashMap.iterator.filter(pair => {
      val key = pair._1
      val dedupMeta = pair._2
      if (dedupMeta.counter != 0) {
        dedupStore.put(key, dedupMeta)
      } else if (!dedupMeta.isNewGroup) {
        dedupStore.remove(key)
      }
      if ((dedupMeta.isNewGroup && dedupMeta.counter != 0)
        || (!dedupMeta.isNewGroup && dedupMeta.counter == 0)) {
        true
      }
      else false
    }).map(pair => {
      val key = pair._1
      val isInsert = pair._2.isNewGroup
      key.cleanStates()
      key.setInsert(isInsert)
      key
    })
  }

  def commit(): Unit = {
    dedupStore.commit()
  }

  def abort(): Unit = {
    dedupStore.abortIfNeeded()
  }

  private class DedupStore (
    keyExpressions: Seq[Expression],
    stateInfo: Option[StatefulOperatorStateInfo],
    storeConf: StateStoreConf,
    hadoopConf: Configuration) extends Logging {

    private val storeName = "DedupStore"

    private val keySchema = StructType(
      keyExpressions.zipWithIndex
        .map{case (k, i) => StructField(s"field$i", k.dataType, k.nullable)})

    private val valSchema = new StructType().add("counter", "int")
    private val valueProj = UnsafeProjection.create(valSchema)
    private val valueRow = valueProj(new SpecificInternalRow(valSchema))

    private val stateStore = getStateStore(keySchema, valSchema)

    /** Get the meta data for a group key */
    def get(key: UnsafeRow): DedupMetaData = {
      // False is the default value for Boolean
      val tmpValRow = stateStore.get(key)
      if (tmpValRow == null) {
        new DedupMetaData(0, true)
      }
      else {
        new DedupMetaData(tmpValRow.getInt(0), false)
      }
    }

    /** Set the meta info for a group key */
    def put(key: UnsafeRow, value: DedupMetaData): Unit = {
      require(value.counter > 0,
        s"dedup counter should be larger than 0 when it is written into store")
      valueRow.setInt(0, value.counter)
      stateStore.put(key, valueRow)
    }

    def remove(key: UnsafeRow): Unit = {
      stateStore.remove(key)
    }

    def commit(): Unit = {
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
