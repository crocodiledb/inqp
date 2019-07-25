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
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeReference, Literal, SpecificInternalRow, UnsafeProjection, UnsafeRow}
import org.apache.spark.sql.execution.streaming.StatefulOperatorStateInfo
import org.apache.spark.sql.execution.streaming.StreamingSymmetricHashJoinHelper._
import org.apache.spark.sql.types.{IntegerType, LongType, StructField, StructType}
import org.apache.spark.util.NextIterator

class SlothThetaJoinStateManager (
    val joinSide: JoinSide,
    inputValueAttributes: Seq[Attribute],
    var stateInfo: Option[StatefulOperatorStateInfo],
    var storeConf: StateStoreConf,
    var hadoopConf: Configuration) extends Logging {

  import SlothThetaJoinStateManager._

  def reInit(stateInfo: Option[StatefulOperatorStateInfo],
                 storeConf: StateStoreConf,
                 hadoopConf: Configuration): Unit = {
    this.stateInfo = stateInfo
    this.storeConf = storeConf
    this.hadoopConf = hadoopConf
    keyToNumValues.reInit()
    keyWithIndexToValue.reInit()
  }

  def purgeState(): Unit = {
    keyToNumValues.purgeState()
    keyWithIndexToValue.purgeState()
  }

  /*
  =====================================================
                  Public methods
  =====================================================
   */

  def get(key: UnsafeRow, value: UnsafeRow): UnsafeRow = {
    val numValues = keyToNumValues.get(key)
    val kvRow = keyWithIndexToValue.getAll(key, numValues).find(kvRow =>
      kvRow.value.equals(value))
    if (kvRow.isDefined) kvRow.get.value
    else null
  }

  /** Append a new value to the key */
  def append(key: UnsafeRow, value: UnsafeRow): Unit = {
    val numExistingValues = keyToNumValues.get(key)
    keyWithIndexToValue.put(key, numExistingValues, value)
    keyToNumValues.put(key, numExistingValues + 1)
  }

  def remove(key: UnsafeRow, value: UnsafeRow): Unit = {
    val numValues = keyToNumValues.get(key)
    val kvRow = keyWithIndexToValue.getAll(key, numValues).find(kvRow =>
      kvRow.value.equals(value))
    require(kvRow.isDefined, "We must find the KV when removing a record from the state")

    keyWithIndexToValue.remove(key, kvRow.get.valueIndex)
  }

  /**
   * Remove using a predicate on keys
   *
   */
  def getAllAndRemove(windowCondition: UnsafeRow => Boolean,
                      removalCondition: UnsafeRow => Boolean):
  Iterator[UnsafeRow] = {
    new NextIterator[UnsafeRow] {

      private var allKeyToNumValues: Iterator[KeyAndNumValues] = _

      private var currentValues: Iterator[KeyWithIndexAndValue] = _

      private var currentKey: UnsafeRow = _
      private var currentNumValues: Long = _

      override protected def getNext(): UnsafeRow = {
        // If there are more values for the current key, remove and return the next one.
        if (currentValues != null) {
           while (currentValues.hasNext) {
            val kvWithIndex = currentValues.next()
            if (removalCondition(kvWithIndex.value)) {
              keyWithIndexToValue.remove(currentKey, kvWithIndex.valueIndex)
            } else if (windowCondition(kvWithIndex.value)) {
              return kvWithIndex.value
            }
          }
        }

        if (allKeyToNumValues == null) {
          allKeyToNumValues = keyToNumValues.getIterator()
        }

        // If there weren't any values left, try and find the next key that satisfies the removal
        // condition and has values.
        while (allKeyToNumValues.hasNext) {
          val currentKeyToNumValue = allKeyToNumValues.next()
          currentKey = currentKeyToNumValue.key
          currentNumValues = currentKeyToNumValue.numValue

          currentValues = keyWithIndexToValue.getAll(currentKey, currentNumValues)
          while (currentValues.hasNext) {
            val kvWithIndex = currentValues.next()
            if (removalCondition(kvWithIndex.value)) {
              keyWithIndexToValue.remove(currentKey, kvWithIndex.valueIndex)
            } else if (windowCondition(kvWithIndex.value)) {
              return kvWithIndex.value
            }
          }
        }

        // We only reach here if there were no satisfying keys left, which means we're done.
        finished = true
        return null
      }

      override def close: Unit = {}
    }
  }

  /** Commit all the changes to all the state stores */
  def commit(): Unit = {
    keyToNumValues.commit()
    keyWithIndexToValue.commit()
  }

  /** Abort any changes to the state stores if needed */
  def abortIfNeeded(): Unit = {
    keyToNumValues.abortIfNeeded()
    keyWithIndexToValue.abortIfNeeded()
  }

  /** Get the combined metrics of all the state stores */
  def metrics: StateStoreMetrics = {
    val keyToNumValuesMetrics = keyToNumValues.metrics
    val keyWithIndexToValueMetrics = keyWithIndexToValue.metrics
    def newDesc(desc: String): String = s"${joinSide.toString.toUpperCase}: $desc"

    StateStoreMetrics(
      keyWithIndexToValueMetrics.numKeys,       // represent each buffered row only once
      keyToNumValuesMetrics.memoryUsedBytes + keyWithIndexToValueMetrics.memoryUsedBytes,
      keyWithIndexToValueMetrics.customMetrics.map {
        case (s @ StateStoreCustomSumMetric(_, desc), value) =>
          s.copy(desc = newDesc(desc)) -> value
        case (s @ StateStoreCustomSizeMetric(_, desc), value) =>
          s.copy(desc = newDesc(desc)) -> value
        case (s @ StateStoreCustomTimingMetric(_, desc), value) =>
          s.copy(desc = newDesc(desc)) -> value
        case (s, _) =>
          throw new IllegalArgumentException(
            s"Unknown state store custom metric is found at metrics: $s")
      }
    )
  }

  /*
  =====================================================
            Private methods and inner classes
  =====================================================
   */

  private val keySchema = StructType(StructField("hashCode", IntegerType) :: Nil)
  private val valueSchema = StructType(
    inputValueAttributes.zipWithIndex.
      map { case (k, i) => StructField(s"field$i", k.dataType, k.nullable) })
  private val keyAttributes = keySchema.toAttributes
  private val keyToNumValues = new KeyToNumValuesStore()
  private val keyWithIndexToValue = new KeyWithIndexToValueStore()

  // Clean up any state store resources if necessary at the end of the task
  Option(TaskContext.get()).foreach { _.addTaskCompletionListener[Unit] { _ => abortIfNeeded() } }

  /** Helper trait for invoking common functionalities of a state store. */
  private abstract class StateStoreHandler(stateStoreType: StateStoreType) extends Logging {

    /** StateStore that the subclasses of this class is going to operate on */
    protected def stateStore: StateStore

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
    protected def getStateStore(keySchema: StructType, valueSchema: StructType): StateStore = {
      val storeProviderId = StateStoreProviderId(
        stateInfo.get, TaskContext.getPartitionId(), getStateStoreName(joinSide, stateStoreType))
      val store = StateStore.get(
        storeProviderId, keySchema, valueSchema, None,
        stateInfo.get.storeVersion, storeConf, hadoopConf)
      logInfo(s"Loaded store ${store.id}")
      store
    }
  }

  /**
   * Helper class for representing data returned by [[KeyWithIndexToValueStore]].
   * Designed for object reuse.
   */
  private case class KeyAndNumValues(var key: UnsafeRow = null, var numValue: Long = 0) {
    def withNew(newKey: UnsafeRow, newNumValues: Long): this.type = {
      this.key = newKey
      this.numValue = newNumValues
      this
    }
  }

  /**
   * A wrapper around a [[StateStore]] that stores [key -> number of values].
   * TODO: an alternative implementation is to use the statestore directly
   */
  private class KeyToNumValuesStore extends StateStoreHandler(KeyToNumValuesType) {
    private val longValueSchema = new StructType().add("value", "long")
    private val longToUnsafeRow = UnsafeProjection.create(longValueSchema)
    private val valueRow = longToUnsafeRow(new SpecificInternalRow(longValueSchema))
    protected var stateStore: StateStore = getStateStore(keySchema, longValueSchema)
    private var loadedStore = false

    private var hashMap: HashMap[UnsafeRow, Long] = HashMap.empty[UnsafeRow, Long]

    /** Get the number of values the key has */
    def get(key: UnsafeRow): Long = {
      val longValueRow = hashMap.get(key)
      if (longValueRow.isDefined) longValueRow.get
      else {
        val value = stateStore.get(key)
        if (value == null) 0L
        else {
          val count = value.getLong(0)
          hashMap.put(key.copy(), count)
          count
        }
      }
    }

    /** Set the number of values the key has */
    def put(key: UnsafeRow, numValues: Long): Unit = {
      require(numValues > 0)
      if (hashMap.contains(key)) {
        hashMap(key) = numValues
      } else {
        hashMap.put(key.copy(), numValues)
      }
    }

    def remove(key: UnsafeRow): Unit = {
      hashMap.remove(key)
      stateStore.remove(key)
    }

    private def loadStore(): Unit = {
      loadedStore = true
      stateStore.getRange(None, None).foreach(pair => {
        val key = pair.key
        val count = pair.value.getLong(0)
        if (!hashMap.contains(key)) hashMap.put(key.copy(), count)
      })
    }

    def getIterator(): Iterator[KeyAndNumValues] = {
      if (!loadedStore) loadStore()

      val keyAndNumValues = KeyAndNumValues()
      hashMap.iterator.map(pair => {
        keyAndNumValues.withNew(pair._1, pair._2)
      })
    }

    private def saveToStateStore(): Unit = {
      hashMap.iterator.foreach(pair => {
        val key = pair._1
        valueRow.setLong(0, pair._2)
        stateStore.put(key, valueRow)
      })
    }

    override def commit(): Unit = {
      saveToStateStore()
      super.commit()
    }

    def reInit(): Unit = {
      stateStore = getStateStore(keySchema, longValueSchema)
      hashMap = HashMap.empty[UnsafeRow, Long]
      loadedStore = false
    }

    def purgeState(): Unit = {
      hashMap = null
    }
  }

  private case class KeyWithIndexAndValue(
    var key: UnsafeRow = null, var valueIndex: Long = -1,
    var value: UnsafeRow = null) {
    def withNew(newKey: UnsafeRow, newIndex: Long,
                newValue: UnsafeRow): this.type = {
      this.key = newKey
      this.valueIndex = newIndex
      this.value = newValue
      this
    }
  }

  /** A wrapper around a [[StateStore]] that stores [(key, index) -> value]. */
  private class KeyWithIndexToValueStore extends StateStoreHandler(KeyWithIndexToValueType) {
    private val keyWithIndexExprs = keyAttributes :+ Literal(1L)
    private val keyWithIndexSchema = keySchema.add("index", LongType)
    private val indexOrdinalInKeyWithIndexRow = keyAttributes.size

    // Projection to generate (key + index) row from key row
    private val keyWithIndexRowGenerator = UnsafeProjection.create(keyWithIndexExprs, keyAttributes)

    // Projection to generate key row from (key + index) row
    private val keyRowGenerator = UnsafeProjection.create(
      keyAttributes, keyAttributes :+ AttributeReference("index", LongType)())

    protected var stateStore = getStateStore(keyWithIndexSchema, valueSchema)

    /**
     * Get all values and indices for the provided key.
     * Should not return null.
     */
    def getAll(key: UnsafeRow, numValues: Long): Iterator[KeyWithIndexAndValue] = {
      val keyWithIndexAndValue = KeyWithIndexAndValue()
      var index = -1
      new NextIterator[KeyWithIndexAndValue] {
        override protected def getNext(): KeyWithIndexAndValue = {
          var retKV: KeyWithIndexAndValue = null
          while (index < numValues && retKV == null) {
            index += 1
            val keyWithIndex = keyWithIndexRow(key, index)
            val value = stateStore.get(keyWithIndex)
            if (value != null) retKV = keyWithIndexAndValue.withNew(key, index, value)
          }

          if (index >= numValues) finished = true
          retKV
        }

        override protected def close(): Unit = {}
      }
    }

    /** Put new value for key at the given index */
    def put(key: UnsafeRow, valueIndex: Long, value: UnsafeRow): Unit = {
      stateStore.put(keyWithIndexRow(key, valueIndex), value)
    }

    /**
     * Remove key and value at given index. Note that this will create a hole in
     * (key, index) and it is upto the caller to deal with it.
     */
    def remove(key: UnsafeRow, valueIndex: Long): Unit = {
      stateStore.remove(keyWithIndexRow(key, valueIndex))
    }

    /** Remove all values (i.e. all the indices) for the given key. */
    def removeAllValues(key: UnsafeRow, numValues: Long): Unit = {
      var index = 0
      while (index < numValues) {
        stateStore.remove(keyWithIndexRow(key, index))
        index += 1
      }
    }

    // def iterator: Iterator[KeyWithIndexAndValue] = {
    //   val keyWithIndexAndValue = new KeyWithIndexAndValue()
    //   stateStore.getRange(None, None).map { pair =>
    //     keyWithIndexAndValue.withNew(
    //       keyRowGenerator(pair.key), pair.key.getLong(indexOrdinalInKeyWithIndexRow), pair.value)
    //     keyWithIndexAndValue
    //   }
    // }

    /** Generated a row using the key and index */
    private def keyWithIndexRow(key: UnsafeRow, valueIndex: Long): UnsafeRow = {
      val row = keyWithIndexRowGenerator(key)
      row.setLong(indexOrdinalInKeyWithIndexRow, valueIndex)
      row
    }

    def reInit(): Unit = {
      stateStore = getStateStore(keyWithIndexSchema, valueSchema)
    }

    def purgeState(): Unit = {
    }

  }
}

object SlothThetaJoinStateManager {

  def allStateStoreNames(joinSides: JoinSide*): Seq[String] = {
    val allStateStoreTypes: Seq[StateStoreType] =
      Seq(KeyToNumValuesType, KeyWithIndexToValueType)
    for (joinSide <- joinSides; stateStoreType <- allStateStoreTypes) yield {
      getStateStoreName(joinSide, stateStoreType)
    }
  }

  private sealed trait StateStoreType

  private case object KeyToNumValuesType extends StateStoreType {
    override def toString: String = "keyToNumValues"
  }

  private case object KeyWithIndexToValueType extends StateStoreType {
    override def toString: String = "keyWithIndexToValue"
  }

  private def getStateStoreName(joinSide: JoinSide, storeType: StateStoreType): String = {
    s"$joinSide-$storeType"
  }
}
