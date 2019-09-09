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
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeReference, Expression, Literal, SpecificInternalRow, UnsafeProjection, UnsafeRow}
import org.apache.spark.sql.execution.streaming.{StatefulOperatorStateInfo, StreamingSymmetricHashJoinExec}
import org.apache.spark.sql.execution.streaming.StreamingSymmetricHashJoinHelper._
import org.apache.spark.sql.types.{IntegerType, LongType, StructField, StructType}
import org.apache.spark.util.NextIterator


case class KeyWithIndexAndValueWithCounter(
  var key: UnsafeRow = null, var valueIndex: Long = -1,
  var valueWithCounter: UnsafeRow = null) {
  def withNew(newKey: UnsafeRow, newIndex: Long,
              newValueWithCounter: UnsafeRow): this.type = {
    this.key = newKey
    this.valueIndex = newIndex
    this.valueWithCounter = newValueWithCounter
    this
  }
}

// case class ValueWithCounter(var value: UnsafeRow = null, var counter: Int = 0) {
//   def withNew(newValue: UnsafeRow, newCounter: Int): this.type = {
//     this.value = newValue
//     this.counter = newCounter
//     this
//   }
//
//   def getCounter: Int = counter
//   def incCounter: Unit = {counter += 1}
//   def decCounter: Unit = {counter -= 1}
//   def getValue: UnsafeRow = value
// }

/**
 * Helper class to manage state required by a single side of [[StreamingSymmetricHashJoinExec]].
 * The interface of this class is basically that of a multi-map:
 * - Get: Returns an iterator of multiple values for given key
 * - Append: Append a new value to the given key
 * - Remove Data by predicate: Drop any state using a predicate condition on keys or values
 *
 * @param joinSide              Defines the join side
 * @param inputValueAttributes  Attributes of the input row which will be stored as value
 * @param joinKeys              Expressions to generate rows that will be used to key the value rows
 * @param stateInfo             Information about how to retrieve the correct version of state
 * @param storeConf             Configuration for the state store.
 * @param hadoopConf            Hadoop configuration for reading state data from storage
 *
 * Internally, the key -> multiple values is stored in two [[StateStore]]s.
 * - Store 1 ([[KeyToNumValuesStore]]) maintains mapping between key -> number of values
 * - Store 2 ([[KeyWithIndexToValueStore]]) maintains mapping between (key, index) -> value
 * - Put:   update count in KeyToNumValuesStore,
 *          insert new (key, count) -> value in KeyWithIndexToValueStore
 * - Get:   read count from KeyToNumValuesStore,
 *          read each of the n values in KeyWithIndexToValueStore
 * - Remove state by predicate on keys:
 *          scan all keys in KeyToNumValuesStore to find keys that do match the predicate,
 *          delete from key from KeyToNumValuesStore, delete values in KeyWithIndexToValueStore
 * - Remove state by condition on values:
 *          scan all [(key, index) -> value] in KeyWithIndexToValueStore to find values that match
 *          the predicate, delete corresponding (key, indexToDelete) from KeyWithIndexToValueStore
 *          by overwriting with the value of (key, maxIndex), and removing [(key, maxIndex),
 *          decrement corresponding num values in KeyToNumValuesStore
 */
class SlothHashJoinStateManager (
    val joinSide: JoinSide,
    inputValueAttributes: Seq[Attribute],
    joinKeys: Seq[Expression],
    var stateInfo: Option[StatefulOperatorStateInfo],
    var storeConf: StateStoreConf,
    var hadoopConf: Configuration) extends Logging {

  import SlothHashJoinStateManager._

  private var stateSize = 0

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

  def getStateSize(): Long = {
    return stateSize
  }

  /*
  =====================================================
                  Public methods
  =====================================================
   */

  /** Get all the values of a key */
  def getAll(key: UnsafeRow): Iterator[UnsafeRow] = {
    val numValues = keyToNumValues.get(key)
    keyWithIndexToValue.getAll(key, numValues).map(_.valueWithCounter)
  }

  def get(key: UnsafeRow, value: UnsafeRow): UnsafeRow = {
    val numValues = keyToNumValues.get(key)
    val kvRow = keyWithIndexToValue.getAll(key, numValues).find(kvRow =>
      keyWithIndexToValue.equalWithoutCounter(kvRow.valueWithCounter, value))
    if (kvRow.isDefined) kvRow.get.valueWithCounter
    else null
  }

  /** Get the most recent row that is just inserted */
  def getMostRecent(key: UnsafeRow): UnsafeRow = {
    val numExistingValues = keyToNumValues.get(key)
    require(numExistingValues > 0,
      "getting the most recent one " +
        "requires the number of existing values must be larger than 0")
    keyWithIndexToValue.get(key, numExistingValues - 1)
  }

  /** Append a new value to the key */
  def append(key: UnsafeRow, value: UnsafeRow): Unit = {
    val numExistingValues = keyToNumValues.get(key)
    keyWithIndexToValue.put(key, numExistingValues, value)
    keyToNumValues.put(key, numExistingValues + 1)

    stateSize += 1
  }

  def remove(key: UnsafeRow, value: UnsafeRow): Unit = {
    val numValues = keyToNumValues.get(key)
    val kvRow = keyWithIndexToValue.getAll(key, numValues).find(kvRow =>
      keyWithIndexToValue.equalWithoutCounter(kvRow.valueWithCounter, value))
    require(kvRow.isDefined, "We must find the KV when removing a record from the state")

    keyWithIndexToValue.remove(key, kvRow.get.valueIndex)

    stateSize -= 1
  }

  /**
   * Remove using a predicate on keys, and return all KV that are not removed
   *
   */
  def scanAndremoveByKeyCondition(removalCondition: UnsafeRow => Boolean, isScan: Boolean):
  Iterator[KeyWithIndexAndValueWithCounter] = {
    new NextIterator[KeyWithIndexAndValueWithCounter] {

      private var allKeyToNumValues: Iterator[KeyAndNumValues] = _

      private var currentValues: Iterator[KeyWithIndexAndValueWithCounter] = _

      private var currentKey: UnsafeRow = _
      private var currentNumValues: Long = _

      override protected def getNext(): KeyWithIndexAndValueWithCounter = {
        // If there are more values for the current key, remove and return the next one.
        if (currentValues != null && currentValues.hasNext) {
          return currentValues.next()
        }

        if (allKeyToNumValues == null) allKeyToNumValues = keyToNumValues.getIterator()

        // If there weren't any values left, try and find the next key that satisfies the removal
        // condition and has values.
        while (allKeyToNumValues.hasNext) {
          val currentKeyToNumValue = allKeyToNumValues.next()
          currentKey = currentKeyToNumValue.key
          currentNumValues = currentKeyToNumValue.numValue

          if (removalCondition(currentKey)) {
            keyWithIndexToValue.
              removeAllValues(currentKey, currentNumValues)
          } else if (isScan) {
            currentValues = keyWithIndexToValue.getAll(currentKey, currentNumValues)

            if (currentValues.hasNext) {
              return currentValues.next()
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

  private val keySchema = StructType(
    joinKeys.zipWithIndex.map { case (k, i) => StructField(s"field$i", k.dataType, k.nullable) })
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

    /** Get the number of values the key has */
    def get(key: UnsafeRow): Long = {
      val value = stateStore.get(key)
      if (value == null) 0L
      else value.getLong(0)
    }

    /** Set the number of values the key has */
    def put(key: UnsafeRow, numValues: Long): Unit = {
      require(numValues > 0)
      val value = stateStore.get(key)
      if (value == null) {
        valueRow.setLong(0, numValues)
        stateStore.put(key, valueRow)
      } else {
        value.setLong(0, numValues)
      }
    }

    def remove(key: UnsafeRow): Unit = {
      stateStore.remove(key)
    }

    def getIterator(): Iterator[KeyAndNumValues] = {
      val keyNumVal = new KeyAndNumValues()
      stateStore.getRange(None, None).map(pair =>
        keyNumVal.withNew(pair.key, pair.value.getLong(0))
      )
    }

    override def commit(): Unit = {
      super.commit()
    }

    def reInit(): Unit = {
      stateStore = getStateStore(keySchema, longValueSchema)
    }

    def purgeState(): Unit = {
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

    // Now we need some utility functions for value
    private val valueWithCounterExprs = inputValueAttributes :+ Literal(1)
    private val valueWithCounterSchema = valueSchema.add("counter", IntegerType)
    private val indexOrdinalInValueWithCounterRow = inputValueAttributes.size
    private val valueWithCounterRowGenerator = UnsafeProjection.create(
      valueWithCounterExprs, inputValueAttributes)
    private val valueRowGenerator = UnsafeProjection.create(
      inputValueAttributes, inputValueAttributes :+ AttributeReference("counter", IntegerType)())

    protected var stateStore = getStateStore(keyWithIndexSchema, valueWithCounterSchema)

    def get(key: UnsafeRow, valueIndex: Long): UnsafeRow = {
      val keyWithIndex = keyWithIndexRow(key, valueIndex)
      stateStore.get(keyWithIndex)
    }

    /**
     * Get all values and indices for the provided key.
     * Should not return null.
     */
    def getAll(key: UnsafeRow, numValues: Long): Iterator[KeyWithIndexAndValueWithCounter] = {
      val keyWithIndexAndValueWithCounter = KeyWithIndexAndValueWithCounter()
      var index = -1
      new NextIterator[KeyWithIndexAndValueWithCounter] {
        override protected def getNext(): KeyWithIndexAndValueWithCounter = {
          var retKV: KeyWithIndexAndValueWithCounter = null
          while (index < numValues && retKV == null) {
            index += 1
            val value = get(key, index)
            if (value != null) retKV = keyWithIndexAndValueWithCounter.withNew(key, index, value)
          }

          if (index >= numValues) finished = true
          retKV
        }

        override protected def close(): Unit = {}
      }
    }

    /** Put new value for key at the given index */
    def put(key: UnsafeRow, valueIndex: Long, value: UnsafeRow): Unit = {
      val keyWithIndex = keyWithIndexRow(key, valueIndex)
      val valueWithCounter = valueWithCounterRow(value, 0)
      stateStore.put(keyWithIndex, valueWithCounter)
    }

    /**
     * Remove key and value at given index. Note that this will create a hole in
     * (key, index) and it is upto the caller to deal with it.
     */
    def remove(key: UnsafeRow, valueIndex: Long): Unit = {
      val keyWithIndex = keyWithIndexRow(key, valueIndex)
      stateStore.remove(keyWithIndex)
    }

    /** Remove all values (i.e. all the indices) for the given key. */
    def removeAllValues(key: UnsafeRow, numValues: Long): Unit = {
      var index = 0
      while (index < numValues) {
        remove(key, index)
        index += 1
      }
    }

    override def commit(): Unit = {
      super.commit()
    }

    /** Generated a row using the key and index */
    private def keyWithIndexRow(key: UnsafeRow, valueIndex: Long): UnsafeRow = {
      val row = keyWithIndexRowGenerator(key)
      row.setLong(indexOrdinalInKeyWithIndexRow, valueIndex)
      row
    }

    private def valueWithCounterRow(value: UnsafeRow, counter: Int): UnsafeRow = {
      val row = valueWithCounterRowGenerator(value)
      row.setInt(indexOrdinalInValueWithCounterRow, counter)
      row
    }

    def reInit(): Unit = {
      stateStore = getStateStore(keyWithIndexSchema, valueWithCounterSchema)
    }

    def purgeState(): Unit = {
    }

    def equalWithoutCounter (valueWithCounter: UnsafeRow,
                             valueWithoutCounter: UnsafeRow): Boolean = {
      val row = valueRowGenerator(valueWithCounter)
      row.equals(valueWithoutCounter)
    }
  }
}

object SlothHashJoinStateManager {

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
