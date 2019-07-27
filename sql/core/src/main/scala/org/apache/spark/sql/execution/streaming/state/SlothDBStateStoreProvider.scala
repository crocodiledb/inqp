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

import java.util
import java.util.concurrent.atomic.LongAdder

import scala.collection.JavaConverters._
import scala.util.control.NonFatal

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs._

import org.apache.spark.{SparkConf, SparkEnv}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.expressions.UnsafeRow
import org.apache.spark.sql.types.StructType
import org.apache.spark.util.SizeEstimator


private[state] class SlothDBStateStoreProvider extends StateStoreProvider with Logging {

  // ConcurrentHashMap is used because it generates fail-safe iterators on filtering
  // - The iterator is weakly consistent with the map, i.e., iterator's data reflect the values in
  //   the map when the iterator was created
  // - Any updates to the map while iterating through the filtered iterator does not throw
  //   java.util.ConcurrentModificationException
  type MapType = java.util.concurrent.ConcurrentHashMap[UnsafeRow, UnsafeRow]

  /** Implementation of [[StateStore]] API which is backed by a HDFS-compatible file system */
  class SlothDBStateStore(val version: Long, mapToUpdate: MapType)
    extends StateStore {

    /** Trait and classes representing the internal state of the store */
    trait STATE
    case object UPDATING extends STATE
    case object COMMITTED extends STATE
    case object ABORTED extends STATE

    private val newVersion = version + 1
    @volatile private var state: STATE = UPDATING

    override def id: StateStoreId = SlothDBStateStoreProvider.this.stateStoreId

    override def get(key: UnsafeRow): UnsafeRow = {
      mapToUpdate.get(key)
    }

    override def put(key: UnsafeRow, value: UnsafeRow): Unit = {
      verify(state == UPDATING, s"Cannot put after already ${state}")
      val keyCopy = key.copy()
      val valueCopy = value.copy()
      mapToUpdate.put(keyCopy, valueCopy)
    }

    override def remove(key: UnsafeRow): Unit = {
      verify(state == UPDATING, "Cannot remove after already committed or aborted")
      mapToUpdate.remove(key)
    }

    override def getRange(
        start: Option[UnsafeRow],
        end: Option[UnsafeRow]): Iterator[UnsafeRowPair] = {
      verify(state == UPDATING, "Cannot getRange after already committed or aborted")
      iterator()
    }

    /** Commit all the updates that have been made to the store, and return the new version. */
    override def commit(): Long = {
      verify(state == UPDATING, "Cannot commit after already committed or aborted")

      try {
        commitUpdates(newVersion, mapToUpdate)
        state = COMMITTED
        logInfo(s"Committed version $newVersion for $this")
        newVersion
      } catch {
        case NonFatal(e) =>
          throw new IllegalStateException(
            s"Error committing version $newVersion into $this", e)
      }
    }

    /** Abort all the updates made on this store. This store will not be usable any more. */
    override def abort(): Unit = {
      // This if statement is to ensure that files are deleted only if there are changes to the
      // StateStore. We have two StateStores for each task, one which is used only for reading, and
      // the other used for read+write. We don't want the read-only to delete state files.
      if (state == UPDATING) {
        state = ABORTED
      } else {
        state = ABORTED
      }
      // logInfo(s"Aborted version $newVersion for $this")
     throw new IllegalArgumentException(s"Aborted version ${newVersion} for $this")
    }

    /**
     * Get an iterator of all the store data.
     * This can be called only after committing all the updates made in the current thread.
     */
    override def iterator(): Iterator[UnsafeRowPair] = {
      val unsafeRowPair = new UnsafeRowPair()
      mapToUpdate.entrySet.asScala.iterator.map { entry =>
        unsafeRowPair.withRows(entry.getKey, entry.getValue)
      }
    }

    override def metrics: StateStoreMetrics = {
      // NOTE: we provide estimation of cache size as "memoryUsedBytes", and size of state for
      // current version as "stateOnCurrentVersionSizeBytes"
      val metricsFromProvider: Map[String, Long] = getMetricsForProvider()

      val customMetrics = metricsFromProvider.flatMap { case (name, value) =>
        // just allow searching from list cause the list is small enough
        supportedCustomMetrics.find(_.name == name).map(_ -> value)
      } + (metricStateOnCurrentVersionSizeBytes -> SizeEstimator.estimate(mapToUpdate))

      StateStoreMetrics(mapToUpdate.size(), metricsFromProvider("memoryUsedBytes"), customMetrics)
    }

    /**
     * Whether all updates have been committed
     */
    override def hasCommitted: Boolean = {
      state == COMMITTED
    }

    override def toString(): String = {
      s"SlothDBStateStore[id=(op=${id.operatorId},part=${id.partitionId})"
    }
  }

  def getMetricsForProvider(): Map[String, Long] = synchronized {
    Map("memoryUsedBytes" -> SizeEstimator.estimate(loadedMaps),
      metricLoadedMapCacheHit.name -> loadedMapCacheHitCount.sum(),
      metricLoadedMapCacheMiss.name -> loadedMapCacheMissCount.sum())
  }

  /** Get the state store for making updates to create a new `version` of the store. */
  override def getStore(version: Long): StateStore = synchronized {
    require(version >= 0, "Version cannot be less than 0")
    // val newMap = new MapType
    // if (version > 0) {
    //   newMap.putAll(loadMap(version))
    // }
    val newMap = loadMap(version)
    val store = new SlothDBStateStore(version, newMap)
    logInfo(s"Retrieved version $version of ${SlothDBStateStoreProvider.this} for update")
    store
  }

  override def init(
      stateStoreId: StateStoreId,
      keySchema: StructType,
      valueSchema: StructType,
      indexOrdinal: Option[Int], // for sorting the data
      storeConf: StateStoreConf,
      hadoopConf: Configuration): Unit = {
    this.stateStoreId_ = stateStoreId
    this.keySchema = keySchema
    this.valueSchema = valueSchema
    this.storeConf = storeConf
    this.hadoopConf = hadoopConf
    this.numberOfVersionsToRetainInMemory = storeConf.maxVersionsToRetainInMemory
  }

  override def stateStoreId: StateStoreId = stateStoreId_

  /** Do maintenance backing data files, including creating snapshots and cleaning up old files */
  override def doMaintenance(): Unit = {
  }

  override def close(): Unit = {
    loadedMaps.values.asScala.foreach(_.clear())
  }

  override def supportedCustomMetrics: Seq[StateStoreCustomMetric] = {
    metricStateOnCurrentVersionSizeBytes :: metricLoadedMapCacheHit :: metricLoadedMapCacheMiss ::
      Nil
  }

  override def toString(): String = {
    s"SlothDBStateStoreProvider[" +
      s"id = (op=${stateStoreId.operatorId},part=${stateStoreId.partitionId})]"
  }

  /* Internal fields and methods */

  @volatile private var stateStoreId_ : StateStoreId = _
  @volatile private var keySchema: StructType = _
  @volatile private var valueSchema: StructType = _
  @volatile private var storeConf: StateStoreConf = _
  @volatile private var hadoopConf: Configuration = _
  @volatile private var numberOfVersionsToRetainInMemory: Int = _

  private lazy val loadedMaps = new util.TreeMap[Long, MapType](Ordering[Long].reverse)

  private lazy val sparkConf = Option(SparkEnv.get).map(_.conf).getOrElse(new SparkConf)

  private val loadedMapCacheHitCount: LongAdder = new LongAdder
  private val loadedMapCacheMissCount: LongAdder = new LongAdder

  private lazy val metricStateOnCurrentVersionSizeBytes: StateStoreCustomSizeMetric =
    StateStoreCustomSizeMetric("stateOnCurrentVersionSizeBytes",
      "estimated size of state only on current version")

  private lazy val metricLoadedMapCacheHit: StateStoreCustomMetric =
    StateStoreCustomSumMetric("loadedMapCacheHitCount",
      "count of cache hit on states cache in provider")

  private lazy val metricLoadedMapCacheMiss: StateStoreCustomMetric =
    StateStoreCustomSumMetric("loadedMapCacheMissCount",
      "count of cache miss on states cache in provider")

  private case class StoreFile(version: Long, path: Path, isSnapshot: Boolean)

  private def commitUpdates(newVersion: Long, map: MapType): Unit = {
    synchronized {
      putStateIntoStateCacheMap(newVersion, map)
    }
  }

  private def putStateIntoStateCacheMap(newVersion: Long, map: MapType): Unit = synchronized {
    if (numberOfVersionsToRetainInMemory <= 0) {
      if (loadedMaps.size() > 0) loadedMaps.clear()
      return
    }

    while (loadedMaps.size() > numberOfVersionsToRetainInMemory) {
      loadedMaps.remove(loadedMaps.lastKey())
    }

    val size = loadedMaps.size()
    if (size == numberOfVersionsToRetainInMemory) {
      val versionIdForLastKey = loadedMaps.lastKey()
      if (versionIdForLastKey > newVersion) {
        // this is the only case which we can avoid putting, because new version will be placed to
        // the last key and it should be evicted right away
        return
      } else if (versionIdForLastKey < newVersion) {
        // this case needs removal of the last key before putting new one
        loadedMaps.remove(versionIdForLastKey)
      }
    }

    loadedMaps.put(newVersion, map)
  }

  /** Load the required version of the map data from the backing files */
  private def loadMap(version: Long): MapType = {

    // Shortcut if the map for this version is already there to avoid a redundant put.
    val loadedCurrentVersionMap = synchronized {
      Option(loadedMaps.remove(version))
      // Option(loadedMaps.get(version))
    }

    if (loadedCurrentVersionMap.isDefined) {
      loadedMapCacheHitCount.increment()
      return loadedCurrentVersionMap.get
    } else {
      logWarning(s"The state for version $version doesn't exist in loadedMaps. " +
        "Reading snapshot file and delta files if needed..." +
        "Note that this is normal for the first batch of starting query.")
      if (version > 0) {
        throw new IllegalArgumentException("Does not find a cache")
      } else {
        return new MapType
      }
    }
  }

  private def verify(condition: => Boolean, msg: String): Unit = {
    if (!condition) {
      throw new IllegalStateException(msg)
    }
  }
}
