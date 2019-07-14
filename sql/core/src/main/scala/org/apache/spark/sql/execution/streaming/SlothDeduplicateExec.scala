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

package org.apache.spark.sql.execution.streaming

import java.util.concurrent.TimeUnit._

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.codegen._
import org.apache.spark.sql.catalyst.plans.physical._
import org.apache.spark.sql.execution._
import org.apache.spark.sql.execution.metric.SQLMetrics
import org.apache.spark.sql.execution.streaming.state._
import org.apache.spark.sql.internal.SessionState
import org.apache.spark.util.CompletionIterator
import org.apache.spark.util.SerializableConfiguration

case class SlothDeduplicateExec(keyExpressions: Seq[Attribute],
                                child: SparkPlan,
                                stateInfo: Option[StatefulOperatorStateInfo] = None,
                                eventTimeWatermark: Option[Long] = None)
  extends UnaryExecNode with WatermarkSupport with SlothMetricsTracker {

  private val storeConf = new StateStoreConf(sqlContext.conf)
  private val hadoopConfBcast = sparkContext.broadcast(
    new SerializableConfiguration(SessionState.newHadoopConf(
      sparkContext.hadoopConfiguration, sqlContext.conf)))

  /** Distribute by grouping attributes */
  override def requiredChildDistribution: Seq[Distribution] =
    ClusteredDistribution(keyExpressions, stateInfo.map(_.numPartitions)) :: Nil

  override lazy val metrics = Map(
    "numOutputRows" -> SQLMetrics.createMetric(sparkContext, "number of output rows"),
    "aggTimeMs" -> SQLMetrics.createTimingMetric(sparkContext, "aggregate time"),
    "commitTimeMs" -> SQLMetrics.createTimingMetric(sparkContext, "commit time"),
    "stateMemory" -> SQLMetrics.createSizeMetric(sparkContext, "peak memory")
  )

  override protected def doExecute(): RDD[InternalRow] = {
    metrics // force lazy init at driver

    child.execute().mapPartitionsWithIndex((partIndex, iter) => {
      val dedupStateManager = new SlothDeduplicateStateManager(keyExpressions,
        stateInfo,
        storeConf,
        hadoopConfBcast.value.value)

      val getKey = GenerateUnsafeProjection.generate(keyExpressions, child.output)

      val numOutputRows = longMetric("numOutputRows")
      val aggTimeMs = longMetric("commitTimeMs")
      val commitTimeMs = longMetric("commitTimeMs")

      val baseIterator = watermarkPredicateForData match {
        case Some(predicate) => iter.filter(row => !predicate.eval(row))
        case None => iter
      }

      val startUpdateTimeNs = System.nanoTime

      baseIterator.foreach(r => {
        val row = r.asInstanceOf[UnsafeRow]
        val key = getKey(row)
        if (row.isInsert) dedupStateManager.insert(key)
        else dedupStateManager.delete(key)
      })

      val result = dedupStateManager.distinctIterAndWriteStore()
        .map(key => {
          numOutputRows += 1
          key
        })

      CompletionIterator[InternalRow, Iterator[InternalRow]](result, {
        aggTimeMs += NANOSECONDS.toMillis(System.nanoTime - startUpdateTimeNs)
        commitTimeMs += timeTakenMs { dedupStateManager.commit() }
      })
    })
  }

  override def output: Seq[Attribute] = child.output

  override def outputPartitioning: Partitioning = child.outputPartitioning
}
