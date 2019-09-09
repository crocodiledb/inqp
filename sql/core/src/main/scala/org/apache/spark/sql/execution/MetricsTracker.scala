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

package org.apache.spark.sql.execution

import java.{util => ju}
import java.lang.{Long => JLong}

import scala.collection.JavaConverters._

import org.json4s._
import org.json4s.JsonAST.JValue
import org.json4s.JsonDSL._
import org.json4s.jackson.JsonMethods._

import org.apache.spark.sql.SlothDBCostModel._
import org.apache.spark.util.Utils

case class SlothSummarizedMetrics() {

  var nodeType: Int = _
  var nodeName: String = _
  var numPart: Int = _
  var children: Seq[SlothSummarizedMetrics] = _
  var numOfRows: Long = _
  var updateRows: Long = _
  var deleteRows: Long = _

  var numGroups: Long = _
  var left_insert_to_insert: Long = _
  var left_delete_to_delete: Long = _
  var left_update_to_update: Long = _
  var right_insert_to_insert: Long = _
  var right_delete_to_delete: Long = _
  var right_update_to_update: Long = _
  var insert_to_insert: Long = _
  var delete_to_delete: Long = _
  var update_to_update: Long = _
  var numBatch: Long = _

  var hasMetrics: Boolean = _
  val formatter = java.text.NumberFormat.getIntegerInstance

  def updateMetrics(metricsTracker: SlothMetricsTracker): Unit = {
    numBatch += 1

    numOfRows += metricsTracker.getNumOutputRows
    updateRows += metricsTracker.getUpdateRows
    deleteRows += metricsTracker.getDeleteRows

    numGroups = metricsTracker.getNumGroups
    left_insert_to_insert += metricsTracker.getLeftInsertToInsert
    left_delete_to_delete += metricsTracker.getLeftDeleteToDelete
    left_update_to_update += metricsTracker.getLeftUpdateToUpdate
    right_insert_to_insert += metricsTracker.getRightInsertToInsert
    right_delete_to_delete += metricsTracker.getRightDeleteToDelete
    right_update_to_update += metricsTracker.getRightUpdateToUpdate
    insert_to_insert += metricsTracker.getInsertToInsert
    delete_to_delete += metricsTracker.getDeleteToDelete
    update_to_update += metricsTracker.getUpdateToUpdate
  }

  def getFormattedMetrics(): String = {
    val baseString = s"${nodeName} [numOfRows: ${formatter.format(numOfRows)}]"
    val updateString = if (updateRows != 0) s" [updateRows: ${formatter.format(updateRows)}]"
                       else ""
    val deleteString = if (deleteRows != 0) s" [deleteRows: ${formatter.format(deleteRows)}]"
                       else ""
    baseString + updateString + deleteString + "\n"
  }

  def getCostModelInfo(): String = {
    val nodeName = findNameFromType(nodeType)
    val totalNum = numBatch * numPart
    if (nodeType == SLOTHJOIN) {
      val left_insert_prob = (left_insert_to_insert/totalNum).toDouble/SF.toDouble
      val left_delete_prob = (left_delete_to_delete/totalNum).toDouble/SF.toDouble
      val left_update_prob = (left_update_to_update/totalNum).toDouble/SF.toDouble
      val right_insert_prob = (right_insert_to_insert/totalNum).toDouble/SF.toDouble
      val right_delete_prob = (right_delete_to_delete/totalNum).toDouble/SF.toDouble
      val right_update_prob = (right_update_to_update/totalNum).toDouble/SF.toDouble
      return f"$nodeName,$left_insert_prob,$left_delete_prob,$left_update_prob," +
        f"$right_insert_prob,$right_delete_prob,$right_update_prob\n"
    } else if (nodeType == SLOTHAGGREGATE) {
      return f"$nodeName,$numGroups\n"
    } else if (nodeType == SLOTHSELECT) {
      val insert_prob = (insert_to_insert/totalNum).toDouble/SF.toDouble
      val delete_prob = (delete_to_delete/totalNum).toDouble/SF.toDouble
      val update_prob = (update_to_update/totalNum).toDouble/SF.toDouble
      return f"$nodeName,$insert_prob,$delete_prob,$update_prob\n"
    } else if (nodeType == SLOTHSCAN) {
      return f"$nodeName,$numOfRows\n"
    } else if (nodeType == SLOTHDISTINCT) {
      return f"$nodeName,$numGroups\n"
    } else {
      return f"$nodeName\n"
    }
  }
}

class SlothProgressMetrics (val shortName: String,
                       val metricMap: ju.Map[String, JLong]) {
  /** The compact JSON representation of this progress. */
  def json: String = compact(render(jsonValue))

  /** The pretty (i.e. indented) JSON representation of this progress. */
  def prettyJson: String = pretty(render(jsonValue))

  private[sql] def jsonValue: JValue = {
    (shortName -> {
      if (!metricMap.isEmpty) {
        val keys = metricMap.keySet.asScala.toSeq.sorted
        keys.map { k => k -> JInt(metricMap.get(k).toLong): JObject }.reduce(_ ~ _)
      } else {
        JNothing
      }
    })
  }

  override def toString: String = prettyJson
}

trait SlothMetricsTracker extends SparkPlan { self: SparkPlan =>

  def getProgress(): SlothProgressMetrics = {
    val extractedMetrics = metrics.map(
      entry => entry._1 -> longMetric(entry._1).value)

    val javaMetrics: java.util.HashMap[String, java.lang.Long] =
      new java.util.HashMap(extractedMetrics.mapValues(long2Long).asJava)

    new SlothProgressMetrics(nodeName, javaMetrics)
  }

  def getRowProgress(): String = {
    var progressString: String = nodeName
    val extractedMetrics = metrics
      .filter(entry => entry._1.equals("numOutputRows") || entry._1.equals("deleteRows"))
      .map(entry => entry._1 -> longMetric(entry._1).value)
      .foreach(entry => progressString = progressString + "[" + entry._1 + ": " + entry._2 + "]")

    progressString + "\n"
  }

  def getNumOutputRows(): Long = {
    val metric = metrics.get("numOutputRows")
    if (metric.isDefined) metric.get.value
    else 0L
  }

  def getUpdateRows(): Long = {
    val metric = metrics.get("updateRows")
    if (metric.isDefined) metric.get.value
    else 0L
  }

  def getDeleteRows(): Long = {
    val metric = metrics.get("deleteRows")
    if (metric.isDefined) metric.get.value
    else 0L
  }

  def getNumGroups(): Long = {
    val metric = metrics.get("numGroups")
    if (metric.isDefined) metric.get.value
    else 0L
  }

  def getLeftInsertToInsert(): Long = {
    val metric = metrics.get("left_insert_to_insert")
    if (metric.isDefined) metric.get.value
    else 0L
  }

  def getLeftDeleteToDelete(): Long = {
    val metric = metrics.get("left_delete_to_delete")
    if (metric.isDefined) metric.get.value
    else 0L
  }

  def getLeftUpdateToUpdate(): Long = {
    val metric = metrics.get("left_update_to_update")
    if (metric.isDefined) metric.get.value
    else 0L
  }

  def getRightInsertToInsert(): Long = {
    val metric = metrics.get("right_insert_to_insert")
    if (metric.isDefined) metric.get.value
    else 0L
  }

  def getRightDeleteToDelete(): Long = {
    val metric = metrics.get("right_delete_to_delete")
    if (metric.isDefined) metric.get.value
    else 0L
  }

  def getRightUpdateToUpdate(): Long = {
    val metric = metrics.get("right_update_to_update")
    if (metric.isDefined) metric.get.value
    else 0L
  }

  def getInsertToInsert(): Long = {
    val metric = metrics.get("insert_to_insert")
    if (metric.isDefined) metric.get.value
    else 0L
  }

  def getDeleteToDelete(): Long = {
    val metric = metrics.get("delete_to_delete")
    if (metric.isDefined) metric.get.value
    else 0L
  }

  def getUpdateToUpdate(): Long = {
    val metric = metrics.get("update_to_update")
    if (metric.isDefined) metric.get.value
    else 0L
  }

  /** Records the duration of running `body` for the next query progress update. */
  protected def timeTakenMs(body: => Unit): Long = Utils.timeTakenMs(body)._2
}
