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

package org.apache.spark.sql

import java.io.{BufferedReader, FileReader, IOException}

import scala.collection.mutable.ArrayBuffer

import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.execution.{SlothFilterExec, SortExec, SparkPlan}
import org.apache.spark.sql.execution.aggregate.SlothHashAggregateExec
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2ScanExec
import org.apache.spark.sql.execution.streaming.{BaseStreamingSource, SlothDeduplicateExec, SlothSymmetricHashJoinExec, SlothThetaJoinExec}

class SlothDBCostModel extends Logging {

  import SlothDBCostModel._

  var incPlan: OperatorCostModel = _
  var subPlans: Array[SlothSubPlan] = _
  var batchNums: Array[Int] = _

  var BATCH_LATENCY: Double = _
  var BATCH_RESOURCE: Double = _

  val MIN_BATCHNUM = 1
  val MAX_BATCHNUM = 100

  def initialize(logicalPlan: LogicalPlan,
                 name: String,
                 slothdbStatDir: String,
                 numPart: Int,
                 mpDecompose: Boolean): Unit = {

    incPlan = buildPlan(logicalPlan, name, slothdbStatDir, numPart)
    populateHasNewData(incPlan)

    subPlans = buildSubPlans(incPlan, mpDecompose)

    val pair = computeLatencyAndResource(1, incPlan)
    BATCH_LATENCY = pair._1
    BATCH_RESOURCE = pair._2
  }

  private def buildPlan(logicalPlan: LogicalPlan,
                        name: String,
                        slothdbStatDir: String,
                        numPart: Int): OperatorCostModel = {
    var modelFile: String = null
    try {
      modelFile = slothdbStatDir + s"/${name}.model"
      val modelBr = new BufferedReader(new FileReader(modelFile))
      return buildChildPlan(0, logicalPlan, numPart, modelBr)
    } catch {
      case e: NoSuchElementException =>
        logError("SlothDB Stats Dir is not set")
      case e: IOException =>
        logError(s"Reading ${modelFile} error")
    }

    return null
  }

  private def buildChildPlan(id: Int,
                             logicalPlan: LogicalPlan,
                             numPart: Int,
                             br: BufferedReader): OperatorCostModel = {
    val newLine: String = br.readLine()
    if (newLine == null) assert(false, "BuildChildPlan: should not reach here")

    val (op, newPlan) = buildOperator(logicalPlan)
    val strArray = newLine.split(",")
    assert(strArray(0) == op.nodeName)

    val children = new Array[OperatorCostModel](newPlan.children.size)
    newPlan.children.zipWithIndex.foreach(pair => {
      val childPlan = pair._1
      val index = pair._2
      op.children(index) = buildChildPlan(id + index + 1, childPlan, numPart, br)
    })

    op.initialize(id, children, numPart, strArray)

    return op
  }

  private def buildOperator(logicalPlan: LogicalPlan):
  Tuple2[OperatorCostModel, LogicalPlan] = {
    logicalPlan match {
      case _: Aggregate =>
        (new AggregateCostModel, logicalPlan)
      case _: Join =>
        (new JoinCostModel, logicalPlan)
      // TODO
      // case _: Kafkav2 =>
      //  new OperatorCostModel
      case _: Filter =>
        (new SelectCostModel, logicalPlan)
      case _: Sort =>
        (new SortCostModel, logicalPlan)
      case _: Deduplicate =>
        (new DistinctCostModel, logicalPlan)
      case _ =>
        buildOperator(logicalPlan.children(0))
    }
  }

  private def populateHasNewData(op: OperatorCostModel): Boolean = {
    if (op.children.isEmpty) return op.leftHasNewData

    op.children.zipWithIndex.map(pair => {
        val child = pair._1
        val index = pair._2
        val hasNewData = populateHasNewData(child)
        if (index == LEFT) op.leftHasNewData = hasNewData
        else op.rightHasNewData = hasNewData
        hasNewData
      }).exists(hasNewData => hasNewData)
  }

  private def buildSubPlans(op: OperatorCostModel, mpDecompose: Boolean):
  Array[SlothSubPlan] = {
    val subPlanSet = new ArrayBuffer[SlothSubPlan]()
    var index = 0

    val rootSubPlan = new SlothSubPlan
    rootSubPlan.initialize(op.copy(), index, -1)
    subPlanSet.append(rootSubPlan)

    while (index < subPlanSet.length) {
      val tmpSubPlan = subPlanSet(index)
      val mpNum = findOneSubPlan(tmpSubPlan, tmpSubPlan.root, subPlanSet)
      tmpSubPlan.setMPNum(mpNum)
      index += 1
    }

    if (mpDecompose) {
      val pathSet = new ArrayBuffer[SlothSubPlan]()
      decomposeIntoMP(null, subPlanSet(0), pathSet)
      var i = 0
      while (i < pathSet.length) {
        val parentPath = pathSet(i)
        val subPlan = findSubPlanOfParentPath(parentPath, subPlanSet)
        decomposeIntoMP(parentPath, subPlan, pathSet)
        i += 1
      }
      pathSet.toArray
    } else {
      subPlanSet.toArray
    }
  }

  private def findOneSubPlan(subPlan: SlothSubPlan,
                             op: OperatorCostModel,
                             subPlanSet: ArrayBuffer[SlothSubPlan]): Int = {
    if (isBlockingOperator(op)) {
      val childSubPlan = new SlothSubPlan
      val childIndex = subPlanSet.length
      childSubPlan.initialize(op.copy().setLowerBlockingOperator(true),
        childIndex, subPlan.index)
      subPlan.addChildDep(childIndex)
      subPlanSet.append(childSubPlan)
      return 1;
    } else if (isScanOperator(op)) {
      return 1;
    } else {
      return op.children.map(child => findOneSubPlan(subPlan, child, subPlanSet)).sum
    }
  }

  private def decomposeIntoMP(parentPath: SlothSubPlan,
                              subPlan: SlothSubPlan,
                              pathSet: ArrayBuffer[SlothSubPlan]): Unit = {
    for (i <- 0 until subPlan.mpNum) {
      val newPath = subPlan.copy()
      newPath.setIndex(pathSet.length)
      newPath.setMPNum(1)
      newPath.setMaintainPath(i)

      if (parentPath != null ) {
        newPath.setParentDep(parentPath.index)
        parentPath.addChildDep(newPath.index)
      } else {
        newPath.setParentDep(-1)
      }
      pathSet.append(newPath)
    }
  }

  private def findSubPlanOfParentPath(parentPath: SlothSubPlan,
                                      subPlanSet: ArrayBuffer[SlothSubPlan]): SlothSubPlan = {
    val op = findBlockingOPOfPath(parentPath)
    if (op != null) {
      val retPlan =
        subPlanSet.find(subPlan => {
          val root = subPlan.root
          if (!isBlockingOperator(root) && root.id == op.id) true
          else false
        })
      if (retPlan.isDefined) retPlan.get
      else null
    } else null
  }

  private def findBlockingOPOfPath(path: SlothSubPlan): OperatorCostModel = {
    var op = path.root
    while (!isBlockingOperator(op)) {
      if (isScanOperator(op)) return null

      if (op.leftHasNewData) op = op.children(LEFT)
      else if (op.rightHasNewData) op = op.children(RIGHT)
      else return null
    }
    op
  }

  // With one constraint and optimize the other one
  def genTriggerPlanForLatencyConstraint(latencyConstraint: Double): Unit = {
    val realConstraint = latencyConstraint * BATCH_LATENCY
    val tmpBatchNums = Array.fill[Int](subPlans.length)(MAX_BATCHNUM)

    var curLatency = computeLatencyForSubPlans(subPlans, tmpBatchNums)
    while (curLatency < realConstraint) {

      val pair = subPlans.filter(_.hasNewData())
        .filter(validBatchNumForLatencyConstraint(_, tmpBatchNums))
        .map(subPlan => {
          val index = subPlan.index

          val batchNum = tmpBatchNums(index)
          val incability = subPlan.computeIncrementability(batchNum, batchNum - 1)

          (incability, index)
        }).reduceLeft(
        (pairA, pairB) => {
          if (pairA._1 < pairB._1) pairA
          else pairB
        })

      tmpBatchNums(pair._2) -= 1

      curLatency = computeLatencyForSubPlans(subPlans, tmpBatchNums)
    }

    batchNums = tmpBatchNums
  }

  def genTriggerPlanForResourceConstraint(resourceConstraint: Double): Unit = {
    val realConstraint = resourceConstraint * BATCH_RESOURCE
    val tmpBatchNums = Array.fill[Int](subPlans.length)(MIN_BATCHNUM)

    var curResource = computeResourceForSubPlans(subPlans, tmpBatchNums)
    while (curResource < realConstraint) {

      val pair = subPlans.filter(_.hasNewData())
        .filter(validBatchNumForResourceConstraint(_, tmpBatchNums))
        .map(subPlan => {
          val index = subPlan.index

          val batchNum = tmpBatchNums(index)
          val incability = subPlan.computeIncrementability(batchNum, batchNum + 1)

          (incability, index)
        }).reduceLeft(
        (pairA, pairB) => {
          if (pairA._1 > pairB._1) pairA
          else pairB
        })

      tmpBatchNums(pair._2) += 1

      curResource = computeResourceForSubPlans(subPlans, tmpBatchNums)
    }

    batchNums = tmpBatchNums
  }

  private def validBatchNumForLatencyConstraint(subPlan: SlothSubPlan,
                               tmpBatchNums: Array[Int]): Boolean = {
    // No dependencies
    if (subPlan.parentDep == -1) return true
    else {
      val index = subPlan.index

      return tmpBatchNums(index) > tmpBatchNums(subPlan.parentDep)
    }
  }

  private def validBatchNumForResourceConstraint(subPlan: SlothSubPlan,
                               tmpBatchNums: Array[Int]): Boolean = {
    // No dependencies
    if (subPlan.getChildDep.size == 0) return true
    else {
      val index = subPlan.index

      return !subPlan.getChildDep.exists(tmpIndex => {
        tmpBatchNums(tmpIndex) >= tmpBatchNums(index)})
    }
  }

  def constructNewData(): Map[BaseStreamingSource, LogicalPlan] = {
    return null
  }

  def triggerNewExecution(plan: SparkPlan): Unit = {

  }

}

class SlothSubPlan {

  import SlothDBCostModel._

  var root: OperatorCostModel = _
  var index: Int = _
  var parentDep: Int = _
  private var childDeps: Array[Int] = new Array[Int](0)
  var mpNum: Int = _

  def hasNewData(): Boolean = {
    root.leftHasNewData || root.rightHasNewData
  }

  def computeIncrementability(batchNumA: Int, batchNumB: Int): Double = {
    val lrPairA = computeLatencyAndResource(batchNumA, root)
    val lrPairB = computeLatencyAndResource(batchNumB, root)

    return (lrPairA._1 - lrPairB._1)/(lrPairA._2 - lrPairB._2)
  }

  def initialize(root: OperatorCostModel, index: Int, parentDep: Int): Unit = {
    this.root = root
    this.index = index
    this.parentDep = parentDep
  }

  def addChildDep(childIndex: Int): Unit = {
    val newChildDeps = new Array[Int](childDeps.length + 1)
    childDeps.zipWithIndex.foreach(pair => {
      newChildDeps(pair._2) = pair._1
    })
    newChildDeps(childDeps.length) = childIndex
    childDeps = newChildDeps
  }

  def getChildDep(): Array[Int] = childDeps

  def setMPNum(mpNum: Int): Unit = {
    this.mpNum = mpNum
  }

  def setIndex(newIndex: Int): Unit = {
    this.index = newIndex
  }

  def setParentDep(parentDep: Int): Unit = {
    this.parentDep = parentDep
  }

  def copy(): SlothSubPlan = {
    val newSubPlan = new SlothSubPlan
    newSubPlan.root = root.copy()
    newSubPlan
  }

  def setMaintainPath(mpIndex: Int): Unit = {
    setMPHelper(root, mpIndex, 0)
  }

  private def setMPHelper(op: OperatorCostModel,
                          mpIndex: Int,
                          mpCounter: Int): Tuple2[Boolean, Int] = {
    op match {
      case joinOP: JoinCostModel =>
        val (leftHasNewData, leftNewMPCounter) =
          setMPHelper(op.children(LEFT), mpIndex, mpCounter)
        joinOP.setLeftHasNewData(leftHasNewData)
        val (rightHasNewData, rightNewMPCounter) =
          setMPHelper(op.children(RIGHT), mpIndex, leftNewMPCounter)
        joinOP.setRightHasNewData(rightHasNewData)
        return (leftHasNewData || rightHasNewData, rightNewMPCounter)

      case leafOP: ScanCostModel =>
        val hasNewData = (mpCounter == mpIndex) && leafOP.leftHasNewData
        leafOP.setLeftHasNewData(hasNewData)
        return (hasNewData, mpCounter + 1)

      case otherOP: OperatorCostModel =>
        val (hasNewData, newMPCounter) = setMPHelper(op.children(LEFT), mpIndex, mpCounter)
        otherOP.setLeftHasNewData(hasNewData)
        return (hasNewData, newMPCounter)
    }
  }
}

object SlothDBCostModel {

  // scale factor
  val SF = 1000000000L

  // LEFT or RIGHT
  val LEFT = 0
  val RIGHT = 1

  // operation for cardinality
  val INSERT = 0
  val DELETE = 1
  val UPDATE = 2
  val OPERATION_SIZE = 3

  // operator type
  val SLOTHJOIN = 0
  val SLOTHAGGREGATE = 1
  val SLOTHSORT = 2
  val SLOTHSCAN = 3
  val SLOTHSELECT = 4
  val SLOTHDISTINCT = 5
  val SLOTHUNKNOWN = 6

  // Cost functions
  val TUPLECOST = 100L
  val SCANCOST = 1L

  def findNodeType(sparkPlan: SparkPlan): Int = {
    sparkPlan match {
      case _: SlothThetaJoinExec =>
        SLOTHJOIN
      case _: SlothSymmetricHashJoinExec =>
        SLOTHJOIN
      case _: SortExec =>
        SLOTHSORT
      case _: SlothHashAggregateExec =>
        SLOTHAGGREGATE
      case _: SlothFilterExec =>
        SLOTHSELECT
      case _: DataSourceV2ScanExec =>
        SLOTHSCAN
      case _: SlothDeduplicateExec =>
        SLOTHDISTINCT
      case _ =>
        SLOTHUNKNOWN
    }
  }

  def findNameFromType(nodeType: Int): String = {
    if (nodeType == SLOTHJOIN) "join"
    else if (nodeType == SLOTHAGGREGATE) "agg"
    else if (nodeType == SLOTHSELECT) "filter"
    else if (nodeType == SLOTHSCAN) "scan"
    else if (nodeType == SLOTHDISTINCT) "distinct"
    else if (nodeType == SLOTHSORT) "sort"
    else "unknown"
  }

  def findTypeFromName(name: String): Int = {
    if (name == "join") SLOTHJOIN
    else if (name == "agg") SLOTHAGGREGATE
    else if (name == "filter") SLOTHSELECT
    else if (name == "scan") SLOTHSCAN
    else if (name == "distinct") SLOTHDISTINCT
    else if (name == "sort") SLOTHSORT
    else SLOTHUNKNOWN
  }

  def isBlockingOperator(op: OperatorCostModel): Boolean = {
    op match {
      case agg: AggregateCostModel if !agg.lowerBlocking =>
        true
      case distinct: DistinctCostModel if !distinct.lowerBlocking =>
        true
      case sort: SortCostModel if !sort.lowerBlocking =>
        true
      case _ =>
        false
    }
  }

  def isScanOperator(op: OperatorCostModel): Boolean = {
    op match {
      case _: ScanCostModel =>
        true
      case _ =>
        false
    }
  }

  def computeResourceForSubPlans(subPlans: Array[SlothSubPlan], batchNums: Array[Int]): Double = {
    subPlans.zipWithIndex.map(planWithIndex => {
      computeLatencyAndResource(batchNums(planWithIndex._2), planWithIndex._1.root)._2
    }).sum
  }

  def computeLatencyForSubPlans(subPlans: Array[SlothSubPlan], batchNums: Array[Int]): Double = {
    subPlans.zipWithIndex.map(planWithIndex => {
      computeLatencyAndResource(batchNums(planWithIndex._2), planWithIndex._1.root)._1
    }).sum
  }

  // The first item is latency and the second is resource
  def computeLatencyAndResource(batchNum: Int, root: OperatorCostModel): Tuple2[Double, Double] = {
    root.getLatencyAndResource(batchNum)
  }

}
