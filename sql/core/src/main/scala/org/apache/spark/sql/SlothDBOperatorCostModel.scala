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

import scala.math._

import org.apache.spark.sql.catalyst.plans.{FullOuter, LeftAnti, LeftOuter, RightOuter}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan

abstract class OperatorCostModel {

  import SlothDBCostModel._

  var children: Array[OperatorCostModel] = _

  // Meta data for all operators
  var id: Int = _
  var logicalPlan: LogicalPlan = _
  var nodeType: Int = _
  var nodeName: String = _
  var realNumPart: Double = _
  var numPart: Double = _
  var cardinalityOnly: Boolean = _
  var mpIdx: Int = -1

  var leftHasNewData: Boolean = _
  var rightHasNewData: Boolean = _
  var leftPreprocessed: Boolean = _
  var rightPreProcessed: Boolean = _
  var lowerBlocking: Boolean = _

  // cardinality
  var outputRows: Double = _
  var opLatency: Double = _
  var opResource: Double = _

  // unit cost
  var unitCost: Double = _

  def hasNewData(): Boolean = {
    leftHasNewData || rightHasNewData
  }

  def setMPIdx(mpIdx: Int): Unit = {
    this.mpIdx = mpIdx
    children.foreach(_.setMPIdx(mpIdx))
  }

  protected def initHelper(id: Int, logicalPlan: LogicalPlan,
                           children: Array[OperatorCostModel], numPart: Int): Unit = {

    this.id = id
    this.logicalPlan = logicalPlan
    this.children = children
    this.realNumPart = numPart
    this.numPart = numPart.toDouble

    leftHasNewData = false
    rightHasNewData = false
    leftPreprocessed = false
    rightPreProcessed = false
    lowerBlocking = false

    unitCost =
      if (this.isInstanceOf[ScanCostModel]) SCANUNITCOST
      else OTHERUNITCOST
  }

  protected def copyHelper(newOP: OperatorCostModel): Unit = {

    newOP.id = this.id
    newOP.logicalPlan = this.logicalPlan
    newOP.children = new Array[OperatorCostModel](children.size)
    for (i <- 0 until children.size) {
      newOP.children(i) = children(i).copy()
    }
    newOP.nodeType = nodeType
    newOP.nodeName = nodeName
    newOP.realNumPart = realNumPart
    newOP.numPart = numPart
    newOP.mpIdx = mpIdx
    newOP.unitCost = unitCost

    newOP.leftHasNewData = leftHasNewData
    newOP.rightHasNewData = rightHasNewData
    newOP.leftPreprocessed = leftPreprocessed
    newOP.rightPreProcessed = rightPreProcessed
    newOP.lowerBlocking = lowerBlocking
  }

  protected def resetHelper(cardinalityOnly: Boolean): Unit = {
    outputRows = 0
    opLatency = 0
    opResource = 0
    this.cardinalityOnly = cardinalityOnly
  }

  def setLeftHasNewData(hasNewData: Boolean): OperatorCostModel = {
    leftHasNewData = hasNewData
    this
  }

  def setRightHasNewData(hasNewData: Boolean): OperatorCostModel = {
    rightHasNewData = hasNewData
    this
  }

  def setLowerBlockingOperator(lower: Boolean): OperatorCostModel = {
    this.lowerBlocking = lower
    this
  }

  protected def accumulateCost(isLastBatch: Boolean,
                     stateSize: Double,
                     outputTuples: Double): Unit = {
    val staticCost =
      if (this.isInstanceOf[AggregateCostModel]) AGGSTATICCOST
      else JOINSTATICCOST

    outputRows += outputTuples
    if (!cardinalityOnly) {
      opResource += (outputTuples * unitCost * TUPLECOST + stateSize * SCANCOST + staticCost)
    } else opResource += outputTuples

    if (isLastBatch) {
      if (!cardinalityOnly) {
       opLatency += (outputTuples * unitCost + TUPLECOST + stateSize * SCANCOST + staticCost)
      } else opLatency += outputTuples
    }
  }

  protected def collectLatencyAndResource(subPlanAware: Boolean):
  (Double, Double) = {
    if (!this.hasNewData()) return (0.0, 0.0)
    else if (subPlanAware && isBlockingOperator(this)) return (opLatency, opResource)
    else {
      val leftPair = if (children.size > 0) {
        children(LEFT).collectLatencyAndResource(subPlanAware)
      } else (0.0, 0.0)

      val rightPair = if (children.size > 1) {
        children(RIGHT).collectLatencyAndResource(subPlanAware)
      } else (0.0, 0.0)

      // if (this.isInstanceOf[AggregateCostModel]) {
      //   printf(s"Aggregate: Latency ${opLatency}, Resource ${opResource}\n")
      // }

      if (subPlanAware && lowerBlocking) {
        return (leftPair._1 + rightPair._1,
          leftPair._2 + rightPair._2)
      } else {
        return (leftPair._1 + rightPair._1 + opLatency,
          leftPair._2 + rightPair._2 + opResource)
      }
    }
  }

  protected def tupleOperationSum(tuple: (Double, Double, Double)): Double = {
    // if (this.cardinalityOnly) {
    //   tuple._1 + tuple._2 + tuple._3
    // } else {
    tuple._1 + tuple._2 + 2 * tuple._3
    // }
  }

  protected def tupleSum(tuple: (Double, Double, Double)): Double = {
    tuple._1 + tuple._2 + tuple._3
  }

  protected def tupleAdd(tupleA: (Double, Double, Double),
                         tupleB: (Double, Double, Double)):
  (Double, Double, Double) = {
    (tupleA._1 + tupleB._1,
      tupleA._2 + tupleB._2,
      tupleA._3 + tupleB._3)
  }

  def getLatencyAndResource(batchNum: Int,
                            subPlanAware: Boolean,
                            cardinalityOnly: Boolean): (Double, Double) = {
    this.reset(cardinalityOnly)
    this.preProcess()
    val batchNums = Array.fill(1)(batchNum)
    val executable = Array.fill(1)(true)
    for (i <- 0 until batchNum) {
      val isLastBatch = (i + 1 == batchNum)
      this.getNextBatch(batchNums, isLastBatch, executable)
    }
    val pair = this.collectLatencyAndResource(subPlanAware)
    (pair._1 * numPart, pair._2 * numPart)
  }

  private def findNextStep(batchNums: Array[Int],
                           batchSteps: Array[Int],
                           mpIsSource: Array[Boolean],
                           sourceFirst: Boolean): Int = {
    val pair = batchNums.zipWithIndex.reduceLeft((pairA, pairB) => {
      val batchNumA = pairA._1.toDouble
      val batchIdxA = pairA._2
      val batchNumB = pairB._1.toDouble
      val batchIdxB = pairB._2
      val isSourceA = mpIsSource(batchIdxA)
      val isSourceB = mpIsSource(batchIdxB)

      val progressA = (batchSteps(batchIdxA) + 1).toDouble/batchNumA
      val progressB = (batchSteps(batchIdxB) + 1).toDouble/batchNumB
      if (progressA < progressB) pairA
      else if (progressA > progressB) pairB
      else {
        if (sourceFirst) {
          if (isSourceA) pairA
          else if (isSourceB) pairB
          else pairA
        } else {
          if (!isSourceA) pairA
          else if (!isSourceB) pairB
          else pairA
        }
      }
    })

    // batchIndex
    pair._2
  }

  def getLRByBatchNumsHelper(batchNums: Array[Int],
                             mpIsSource: Array[Boolean],
                             subPlanAware: Boolean,
                             cardinalityOnly: Boolean,
                             sourceFirst: Boolean): (Double, Double) = {
    this.reset(cardinalityOnly)
    this.preProcess()

    val totalSteps = batchNums.sum - batchNums.length + 1
    val batchSteps = Array.fill(batchNums.length)(0)

    for (i <- 0 until totalSteps) {
      val batchIdx = findNextStep(batchNums, batchSteps, mpIsSource, sourceFirst)
      batchSteps(batchIdx) += 1

      val isLastBatch = (i == (totalSteps - 1))
      val executable =
        if (!isLastBatch) {
          val tmpExecutable = Array.fill(batchNums.length)(false)
          tmpExecutable(batchIdx) = true
          tmpExecutable
        } else {
          Array.fill(batchNums.length)(true)
        }
      this.getNextBatch(batchNums, isLastBatch, executable)
    }

    val pair = this.collectLatencyAndResource(subPlanAware)
    (pair._1 * numPart, pair._2 * numPart)
  }

  def getLRByBatchNums(batchNums: Array[Int],
                       mpIsSource: Array[Boolean],
                       subPlanAware: Boolean,
                       cardinalityOnly: Boolean,
                       larger: Boolean): (Double, Double) = {
    val pairA = getLRByBatchNumsHelper(batchNums, mpIsSource, subPlanAware, cardinalityOnly, true)
    val pairB =
     getLRByBatchNumsHelper(batchNums, mpIsSource, subPlanAware, cardinalityOnly, false)
    if (larger) {
      if (pairA._2 < pairB._2) pairB
      else pairA
    } else {
      if (pairA._2 < pairB._2) pairA
      else pairB
    }
  }

  def initialize(id: Int,
                 logicalPlan: LogicalPlan,
                 children: Array[OperatorCostModel],
                 numPart: Int,
                 configs: Array[String]): Unit
  def copy(): OperatorCostModel
  def reset(cardinalityOnly: Boolean): Unit
  def getNextBatch(batchNum: Array[Int], isLateBatch: Boolean, executable: Array[Boolean]):
  (Double, Double, Double)
  def preProcess(): Unit
}

class JoinCostModel extends OperatorCostModel {

  import SlothDBCostModel._

  nodeType = SLOTHJOIN
  nodeName = findNameFromType(SLOTHJOIN)

  var left_insert_to_insert: Double = _
  var left_delete_to_delete: Double = _
  var left_update_to_update: Double = _
  var right_insert_to_insert: Double = _
  var right_delete_to_delete: Double = _
  var right_update_to_update: Double = _

  var leftStateSize: Double = _
  var rightStateSize: Double = _

  var joinType: String = _

  override def initialize(id: Int,
                          logicalPlan: LogicalPlan,
                          children: Array[OperatorCostModel],
                          numPart: Int,
                 configs: Array[String]): Unit = {
    initHelper(id, logicalPlan, children, numPart)
    joinType = configs(1)
    left_insert_to_insert = configs(2).toDouble
    left_delete_to_delete = configs(3).toDouble
    left_update_to_update = configs(4).toDouble
    right_insert_to_insert = configs(5).toDouble
    right_delete_to_delete = configs(6).toDouble
    right_update_to_update = configs(7).toDouble
  }

  override def copy(): OperatorCostModel = {
    val newOP = new JoinCostModel
    newOP.left_insert_to_insert = left_insert_to_insert
    newOP.left_delete_to_delete = left_delete_to_delete
    newOP.left_update_to_update = left_update_to_update
    newOP.right_insert_to_insert = right_insert_to_insert
    newOP.right_delete_to_delete = right_delete_to_delete
    newOP.right_update_to_update = right_update_to_update

    copyHelper(newOP)

    newOP
  }

  override def reset(cardinalityOnly: Boolean): Unit = {
    resetHelper(cardinalityOnly)
    leftStateSize = 0.0
    rightStateSize = 0.0

    children.foreach(_.reset(cardinalityOnly))
  }

  override def preProcess(): Unit = {
    if (!leftHasNewData) {
      leftPreprocessed = true
      processLeftInput(children(LEFT).getNextBatch(null, true, null))
    } else children(LEFT).preProcess()

    if (!rightHasNewData) {
      rightPreProcessed = true
      processRightInput(children(RIGHT).getNextBatch(null, true, null))
    } else children(RIGHT).preProcess()
  }

  override def getNextBatch(batchNum: Array[Int], isLastBatch: Boolean, executable: Array[Boolean]):
  (Double, Double, Double) = {

    val leftOutput = if (!leftPreprocessed) {
      val leftInput = children(LEFT).getNextBatch(batchNum, isLastBatch, executable)
      processLeftInput(leftInput)
    } else (0.0, 0.0, 0.0)

    val rightOutput = if (!rightPreProcessed) {
      val rightInput = children(RIGHT).getNextBatch(batchNum, isLastBatch, executable)
      processRightInput(rightInput)
    } else (0.0, 0.0, 0.0)

    accumulateCost(isLastBatch, 0,
      tupleOperationSum(leftOutput) + tupleOperationSum(rightOutput))

    return tupleAdd(leftOutput, rightOutput)
  }

  private def processLeftInput(leftInput: (Double, Double, Double)):
  (Double, Double, Double) = {
    // Inner Parts
    var left_insert_output = leftInput._1 * left_insert_to_insert * rightStateSize
    var left_delete_output = leftInput._2 * left_delete_to_delete * rightStateSize
    val left_update_output = leftInput._3 * left_update_to_update * rightStateSize

    // Outer parts: left side
    if (joinType == LeftOuter.toString ||
      joinType == FullOuter.toString ||
      joinType == LeftAnti.toString) {
      left_insert_output += leftInput._1 * pow(1 - left_insert_to_insert, rightStateSize)
      left_delete_output += leftInput._2 * pow(1 - left_delete_to_delete, rightStateSize)
      left_insert_output += leftInput._3 * pow(1 - left_update_to_update, rightStateSize)
      left_delete_output += leftInput._3 * pow(1 - left_update_to_update, rightStateSize)
    }

    // Outer parts: right side
    if (joinType == RightOuter.toString ||
      joinType == FullOuter.toString) {
      left_delete_output += leftInput._1 * rightStateSize *
        left_insert_to_insert * rightMatchRatio
      left_insert_output += leftInput._2 * rightStateSize *
        right_delete_to_delete * rightMatchRatio

      left_delete_output += leftInput._3 * rightStateSize *
        left_update_to_update * rightMatchRatio
      left_insert_output += leftInput._3 * rightStateSize *
        left_update_to_update * rightMatchRatio
    }

    leftStateSize = leftStateSize + leftInput._1 - leftInput._2
    outputRows += (left_insert_output + left_delete_output + left_update_output)
    (left_insert_output, left_delete_output, left_update_output)
  }

  private def processRightInput(rightInput: (Double, Double, Double)):
  (Double, Double, Double) = {
    var right_insert_output = rightInput._1 * right_insert_to_insert * leftStateSize
    var right_delete_output = rightInput._2 * right_delete_to_delete * leftStateSize
    val right_update_output = rightInput._3 * right_update_to_update * leftStateSize

    // Outer parts: left side
    if (joinType == RightOuter.toString ||
      joinType == FullOuter.toString) {
      right_insert_output += rightInput._1 * pow(1 - right_insert_to_insert, leftStateSize)
      right_delete_output += rightInput._2 * pow(1 - right_delete_to_delete, leftStateSize)
      right_insert_output += rightInput._3 * pow(1 - right_update_to_update, leftStateSize)
      right_delete_output += rightInput._3 * pow(1 - right_update_to_update, leftStateSize)
    }

    // Outer parts: right side
    if (joinType == LeftOuter.toString ||
      joinType == FullOuter.toString ||
      joinType == LeftAnti.toString) {
      right_delete_output += rightInput._1 * leftStateSize *
        right_insert_to_insert * leftMatchRatio
      right_insert_output += rightInput._2 * leftStateSize *
        right_delete_to_delete * leftMatchRatio

      right_delete_output += rightInput._3 * leftStateSize *
        right_update_to_update * leftMatchRatio
      right_insert_output += rightInput._3 * leftStateSize *
        right_update_to_update * leftMatchRatio
    }

    rightStateSize = rightStateSize + rightInput._1 - rightInput._2
    outputRows += (right_insert_output + right_delete_output + right_update_output)
    (right_insert_output, right_delete_output, right_update_output)
  }

  private def leftMatchRatio(): Double = {
    1.0 - pow(1 - left_insert_to_insert, rightStateSize)
  }

  private def rightMatchRatio(): Double = {
    1.0 - pow(1 - right_insert_to_insert, leftStateSize)
  }

}

class ScanCostModel extends OperatorCostModel {

  import SlothDBCostModel._

  nodeType = SLOTHSCAN
  nodeName = findNameFromType(SLOTHSCAN)

  var inputRows: Double = _
  var isStatic: Boolean = _

  override def initialize(id: Int,
                          logicalPlan: LogicalPlan,
                          children: Array[OperatorCostModel],
                          numPart: Int,
                 configs: Array[String]): Unit = {
    initHelper(id, logicalPlan, children, numPart)
    inputRows = configs(1).toDouble
    isStatic = (inputRows < MIN_STREAMING_SIZE)
    leftHasNewData = !isStatic

    this.realNumPart = scala.math.min(inputRows, numPart.toDouble)
  }

  override def copy(): OperatorCostModel = {
    val newOP = new ScanCostModel
    newOP.inputRows = inputRows
    newOP.isStatic = isStatic
    copyHelper(newOP)

    newOP
  }

  override def reset(cardinalityOnly: Boolean): Unit = {
    resetHelper(cardinalityOnly)
    children.foreach(_.reset(cardinalityOnly))
  }

  override def preProcess(): Unit = {
    return
  }

  override def getNextBatch(batchNums: Array[Int], isLastBatch: Boolean,
                            executable: Array[Boolean]):
  (Double, Double, Double) = {
    val batchNum =
      if (!hasNewData) 1
      else batchNums(mpIdx)

    if (executable != null && !executable(mpIdx)) (0.0, 0.0, 0.0)
    else {
      val output = (inputRows/(batchNum.toDouble*realNumPart), 0.0, 0.0)
      accumulateCost(isLastBatch, 0, tupleOperationSum(output))
      output
    }
  }
}

class AggregateCostModel extends OperatorCostModel {

  import SlothDBCostModel._

  nodeType = SLOTHAGGREGATE
  nodeName = findNameFromType(SLOTHAGGREGATE)

  var numGroups: Double = _
  var stateSize: Double = _

  override def initialize(id: Int,
                          logicalPlan: LogicalPlan,
                          chidren: Array[OperatorCostModel],
                           numPart: Int,
                 configs: Array[String]): Unit = {
    initHelper(id, logicalPlan, chidren, numPart)
    numGroups = configs(1).toDouble
    this.realNumPart = scala.math.min(numGroups, numPart.toDouble)
  }

  override def copy(): OperatorCostModel = {
    val newOP = new AggregateCostModel
    newOP.numGroups = numGroups
    copyHelper(newOP)

    newOP
  }

  override def reset(cardinalityOnly: Boolean): Unit = {
    stateSize = 0.0
    resetHelper(cardinalityOnly)
    children.foreach(_.reset(cardinalityOnly))
  }

  override def preProcess(): Unit = {
    children(LEFT).preProcess()
  }

  override def getNextBatch(batchNum: Array[Int], isLastBatch: Boolean, executable: Array[Boolean]):
  (Double, Double, Double) = {
    val realGroups = numGroups/realNumPart

    val input = children(LEFT).getNextBatch(batchNum, isLastBatch, executable)
    val output =
      if (stateSize < realGroups) {
        val insert = scala.math.min(realGroups - stateSize, input._1)
        val insert_for_update = scala.math.max(0, input._1 - (realGroups - stateSize))
        val insert_to_update = scala.math.min(stateSize, insert_for_update)
        (insert, 0.0, input._2 + input._3 + insert_to_update)
      } else {
        (0.0, 0.0, scala.math.min(tupleSum(input), realGroups))
      }

    stateSize = stateSize + input._1 - input._2

    accumulateCost(isLastBatch, stateSize, tupleOperationSum(output))
    output
  }
}

class SortCostModel extends OperatorCostModel {

  import SlothDBCostModel._

  nodeType = SLOTHSORT
  nodeName = findNameFromType(SLOTHSORT)

  var stateSize: Double = _

  override def initialize(id: Int,
                          logicalPlan: LogicalPlan,
                          children: Array[OperatorCostModel],
                           numPart: Int,
                 configs: Array[String]): Unit = {
    initHelper(id, logicalPlan, children, numPart)
  }

  override def copy(): OperatorCostModel = {
    val newOP = new SortCostModel
    copyHelper(newOP)

    newOP
  }

  override def reset(cardinalityOnly: Boolean): Unit = {
    stateSize = 0.0
    resetHelper(cardinalityOnly)
    children.foreach(_.reset(cardinalityOnly))
  }

  override def preProcess(): Unit = {
    children(LEFT).preProcess()
  }

  override def getNextBatch(batchNum: Array[Int], isLastBatch: Boolean, executable: Array[Boolean]):
  (Double, Double, Double) = {
    val output = children(LEFT).getNextBatch(batchNum, isLastBatch, executable)
    accumulateCost(isLastBatch, 0, tupleOperationSum(output))
    output
  }
}

class SelectCostModel extends OperatorCostModel {

  import SlothDBCostModel._

  nodeType = SLOTHSELECT
  nodeName = findNameFromType(SLOTHSELECT)

  var insert_to_insert: Double = _
  var delete_to_delete: Double = _
  var update_to_update: Double = _

  override def initialize(id: Int,
                          logicalPlan: LogicalPlan,
                          children: Array[OperatorCostModel],
                           numPart: Int,
                 configs: Array[String]): Unit = {
    initHelper(id, logicalPlan, children, numPart)
    insert_to_insert = configs(1).toDouble
    delete_to_delete = configs(2).toDouble
    update_to_update = configs(3).toDouble
  }

  override def copy(): OperatorCostModel = {
    val newOP = new SelectCostModel
    copyHelper(newOP)
    newOP.insert_to_insert = insert_to_insert
    newOP.delete_to_delete = delete_to_delete
    newOP.update_to_update = update_to_update

    newOP
  }

  override def reset(cardinalityOnly: Boolean): Unit = {
    resetHelper(cardinalityOnly)
    children.foreach(_.reset(cardinalityOnly))
  }

  override def preProcess(): Unit = {
    children(LEFT).preProcess()
  }

 override def getNextBatch(batchNum: Array[Int], isLastBatch: Boolean, executable: Array[Boolean]):
  (Double, Double, Double) = {
    val input = children(LEFT).getNextBatch(batchNum, isLastBatch, executable)
    val output = (input._1 * insert_to_insert,
      input._2 * delete_to_delete,
      input._3 * update_to_update)

    accumulateCost(isLastBatch, 0, tupleOperationSum(output))
    output
  }
}

class DistinctCostModel extends OperatorCostModel {

  import SlothDBCostModel._

  nodeType = SLOTHDISTINCT
  nodeName = findNameFromType(SLOTHDISTINCT)

  var numGroups: Double = _
  var stateSize: Double = _

  override def initialize(id: Int,
                          logicalPlan: LogicalPlan,
                          children: Array[OperatorCostModel],
                           numPart: Int,
                 configs: Array[String]): Unit = {
    initHelper(id, logicalPlan, children, numPart)
    numGroups = configs(1).toDouble
  }

  override def copy(): OperatorCostModel = {
    val newOP = new AggregateCostModel
    newOP.numGroups = numGroups
    copyHelper(newOP)

    newOP
  }

  override def reset(cardinalityOnly: Boolean): Unit = {
    stateSize = 0.0
    resetHelper(cardinalityOnly)
    children.foreach(_.reset(cardinalityOnly))
  }

  override def preProcess(): Unit = {
    children(LEFT).preProcess()
  }

  override def getNextBatch(batchNum: Array[Int], isLastBatch: Boolean, executable: Array[Boolean]):
  (Double, Double, Double) = {
    val input = children(LEFT).getNextBatch(batchNum, isLastBatch, executable)
    val output =
      if (stateSize < numGroups) {
        val insert = scala.math.min(numGroups - stateSize, input._1)
        (insert, 0.0, input._2)
      } else {
        (0.0, 0.0, 0.0)
      }

    stateSize = stateSize + input._1 - input._2

    accumulateCost(isLastBatch, stateSize, tupleOperationSum(output))
    output
  }
}
