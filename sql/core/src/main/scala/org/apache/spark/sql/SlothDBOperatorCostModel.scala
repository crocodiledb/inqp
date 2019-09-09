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

abstract class OperatorCostModel {

  import SlothDBCostModel._

  var children: Seq[OperatorCostModel] = _

  // Meta data for all operators
  var id: Int = _
  var nodeType: Int = _
  var nodeName: String = _
  var numPart: Int = _

  var leftHasNewData: Boolean = _
  var rightHasNewData: Boolean = _
  var leftPreprocessed: Boolean = _
  var rightPreProcessed: Boolean = _
  var lowerBlocking: Boolean = _

  // cardinality
  var outputRows: Double = _
  var opLatency: Double = _
  var opResource: Double = _

  protected def initHelper(id: Int, children: Array[OperatorCostModel], numPart: Int): Unit = {

    this.id = id
    this.children = children
    this.numPart = numPart

    leftHasNewData = false
    rightHasNewData = false
    leftPreprocessed = false
    rightPreProcessed = false
    lowerBlocking = false
  }

  protected def copyHelper(newOP: OperatorCostModel): Unit = {

    newOP.id = this.id
    newOP.children = new Array[OperatorCostModel](children.size)
    for (i <- 0 until children.size) {
      newOP.children(i) = children(i).copy()
    }
    newOP.nodeType = nodeType
    newOP.nodeName = nodeName
    newOP.numPart = numPart

    newOP.leftHasNewData = leftHasNewData
    newOP.rightHasNewData = rightHasNewData
    newOP.leftPreprocessed = leftPreprocessed
    newOP.rightPreProcessed = rightPreProcessed
    newOP.lowerBlocking = lowerBlocking
  }

  protected def resetHelper(): Unit = {
    outputRows = 0
    opLatency = 0
    opResource = 0
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

  protected def accumulateCost(batchNum: Int,
                     batchIndex: Int,
                     stateSize: Double,
                     outputTuples: Double): Unit = {
    outputRows += outputTuples
    opResource += (outputTuples * TUPLECOST + stateSize * SCANCOST)
    if (batchIndex == batchNum - 1) {
      opLatency = outputTuples * TUPLECOST + stateSize * SCANCOST
    }
  }

  protected def collectLatencyAndResource(): (Double, Double) = {
    val leftPair = if (!isBlockingOperator(this) &&
      !isScanOperator(this) && leftHasNewData) {
        children(LEFT).collectLatencyAndResource()
    } else (0.0, 0.0)

    val rightPair = if (children.size > 1 && rightHasNewData) {
      children(RIGHT).collectLatencyAndResource()
    } else (0.0, 0.0)

    return (leftPair._1 + rightPair._1 + opLatency,
      leftPair._2 + rightPair._2 + opResource)
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

  def getLatencyAndResource(batchNum: Int): (Double, Double) = {
    this.reset()
    this.preProcess()
    for (i <- 0 until batchNum) {
      this.getNextBatch(batchNum, i)
    }
    this.collectLatencyAndResource()
  }

  def initialize(id: Int,
                 children: Array[OperatorCostModel],
                 numPart: Int,
                 configs: Array[String]): Unit
  def copy(): OperatorCostModel
  def reset(): Unit
  def getNextBatch(batchNum: Int, batchIndex: Int): (Double, Double, Double)
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
                          children: Array[OperatorCostModel],
                          numPart: Int,
                 configs: Array[String]): Unit = {
    initHelper(id, children, numPart)
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

  override def reset(): Unit = {
    resetHelper()
    leftStateSize = 0.0
    rightStateSize = 0.0

    children.foreach(_.reset())
  }

  override def preProcess(): Unit = {
    if (!leftHasNewData) {
      leftPreprocessed = true
      processLeftInput(children(LEFT).getNextBatch(1, 0))
    } else children(LEFT).preProcess()

    if (!rightHasNewData) {
      rightPreProcessed = true
      processRightInput(children(RIGHT).getNextBatch(1, 0))
    } else children(RIGHT).preProcess()
  }

  override def getNextBatch(batchNum: Int, batchIndex: Int):
  (Double, Double, Double) = {
    val leftOutput = if (!leftPreprocessed) {
      val leftInput = children(LEFT).getNextBatch(batchNum, batchIndex)
      processLeftInput(leftInput)
    } else (0, 0, 0)

    val rightOutput = if (!rightPreProcessed) {
      val rightInput = children(RIGHT).getNextBatch(batchNum, batchIndex)
      processRightInput(rightInput)
    } else (0, 0, 0)

    accumulateCost(batchNum, batchIndex, 0,
      tupleSum(leftOutput) + tupleSum(rightOutput))

    return tupleAdd(leftOutput, rightOutput)
  }

  private def processLeftInput(leftInput: Tuple3[Double, Double, Double]):
  (Double, Double, Double) = {
    val left_insert_output = leftInput._1 * left_insert_to_insert * rightStateSize
    val left_delete_output = leftInput._2 * left_delete_to_delete * rightStateSize
    val left_update_output = leftInput._3 * left_update_to_update * rightStateSize
    leftStateSize = leftStateSize + leftInput._1 - leftInput._2
    outputRows += (left_insert_output + left_delete_output + left_update_output)
    (left_insert_output, left_delete_output, left_update_output)
  }

  private def processRightInput(rightInput: Tuple3[Double, Double, Double]):
  (Double, Double, Double) = {
    val right_insert_output = rightInput._1 * right_insert_to_insert * leftStateSize
    val right_delete_output = rightInput._2 * right_delete_to_delete * leftStateSize
    val right_update_output = rightInput._3 * right_update_to_update * leftStateSize
    rightStateSize = rightStateSize + rightInput._1 - rightInput._2
    outputRows += (right_insert_output + right_delete_output + right_update_output)
    (right_insert_output, right_delete_output, right_update_output)
  }
}

class ScanCostModel extends OperatorCostModel {

  import SlothDBCostModel._

  nodeType = SLOTHSCAN
  nodeName = findNameFromType(SLOTHSCAN)

  var inputRows: Double = _
  var isStatic: Boolean = _

  override def initialize(id: Int,
                          children: Array[OperatorCostModel],
                          numPart: Int,
                 configs: Array[String]): Unit = {
    initHelper(id, children, numPart)
    isStatic = configs(1) == "static"
    leftHasNewData = !isStatic
    inputRows = configs(2).toDouble
  }

  override def copy(): OperatorCostModel = {
    val newOP = new ScanCostModel
    newOP.inputRows = inputRows
    newOP.isStatic = isStatic
    copyHelper(newOP)

    newOP
  }

  override def reset(): Unit = {
    resetHelper()
    children.foreach(_.reset())
  }

  override def preProcess(): Unit = {
    children(LEFT).preProcess()
  }

  override def getNextBatch(batchNum: Int, batchIndex: Int):
  (Double, Double, Double) = {

    val output = (inputRows/batchNum, 0.0, 0.0)
    accumulateCost(batchNum, batchIndex, 0, tupleSum(output))

    return output
  }
}

class AggregateCostModel extends OperatorCostModel {

  import SlothDBCostModel._

  nodeType = SLOTHAGGREGATE
  nodeName = findNameFromType(SLOTHAGGREGATE)

  var numGroups: Double = _
  var stateSize: Double = _

  override def initialize(id: Int,
                          chidren: Array[OperatorCostModel],
                           numPart: Int,
                 configs: Array[String]): Unit = {
    initHelper(id, chidren, numPart)
    numGroups = configs(1).toDouble
  }

  override def copy(): OperatorCostModel = {
    val newOP = new AggregateCostModel
    newOP.numGroups = numGroups
    copyHelper(newOP)

    newOP
  }

  override def reset(): Unit = {
    stateSize = _
    resetHelper()
    children.foreach(_.reset())
  }

  override def preProcess(): Unit = {
    children(LEFT).preProcess()
  }

  override def getNextBatch(batchNum: Int, batchIndex: Int):
  (Double, Double, Double) = {
    val input = children(LEFT).getNextBatch(batchNum, batchIndex)
    val output =
      if (stateSize < numGroups) {
        val insert = scala.math.min(numGroups - stateSize, input._1)
        val insert_to_update = scala.math.max(0, input._1 + stateSize - numGroups)
        (insert, 0.0, input._2 + input._3 + insert_to_update)
      } else {
        (0.0, 0.0, scala.math.min(tupleSum(input), numGroups))
      }

    stateSize = stateSize + input._1 - input._2

    accumulateCost(batchNum, batchIndex, stateSize, tupleSum(output))
    output
  }
}

class SortCostModel extends OperatorCostModel {

  import SlothDBCostModel._

  nodeType = SLOTHSORT
  nodeName = findNameFromType(SLOTHSORT)

  var stateSize: Double = _

  override def initialize(id: Int,
                          children: Array[OperatorCostModel],
                           numPart: Int,
                 configs: Array[String]): Unit = {
    initHelper(id, children, numPart)
  }

  override def copy(): OperatorCostModel = {
    val newOP = new SortCostModel
    copyHelper(newOP)

    newOP
  }

  override def reset(): Unit = {
    stateSize = _
    resetHelper()
    children.foreach(_.reset())
  }

  override def preProcess(): Unit = {
    children(LEFT).preProcess()
  }

  override def getNextBatch(batchNum: Int, batchIndex: Int):
  (Double, Double, Double) = {
    val output = children(LEFT).getNextBatch(batchNum, batchIndex)
    accumulateCost(batchNum, batchIndex, 0, tupleSum(output))
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
                          children: Array[OperatorCostModel],
                           numPart: Int,
                 configs: Array[String]): Unit = {
    initHelper(id, children, numPart)
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

  override def reset(): Unit = {
    resetHelper()
    children.foreach(_.reset())
  }

  override def preProcess(): Unit = {
    children(LEFT).preProcess()
  }

  override def getNextBatch(batchNum: Int, batchIndex: Int):
  (Double, Double, Double) = {
    val input = children(LEFT).getNextBatch(batchNum, batchIndex)
    val output = (input._1 * insert_to_insert,
      input._2 * delete_to_delete,
      input._3 * update_to_update)
    accumulateCost(batchNum, batchIndex, 0, tupleSum(output))
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
                          children: Array[OperatorCostModel],
                           numPart: Int,
                 configs: Array[String]): Unit = {
    initHelper(id, children, numPart)
    numGroups = configs(1).toDouble
  }

  override def copy(): OperatorCostModel = {
    val newOP = new AggregateCostModel
    newOP.numGroups = numGroups
    copyHelper(newOP)

    newOP
  }

  override def reset(): Unit = {
    stateSize = _
    resetHelper()
    children.foreach(_.reset())
  }

  override def preProcess(): Unit = {
    children(LEFT).preProcess()
  }

  override def getNextBatch(batchNum: Int, batchIndex: Int):
  (Double, Double, Double) = {
    val input = children(LEFT).getNextBatch(batchNum, batchIndex)
    val output =
      if (stateSize < numGroups) {
        val insert = scala.math.min(numGroups - stateSize, input._1)
        (insert, 0.0, input._2)
      } else {
        (0.0, 0.0, 0.0)
      }

    stateSize = stateSize + input._1 - input._2

    accumulateCost(batchNum, batchIndex, stateSize, tupleSum(output))
    output
  }
}
