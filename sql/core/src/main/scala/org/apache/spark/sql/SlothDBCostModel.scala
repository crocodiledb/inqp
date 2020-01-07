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
import org.apache.spark.sql.catalyst.plans.{FullOuter, LeftAnti, LeftOuter, RightOuter}
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.execution.{SlothFilterExec, SortExec, SparkPlan}
import org.apache.spark.sql.execution.aggregate.SlothHashAggregateExec
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2ScanExec
import org.apache.spark.sql.execution.streaming._

class SlothDBCostModel extends Logging {

  import SlothDBCostModel._

  var incPlan: OperatorCostModel = _
  var subPlans: Array[SlothSubPlan] = _
  var batchNums: Array[Int] = _
  var batchSteps: Array[Int] = _
  var currentStep: Int = _

  var totalBatchNum: Int = _
  var totalBatchStep: Int = _
  var incAware: Boolean = _
  var testOverhead: Boolean = _

  var BATCH_LATENCY: Double = _
  var BATCH_RESOURCE: Double = _

  var blockingOperators: Array[OperatorCostModel] = _
  var sources: Array[BaseStreamingSource] = _
  var joinOperators: Array[OperatorCostModel] = _
  var endOffsets: Array[SlothOffset] = _

  var executableSubPlans: Array[SlothSubPlan] = _

  var mpDecompose: Boolean = _

  var costBias: Double = _

  val parallelism = 50

  val overEstBias = Array(1.0, 2.0, 3.0, 4.0, 5.0)
  val underEstBias = Array(1.0, 0.5, 0.33, 0.25, 0.2)

  def initialize(logicalPlan: LogicalPlan,
                 name: String,
                 slothdbStatDir: String,
                 numPart: Int,
                 mpDecompose: Boolean,
                 incAware: Boolean,
                 costBias: Double,
                 maxStep: Int,
                 testOverhead: Boolean): Unit = {
    this.incAware = incAware
    this.testOverhead = testOverhead

    SlothDBCostModel.MAX_BATCHNUM = maxStep

    val biasArray =
      if (costBias > 0.0) overEstBias
      else underEstBias
    val rnd = new scala.util.Random(System.currentTimeMillis)
    this.costBias = biasArray(rnd.nextInt(costBias.abs.toInt))

    // incPlan = buildPlan(logicalPlan, name, slothdbStatDir, numPart)
    incPlan = buildPlan(logicalPlan, name, slothdbStatDir, 20)
    populateHasNewData(incPlan)

    val blockingBuffer = new ArrayBuffer[OperatorCostModel]()
    val joinBuffer = new ArrayBuffer[OperatorCostModel]()
    collectJoinBlockingOperators(incPlan, joinBuffer, blockingBuffer)
    joinOperators = joinBuffer.toArray
    blockingOperators = blockingBuffer.toArray

    val sourceBuffer = new ArrayBuffer[BaseStreamingSource]()
    collectSources(incPlan, sourceBuffer)
    sources = sourceBuffer.toArray

    incPlan.setMPIdx(0)
    val pair = computeLatencyAndResource(1, incPlan, false, cardinalityOnly)
    BATCH_LATENCY = pair._1
    BATCH_RESOURCE = pair._2

    totalBatchNum = 0
    totalBatchStep = 0

    this.mpDecompose = mpDecompose

    if (incAware) {
      subPlans = buildSubPlans(incPlan, mpDecompose)
      batchSteps = Array.fill[Int](subPlans.length)(0)
      currentStep = 0
    }
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
    val (op, newPlan) = buildOperator(logicalPlan)

    var newLine: String = br.readLine()
    while (skipLine(newLine)) {
      newLine = br.readLine()
    }
    if (newLine == null) assert(false, "BuildChildPlan: should not reach here")
    val strArray = newLine.split(",")
    if (strArray(0) != op.nodeName) {
      assert(strArray(0) == op.nodeName)
    }

    val children = new Array[OperatorCostModel](newPlan.children.size)
    newPlan.children.zipWithIndex.foreach(pair => {
      val childPlan = pair._1
      val index = pair._2
      children(index) = buildChildPlan(id + index + 1, childPlan, numPart, br)
    })

    op.initialize(id, newPlan, children, numPart, strArray, costBias)

    return op
  }

  private def skipLine(str: String): Boolean = {
    str.startsWith("filter,1.0")
  }

  private def buildOperator(logicalPlan: LogicalPlan):
  Tuple2[OperatorCostModel, LogicalPlan] = {
    logicalPlan match {
      case _: Aggregate =>
        (new AggregateCostModel, logicalPlan)
      case _: Join =>
        (new JoinCostModel, logicalPlan)
      case _: StreamingExecutionRelation =>
        (new ScanCostModel, logicalPlan)
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
    rootSubPlan.initialize(op.copy(), index, -1, mpDecompose, testOverhead)
    subPlanSet.append(rootSubPlan)

    while (index < subPlanSet.length) {
      val tmpSubPlan = subPlanSet(index)
      val mpNum = findOneSubPlan(tmpSubPlan, tmpSubPlan.root, subPlanSet, mpDecompose)
      tmpSubPlan.setMPNum(mpNum)
      index += 1
    }

    // if (mpDecompose) {
    //   val pathSet = new ArrayBuffer[SlothSubPlan]()
    //   decomposeIntoMP(null, subPlanSet(0), pathSet)
    //   var i = 0
    //   while (i < pathSet.length) {
    //     val parentPath = pathSet(i)
    //     val subPlan = findSubPlanOfParentPath(parentPath, subPlanSet)
    //     decomposeIntoMP(parentPath, subPlan, pathSet)
    //     i += 1
    //   }
    //   pathSet.toArray
    // } else {
    subPlanSet.toArray
    // }
  }

  private def findOneSubPlan(subPlan: SlothSubPlan,
                             op: OperatorCostModel,
                             subPlanSet: ArrayBuffer[SlothSubPlan],
                             mpDecompose: Boolean): Int = {
    if (isBlockingOperator(op)) {
      val childSubPlan = new SlothSubPlan
      val childIndex = subPlanSet.length
      childSubPlan.initialize(op.copy().setLowerBlockingOperator(true),
        childIndex, subPlan.index, mpDecompose, testOverhead)
      subPlan.addChildDep(childIndex)
      subPlanSet.append(childSubPlan)
      return 1;
    } else if (isScanOperator(op)) {
      return 1;
    } else {
      return op.children.map(child => findOneSubPlan(subPlan, child, subPlanSet, mpDecompose)).sum
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

  def loadEndOffsets(inputEndOffsets: Map[BaseStreamingSource, String]): Unit = {

    import SlothOffsetUtils._

    if (incAware) {

      subPlans.foreach(subPlan => {
        subPlan.sources.zipWithIndex.foreach(sourceWithIndex => {
          val source = sourceWithIndex._1
          val index = sourceWithIndex._2
          subPlan.endOffsets(index) = jsonToOffset(inputEndOffsets.get(source).get)
        })
      })
    } else {
      endOffsets = new Array[SlothOffset](sources.length)
      sources.zipWithIndex.foreach(sourceWithIndex => {
        val source = sourceWithIndex._1
        val index = sourceWithIndex._2
        endOffsets(index) = jsonToOffset(inputEndOffsets.get(source).get)
      })
    }

  }

  // With one constraint and optimize the other one
  def genTriggerPlanForLatencyConstraint(latencyConstraint: Double,
                                         threshold: Double, sampleTime: Double)
  : Unit = {
    val realConstraint = latencyConstraint * BATCH_LATENCY

    if (testOverhead) {
      if (OPT_METHOD == OPT_GREEDY) {
        genMPPlanForLatencyConstraintOverhead(latencyConstraint, threshold)
      } else {
        genMPPlanUsingRandomOrBruteforce(latencyConstraint, sampleTime)
      }
    } else if (incAware && mpDecompose) {
      if (OPT_METHOD == OPT_GREEDY) {
        genMPPlanForLatencyConstraint(latencyConstraint, threshold)
      } else {
        genMPPlanUsingRandomOrBruteforce(latencyConstraint, sampleTime)
      }
    } else if (incAware) {
      val tmpBatchNums = Array.fill[Int](subPlans.length)(MAX_BATCHNUM)
      var curLatency = computeLatencyForSubPlans(subPlans, tmpBatchNums, true, cardinalityOnly)
      var preIndex: Int = -1

      val random = scala.util.Random

      while (curLatency <= realConstraint) {

        val pair = subPlans.filter(_.hasNewData())
          .filter(validBatchNumForLatencyConstraint(_, tmpBatchNums))
          .map(subPlan => {
            val index = subPlan.index

            val batchNum = tmpBatchNums(index)
            val incability = subPlan.computeIncrementability(batchNum, batchNum - 1)

            (incability, index)
          }).reduceLeft(
            (pairA, pairB) => {
              if (random.nextFloat < threshold) { // correct case
                if (pairA._1 < pairB._1) pairA
                else pairB
              } else {
                if (pairA._1 < pairB._1) pairB
                else pairA
              }
            }
          )

        preIndex = pair._2
        tmpBatchNums(pair._2) -= 1

        curLatency = computeLatencyForSubPlans(subPlans, tmpBatchNums, true, cardinalityOnly)
      }

      if (preIndex != -1) tmpBatchNums(preIndex) += 1
      batchNums = tmpBatchNums
      batchNums.zipWithIndex.foreach(batchNumWithIndex => {
        printf(s"${batchNumWithIndex._2}: ${batchNumWithIndex._1}\n")
      })
    } else {
      var tmpTotalBatchNum = MAX_BATCHNUM
      var pair = computeLatencyAndResource(tmpTotalBatchNum, incPlan, false, cardinalityOnly)
      var curLatency = pair._1

      while (curLatency <= realConstraint) {
        // printf(s"TotalBatchNum: ${tmpTotalBatchNum}\t" +
        //   s"\t${curLatency}\t${pair._2}\n")

        tmpTotalBatchNum -= 1
        pair = computeLatencyAndResource(tmpTotalBatchNum, incPlan, false, cardinalityOnly)
        curLatency = pair._1
      }

      totalBatchNum = scala.math.min(tmpTotalBatchNum + 1, MAX_BATCHNUM)
      printf(s"TotalBatchNum: ${totalBatchNum}\n")
    }

    if (mpDecompose) {
      subPlans.foreach(subPlan => {
        subPlan.mpBatchNums.zipWithIndex.foreach(batchNumWithIndex => {
          printf(s"${batchNumWithIndex._2}: ${batchNumWithIndex._1}\n")
        })})
    }

  }

  def genTriggerPlanForResourceConstraint(resourceConstraint: Double, threshold: Double)
  : Unit = {
    val realConstraint = resourceConstraint * BATCH_RESOURCE

    if (incAware && mpDecompose) {
      genMPPlanForResourceConstraint(resourceConstraint, threshold)
    } else if (incAware) {
      val tmpBatchNums = Array.fill[Int](subPlans.length)(MIN_BATCHNUM)
      var curResource = computeResourceForSubPlans(subPlans, tmpBatchNums, true, cardinalityOnly)
      var preIndex: Int = -1

      val random = scala.util.Random

      while (curResource <= realConstraint) {

        val pair = subPlans.filter(_.hasNewData())
          .filter(validBatchNumForResourceConstraint(_, tmpBatchNums))
          .map(subPlan => {
            val index = subPlan.index

            val batchNum = tmpBatchNums(index)
            val incability = subPlan.computeIncrementability(batchNum, batchNum + 1)

            (incability, index)
          }).reduceLeft(
          (pairA, pairB) => {
            if (random.nextFloat < threshold) { // correct case
              if (pairA._1 > pairB._1) pairA
              else pairB
            } else {
              if (pairA._1 > pairB._1) pairB
              else pairA
            }
          })

        preIndex = pair._2
        tmpBatchNums(pair._2) += 1

        curResource = computeResourceForSubPlans(subPlans, tmpBatchNums, true, cardinalityOnly)
      }

      if (preIndex != -1) tmpBatchNums(preIndex) -= 1
      batchNums = tmpBatchNums

      batchNums.zipWithIndex.foreach(batchNumWithIndex => {
        printf(s"${batchNumWithIndex._2}: ${batchNumWithIndex._1}\n")
      })

    } else {
      var tmpTotalBatchNum = MIN_BATCHNUM
      var curResource = computeLatencyAndResource(tmpTotalBatchNum,
        incPlan, false, cardinalityOnly)._2

      while (curResource < realConstraint) {
        tmpTotalBatchNum += 1
        curResource = computeLatencyAndResource(tmpTotalBatchNum,
          incPlan, false, cardinalityOnly)._2
      }

      totalBatchNum = scala.math.max(tmpTotalBatchNum - 1, MIN_BATCHNUM)
      printf(s"TotalBatchNum: ${totalBatchNum}\n")
    }

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


  def genMPPlanForResourceConstraint(resourceConstraint: Double, threshold: Double)
  : Unit = {
    val realConstraint = resourceConstraint * BATCH_RESOURCE

    var curResource = computeResourceForMPs(subPlans, true, cardinalityOnly)
    var preIndex: Int = -1

    val random = scala.util.Random

    while (curResource > realConstraint) {

      val smallInc = random.nextFloat < threshold

      val pair = subPlans.filter(_.hasNewData())
        .map(_.findParentBatchNum(subPlans))
        .map(subPlan => {

          val incability =
            subPlan.computeMPIncrementabilityForLatencyConstraint(smallInc)

          (incability, subPlan.index)
        }).filter(pair => {
        pair._1 != MAX_INCREMENTABILITY
      }).reduceLeft(
        (pairA, pairB) => {
          if (smallInc) {
            if (pairA._1 < pairB._1) pairA
            else pairB
          } else {
            if (pairA._1 < pairB._1) pairB
            else pairA
          }
        })

      preIndex = pair._2
      subPlans(preIndex).decreaseMPBatchNum()

      curResource = computeResourceForMPs(subPlans, true, cardinalityOnly)
    }

    // if (preIndex != -1) subPlans(preIndex).increaseMPBatchNum()

    subPlans.foreach(subPlan => {
      subPlan.mpBatchNums.zipWithIndex.foreach(batchNumWithIndex => {
        printf(s"${batchNumWithIndex._2}: ${batchNumWithIndex._1}\n")
      })})
  }


  def genMPPlanForLatencyConstraint(latencyConstraint: Double, threshold: Double)
  : Unit = {
    val realConstraint = latencyConstraint * BATCH_LATENCY

    var curLatency = computeLatencyForMPs(subPlans, true, cardinalityOnly)
    var preIndex: Int = -1

    val random = scala.util.Random

    while (curLatency <= realConstraint) {

      val smallInc = (random.nextFloat() < threshold)

      val pair = subPlans.filter(_.hasNewData())
        .map(_.findParentBatchNum(subPlans))
        .map(subPlan => {

          val incability =
            subPlan.computeMPIncrementabilityForLatencyConstraint(smallInc)

          (incability, subPlan.index)
        }).filter(pair => {
        pair._1 != MAX_INCREMENTABILITY
      }).reduceLeft(
        (pairA, pairB) => {
          if (smallInc) {
            if (pairA._1 < pairB._1) pairA
            else pairB
          } else {
            if (pairA._1 < pairB._1) pairB
            else pairA
          }
        })

      preIndex = pair._2
      subPlans(preIndex).decreaseMPBatchNum()

      curLatency = computeLatencyForMPs(subPlans, true, cardinalityOnly)
    }

    if (preIndex != -1) subPlans(preIndex).increaseMPBatchNum()
  }

  class BruteforceThread(start: Long, end: Long, parallelism: Int,
                         index: Int, inputMinResource: Double,
                         realConstraint: Double) extends Thread {

    var localMinResource: Double = inputMinResource

    val localSubPlans: Array[SlothSubPlan] = new Array[SlothSubPlan](subPlans.length)
    for (i <- 0 until localSubPlans.length) {
      localSubPlans(i) = subPlans(i).copy()
    }

    val localSubPlanBatchNum = new Array[Array[Int]](localSubPlans.size)
    for (i <- 0 until localSubPlans.length) {
      localSubPlanBatchNum(i) = Array.fill(localSubPlans(i).mpCount)(MAX_BATCHNUM)
    }

    override def run(): Unit = {
      var count = 0
      var validCount = 0
      var curIdx = index.toLong * end/parallelism
      val endIdx = curIdx + end/parallelism
      while (curIdx < endIdx) {
        var tmpCurIdx = curIdx
        localSubPlans.foreach(subPlan => {
          for (i <- 0 until subPlan.mpCount) {
            subPlan.mpBatchNums(i) = (tmpCurIdx % MAX_BATCHNUM + 1).toInt
            tmpCurIdx = tmpCurIdx / MAX_BATCHNUM
          }
        })

        val isValidBatchNums =
           !localSubPlans.filter(_.hasNewData())
            .map(_.findParentBatchNum(localSubPlans))
            .exists(!_.validBatchNums())

        if (isValidBatchNums) {
          validCount += 1
          val newLatency = computeLatencyForMPsSimple(localSubPlans, true, cardinalityOnly)
          if (newLatency < realConstraint) {
            val newResource = computeResourceForMPsSimple(localSubPlans, true, cardinalityOnly)
            if (newResource < localMinResource) {
              localMinResource = newResource
              for (i <- 0 until localSubPlans.length) {
                localSubPlans(i).getBatchNum(localSubPlanBatchNum(i))
              }
            }
          }
        }

        curIdx += 1
        count += 1

        if ((count + 1) % 100000 == 0) printf(s"$index: ${count + 1}\n")
      }

      printf(s"Valid Count $index $validCount\n")
    }
  }

  def genMPPlanUsingRandomOrBruteforce(latencyConstraint: Double,
                                       sampleTime: Double): Unit = {
    val realConstraint = latencyConstraint * BATCH_LATENCY

    val subPlanBatchNum = new Array[Array[Int]](subPlans.size)
    for (i <- 0 until subPlans.length) {
      subPlanBatchNum(i) = Array.fill(subPlans(i).mpCount)(MAX_BATCHNUM)
    }
    var minResource = computeResourceForMPsSimple(subPlans, true, cardinalityOnly)

    var time = 0L
    var validCount = 0

    if (OPT_METHOD == OPT_SAMPLE) {
      val sample_threshold = (sampleTime * NANOPERSECOND.toDouble).toLong
      while (time < sample_threshold) {

        val start = System.nanoTime()
        subPlans.foreach(_.nextMPBatchNum())
        val isValidBatchNums =
          !subPlans.filter(_.hasNewData())
            .map(_.findParentBatchNum(subPlans))
            .exists(!_.validBatchNums())

        if (isValidBatchNums) {
          validCount += 1
          val newLatency = computeLatencyForMPs(subPlans, true, cardinalityOnly)
          val newResource = computeResourceForMPs(subPlans, true, cardinalityOnly)
          if (newLatency < realConstraint) {
            if (newResource < minResource) {
              minResource = newResource
              for (i <- 0 until subPlans.length) {
                subPlans(i).getBatchNum(subPlanBatchNum(i))
              }
            }
          }
        }
        time += System.nanoTime() - start
      }
      printf(s"${time/1000}, ${validCount}\n")

    } else {
      val totalMPCount = math.pow(MAX_BATCHNUM, subPlans.map(_.mpCount).sum).toLong
      val bfThreadArray = new Array[BruteforceThread](parallelism)
      for (i <- 0 until parallelism) {
        bfThreadArray(i) = new BruteforceThread(
          0, totalMPCount,
          parallelism, i,
          minResource, realConstraint)
      }

      bfThreadArray.foreach(_.start())
      bfThreadArray.foreach(_.join())

      bfThreadArray.foreach(thread => {
        if (thread.localMinResource < minResource) {
          minResource = thread.localMinResource
          for (i <- 0 until subPlanBatchNum.length) {
            val oneSubPlanBatchNum = subPlanBatchNum(i)
            for (j <- 0 until oneSubPlanBatchNum.length) {
              subPlanBatchNum(i)(j) = thread.localSubPlanBatchNum(i)(j)
            }
          }
        }
      })

    }

    subPlans.zipWithIndex.foreach(pair => {
      val subPlan = pair._1
      val idx = pair._2
      subPlan.putBatchNum(subPlanBatchNum(idx))
    })

  }

  def genMPPlanForLatencyConstraintOverhead(latencyConstraint: Double, threshold: Double)
  : Unit = {
    for (i <- 0 until subPlans.length) {
      for (j <- 0 until subPlans(i).mpCount) {
        subPlans(i).mpBatchNums(j) = MIN_BATCHNUM
      }
    }

    val realConstraint = latencyConstraint * BATCH_LATENCY
    var curLatency = computeLatencyForMPsSimple(subPlans, true, cardinalityOnly)

    var preIndex: Int = -1

    var timeA = 0L
    var timeB = 0L
    var validCount = 0

    var terminate = false

    while (curLatency > realConstraint && !terminate) {

      val pair = subPlans.filter(_.hasNewData())
        .map(_.findChildBatchNum(subPlans))
        .map(subPlan => {

          val start = System.nanoTime()
          val incability =
            subPlan.computeMPIncrementabilityForOverhead()
          timeA += (System.nanoTime() - start)

          (incability, subPlan.index)
        }).reduceLeft(
        (pairA, pairB) => {
          val incA = pairA._1
          val incB = pairB._1
          if (incA < incB) pairB
          else pairA
        })

      if (pair._1 == MIN_INCREMENTABILITY) {
        terminate = true
      } else {
        preIndex = pair._2
        subPlans(preIndex).increaseMPBatchNum()
        subPlans(preIndex).applyLRPair()

        val start = System.nanoTime()
        validCount += 1
        val newLatency = computeLatencyForMPsSimple(subPlans, true, cardinalityOnly)
        if (math.abs((newLatency - curLatency)/curLatency) < CHANGE_THRESHOLD) {
          subPlans(preIndex).increaseDeltaStep()
        } else {
          subPlans(preIndex).resetDeltaStep()
        }
        curLatency = newLatency
        timeB += (System.nanoTime() - start)
      }
    }

    printf(s"${timeA/1000}, ${timeB/1000}, ${validCount}\n")
  }

  def constructNewData(): Map[BaseStreamingSource, String] = {

    if (incAware) {
      if (mpDecompose) {
        while (currentStep < MAX_BATCHNUM) {
          currentStep += 1

          subPlans.foreach(_.resetMPExecutable())
          findExecutableMPs(subPlans(0), currentStep)

          val isExecutable = subPlans.flatMap(
            subPlan => {
              subPlan.mpExecutable.map(executable =>
              executable)
            }
          ).exists(executable => executable)

          if (isExecutable) {
            val sourceMap = subPlans.flatMap({
              subPlan => {
                subPlan.mpExecutable.zipWithIndex
                  .filter(executableWithIndex => {
                    subPlan.mpIsSource(executableWithIndex._2)
                  })
                  .map(executableWithIndex => {
                    val mpIndex = executableWithIndex._2
                    val sourceIndex = subPlan.mpOPIndex(mpIndex)
                    val source = subPlan.sources(sourceIndex)
                    val offset =
                      subPlan.endOffsets(sourceIndex)
                        .getOffsetByIndex(subPlan.mpBatchNums(mpIndex),
                          subPlan.mpBatchSteps(mpIndex))
                    (source, SlothOffsetUtils.offsetToJSON(offset))
                  })
              }
            }).toMap

            printf(s"currentStep ${currentStep}\n")

            return sourceMap
          }

        }

        null

      } else {
        while (currentStep < MAX_BATCHNUM) {

          currentStep += 1

          val subPlanSet = new ArrayBuffer[SlothSubPlan]()

          // TODO: this has a bug for maintenance path
          // Find a subset of subplans to be executed
          findExecutableSubPlans(subPlanSet, subPlans(0), currentStep)

          if (subPlanSet.nonEmpty) {
            executableSubPlans = subPlanSet.toArray
            return subPlans.flatMap(subPlan => {
              val index = subPlan.index
              subPlan.sources.zipWithIndex.map(sourceWithIndex => {
                val source = sourceWithIndex._1
                val tmpIndex = sourceWithIndex._2
                val offset =
                  subPlan.endOffsets(tmpIndex).getOffsetByIndex(batchNums(index), batchSteps(index))
                (source, SlothOffsetUtils.offsetToJSON(offset))
              })
            }).toMap
          }

        }

        null
      }
    } else {
      while (currentStep < MAX_BATCHNUM) {
        currentStep += 1
        val newBatchStep = findNextStep(currentStep, totalBatchNum, totalBatchStep)
        if (totalBatchStep < newBatchStep) {
          totalBatchStep = newBatchStep

          val sourceMap = sources.zipWithIndex.map(sourceWithIndex => {
            val source = sourceWithIndex._1
            val tmpIndex = sourceWithIndex._2
            val offset =
              endOffsets(tmpIndex).getOffsetByIndex(totalBatchNum, totalBatchStep)
            (source, SlothOffsetUtils.offsetToJSON(offset))
          }).toMap


          printf(s"currentStep ${currentStep}\n")

          return sourceMap
        }
      }

      null
    }
  }

  private def findExecutableSubPlans(subPlanSet: ArrayBuffer[SlothSubPlan],
                                     subPlan: SlothSubPlan,
                                     currentStep: Int): Boolean = {
    val executable =
      !subPlan.getChildDep().map(childIndex => {
        val childSubPlan = subPlans(childIndex)
        findExecutableSubPlans(subPlanSet, childSubPlan, currentStep)
      }).exists(executable => !executable)

    val index = subPlan.index
    val newBatchStep = findNextStep(currentStep, batchNums(index), batchSteps(index)
    )
    if (executable && batchSteps(index) < newBatchStep) {
      batchSteps(index) = newBatchStep
      subPlanSet.append(subPlan)

      true
    } else {
      false
    }
  }

  private def findExecutableMPs(subPlan: SlothSubPlan, currentStep: Int): Boolean = {
    val executable =
      !subPlan.getChildDep().map(childIndex => {
        val childSubPlan = subPlans(childIndex)
        findExecutableMPs(childSubPlan, currentStep)
      }).exists(executable => !executable)

    !subPlan.mpBatchNums.zipWithIndex.map(batchNumWithIdx => {
      val batchNum = batchNumWithIdx._1
      val mpIdx = batchNumWithIdx._2
      val isSource = subPlan.mpIsSource(mpIdx)
      val batchStep = subPlan.mpBatchSteps(mpIdx)

      if (!isSource && !executable) {
        false
      } else {
        val newBatchStep = findNextStep(currentStep, batchNum, batchStep)
        if (batchStep < newBatchStep) {
          subPlan.mpBatchSteps(mpIdx) = newBatchStep
          subPlan.mpExecutable(mpIdx) = true
          true
        } else {
          subPlan.mpExecutable(mpIdx) = false
          false
        }
      }

    }).exists(isExecutable => !isExecutable)
  }

  private def findNextStep(currentStep: Double,
                           batchNum: Int,
                           batchStep: Int): Int = {
    var newBatchStep = batchStep
    var progress = computeCurrentStep(batchNum, newBatchStep)
    while ((currentStep.toDouble + 0.001) >= progress) {
      newBatchStep += 1
      progress = computeCurrentStep(batchNum, newBatchStep)
    }
    newBatchStep - 1
  }

  private def computeCurrentStep(batchNum: Int, batchStep: Int): Double = {
    ((MAX_BATCHNUM.toDouble/batchNum.toDouble) * batchStep.toDouble)
  }

  def triggerBlockingExecution(plan: SparkPlan): Unit = {
    triggerBlockingExecutionHelper(plan, 0)
  }

  private def triggerBlockingExecutionHelper(plan: SparkPlan,
                                             index: Int): Int = {
    var newIndex = index

    if (isBlockingExecutor(plan)) {
      if (incAware) {
        val blockingOP = blockingOperators(index)
        if (isBlockingOPExecutable(blockingOP)) {
          plan.setRepairMode(true)
        } else {
          plan.setRepairMode(false)
        }
      } else {
        plan.setRepairMode(true)
      }
      newIndex += 1
    }

    plan.children.foreach(child => {
      newIndex = triggerBlockingExecutionHelper(child, newIndex)
    })

    return newIndex
  }

  private def isBlockingExecutor(plan: SparkPlan): Boolean = {
    plan match {
      case _: SlothHashAggregateExec =>
        true
      case _: SlothDeduplicateExec =>
        true
      case _: SortExec =>
        true
      case _ =>
        false
    }
  }

  private def isBlockingOPExecutable(op: OperatorCostModel): Boolean = {

    if (mpDecompose) {
      subPlans.flatMap(subPlan => {
        subPlan.mpExecutable.zipWithIndex.filter(_._1)
          .filter(executableWithIndex => {
            val mpIdx = executableWithIndex._2
            !subPlan.mpIsSource(mpIdx)
          }).map( executableWithIndex => {
          val mpIdx = executableWithIndex._2
          val blockingIdx = subPlan.mpOPIndex(mpIdx)
          val blockingOP = subPlan.blockingOperators(blockingIdx)
          blockingOP.id == op.id
        })
      }).exists(isExecutable => isExecutable)
    } else {
      executableSubPlans.flatMap(subPlan => {
        subPlan.blockingOperators.map(blockingOP => {
          blockingOP.id == op.id
        })
      }).exists(isExecutable => isExecutable)
    }
  }

  private def collectJoinBlockingOperators(op: OperatorCostModel,
                                           joinBuffer: ArrayBuffer[OperatorCostModel],
                                           blockingBuffer: ArrayBuffer[OperatorCostModel]):
  Unit = {
    if (isBlockingOperator(op)) {
      blockingBuffer.append(op)
    } else if (isJoinOperator(op)) {
      joinBuffer.append(op)
    }

    op.children.foreach(child => {
      collectJoinBlockingOperators(child, joinBuffer, blockingBuffer)})
  }

  private def collectSources(op: OperatorCostModel,
                             sourceBuffer: ArrayBuffer[BaseStreamingSource]): Unit = {
    if (isScanOperator(op) && op.hasNewData()) {
      sourceBuffer.append(op.logicalPlan.asInstanceOf[StreamingExecutionRelation].source)
    }

    op.children.foreach(child => {
      collectSources(child, sourceBuffer)})
  }

  def estimateLatencyAndResource(batchNum: Int): (Double, Double) = {
    computeLatencyAndResource(batchNum, incPlan, false, cardinalityOnly)
  }

  def estimateCardinality(batchNum: Int): Double = {
    computeLatencyAndResource(batchNum, incPlan, false, true)._2
  }

  def estimateCardArray(batchNum: Int): Array[Double] = {
    incPlan.getCardinalityArray(batchNum)
  }

  def groundTruthLatencyAndResource(totalOutputRows: Long,
                                    totalScanOutputRows: Long,
                                    finalOutputRows: Long,
                                    finalScanOutputRows: Long,
                                    batchNum: Int): (Double, Double) = {
    val numAggOp = blockingOperators.size
    val numJoinOp = joinOperators.size

    val resource =
      (totalOutputRows - totalScanOutputRows).toDouble * OTHERUNITCOST +
      totalScanOutputRows.toDouble * SCANUNITCOST +
      numJoinOp * JOINSTATICCOST * batchNum +
      numAggOp * AGGSTATICCOST * batchNum

    val latency =
      (finalOutputRows - finalScanOutputRows).toDouble * OTHERUNITCOST +
      finalScanOutputRows.toDouble * SCANUNITCOST +
      numJoinOp * JOINSTATICCOST +
      numAggOp * AGGSTATICCOST

    (latency, resource)
  }

}

class SlothSubPlan {

  import SlothDBCostModel._

  var root: OperatorCostModel = _
  var index: Int = _
  var parentDep: Int = -1
  private var childDeps: Array[Int] = new Array[Int](0)
  var mpNum: Int = _
  var mpDecompose: Boolean = _

  var mpCount: Int = 0

  var sources: Array[BaseStreamingSource] = _
  var endOffsets: Array[SlothOffset] = _
  var blockingOperators: Array[OperatorCostModel] = _

  var mpBatchNums: Array[Int] = _
  var mpBatchSteps: Array[Int] = _
  var mpExecutable: Array[Boolean] = _
  var mpIsSource: ArrayBuffer[Boolean] = new ArrayBuffer[Boolean]()
  var mpChildBatchNums: Array[Int] = _
  var mpOPIndex: ArrayBuffer[Int] = new ArrayBuffer[Int]()

  var mpDeltaSteps: Array[Int] = _
  var prevLatency: Double = Double.MinValue
  var prevResource: Double = Double.MinValue
  var tmpPrevLatency: Double = Double.MinValue
  var tmpPrevResource: Double = Double.MinValue

  var mpParentBatchNum: Int = MIN_BATCHNUM - 1

  def findLeafNodes(): Unit = {
    val sourceAB = new ArrayBuffer[BaseStreamingSource]()
    val blockingAB = new ArrayBuffer[OperatorCostModel]()

    findLeafHelper(sourceAB, blockingAB, root)
    sources = sourceAB.toArray
    blockingOperators = blockingAB.toArray
    endOffsets = new Array[SlothOffset](sources.length)
  }

  private def findLeafHelper(sourceAB: ArrayBuffer[BaseStreamingSource],
                             blockingAB: ArrayBuffer[OperatorCostModel],
                             op: OperatorCostModel): Unit = {
    if (isScanOperator(op)) {
      op.setMPIdx(mpCount)
      if (mpDecompose) {
        mpIsSource.append(true)
        mpOPIndex.append(sourceAB.length)
        mpCount += 1
      }
      val opExec = op.logicalPlan.asInstanceOf[StreamingExecutionRelation]
      sourceAB.append(opExec.source)
    } else if (isBlockingOperator(op) && op.hasNewData) {
      op.setMPIdx(mpCount)
      if (mpDecompose) {
        mpIsSource.append(false)
        mpOPIndex.append(blockingAB.length)
        mpCount += 1
      }
      blockingAB.append(op)
      return
    } else {
      op.children.filter(_.hasNewData).foreach(child => {
        findLeafHelper(sourceAB, blockingAB, child)
      })
    }
  }

  def hasNewData(): Boolean = {
    root.hasNewData()
  }

  def computeIncrementability(batchNumA: Int, batchNumB: Int): Double = {
    if (batchNumA == MIN_BATCHNUM - 1 || batchNumB == MIN_BATCHNUM - 1) {
      return MAX_INCREMENTABILITY
    } else if (batchNumA == MAX_BATCHNUM + 1 || batchNumB == MAX_BATCHNUM + 1) {
      return MIN_INCREMENTABILITY
    } else {
      val lrPairA = computeLatencyAndResource(batchNumA, root, true, cardinalityOnly)
      val lrPairB = computeLatencyAndResource(batchNumB, root, true, cardinalityOnly)

      return (lrPairA._1 - lrPairB._1)/(lrPairB._2 - lrPairA._2)
    }
  }

  var curMPIdx: Int = -1

  val rnd = scala.util.Random

  def computeInc(tupleA: (Double, Double),
                              tupleB: (Double, Double)): Double = {
    (tupleB._1 - tupleA._1)/(tupleA._2 - tupleB._2)
  }

  def computeIncForLatencyHelper(newBatchNums: Array[Int],
                                 isSource: Boolean,
                                 isOuterSide: Boolean,
                                 antiOutercase: Boolean,
                                 testOverhead: Boolean): Double = {
    if (testOverhead) {
      val lrPairA = computeLRbyBatchNumsSimple(mpBatchNums, mpIsSource.toArray,
          root, true, cardinalityOnly)
      val lrPairB = computeLRbyBatchNumsSimple(newBatchNums, mpIsSource.toArray,
          root, true, cardinalityOnly)

      (lrPairB._1 - lrPairA._1) / (lrPairA._2 - lrPairB._2)
    } else {
      if (antiOutercase) {
        // This is for anti/outer join

        val lrPairALarger = computeLRbyBatchNums(mpBatchNums, mpIsSource.toArray,
          root, true, cardinalityOnly, true)
        val lrPairASmaller = computeLRbyBatchNums(mpBatchNums, mpIsSource.toArray,
          root, true, cardinalityOnly, true)
        val lrPairBLarger = computeLRbyBatchNums(newBatchNums, mpIsSource.toArray,
          root, true, cardinalityOnly, true)
        val lrPairBSmaller = computeLRbyBatchNums(newBatchNums, mpIsSource.toArray,
          root, true, cardinalityOnly, true)

        val incArray = new ArrayBuffer[Double]()
        incArray.append(computeInc(lrPairALarger, lrPairBLarger))
        incArray.append(computeInc(lrPairALarger, lrPairBSmaller))
        incArray.append(computeInc(lrPairASmaller, lrPairBLarger))
        incArray.append(computeInc(lrPairASmaller, lrPairBLarger))

        if (isOuterSide) {
          return Double.MinValue
        } else {
          return incArray.toArray.max
        }
      } else {
        // This is for aggregate
        val pairALarger = isSource
        val pairBLarger =
          if (isSource) pairALarger
          else false

        val lrPairA = computeLRbyBatchNums(mpBatchNums, mpIsSource.toArray,
          root, true, cardinalityOnly, pairALarger)
        val lrPairB = computeLRbyBatchNums(newBatchNums, mpIsSource.toArray,
          root, true, cardinalityOnly, pairBLarger)

        val newlrPairB =
          if (isSource && lrPairA._2 < lrPairB._2) {
            computeLRbyBatchNums(newBatchNums, mpIsSource.toArray,
              root, true, cardinalityOnly, false)
          } else lrPairB

        (newlrPairB._1 - lrPairA._1) / (lrPairA._2 - newlrPairB._2)
      }
    }
  }

  def computeMPIncrementabilityForLatencyConstraint(smallInc: Boolean): Double = {
    val limit =
      if (smallInc) MAX_INCREMENTABILITY
      else MIN_INCREMENTABILITY

    var curLimit = limit
    var cur: Double = 0
    val testOverhead = false

    for (i <- 0 until mpCount) {
      val newBatchNums = Array.fill(mpCount)(0)
      mpBatchNums.copyToArray(newBatchNums)
      newBatchNums(i) -= BATCHNUM_STEP

      if (newBatchNums(i) <= MIN_BATCHNUM - 1 || newBatchNums(i) < mpParentBatchNum) {
        cur = limit
      } else {
        val isOuterSide =
          if (antiOuterCase) mpIsOuter(i)
          else false
        cur = computeIncForLatencyHelper(newBatchNums, mpIsSource(i),
          isOuterSide, antiOuterCase, testOverhead)
        if (mpCount == 3) {
          val a = 0
        }
      }

      if (smallInc) {
        if (cur < curLimit) {
          curLimit = cur
          curMPIdx = i
        }
      } else {
        if (cur > curLimit) {
          curLimit = cur
          curMPIdx = i
        }
      }
    }

    curLimit
  }


  def computeMPIncrementabilityForOverhead(): Double = {

    val limit = MIN_INCREMENTABILITY
    var curLimit = limit
    var cur: Double = 0

    var lrPair: Tuple2[Double, Double] = null

    for (i <- 0 until mpCount) {
      val newBatchNums = Array.fill(mpCount)(0)
      mpBatchNums.copyToArray(newBatchNums)
      newBatchNums(i) += mpDeltaSteps(i)

      if (newBatchNums(i) >= MAX_BATCHNUM + 1) {
        cur = limit
      } else if (newBatchNums(i) > mpChildBatchNums(i)) {
        cur = limit
      } else {
        if (prevLatency == Double.MinValue) {
          val tmplrPair = computeLRbyBatchNumsSimple(mpBatchNums, mpIsSource.toArray,
            root, true, cardinalityOnly)
          prevLatency = tmplrPair._1
          prevResource = tmplrPair._2
        }

        lrPair = computeLRbyBatchNumsSimple(newBatchNums, mpIsSource.toArray,
          root, true, cardinalityOnly)

        cur = (lrPair._1 - prevLatency) / (prevResource - lrPair._2)
      }

      if (cur > curLimit) {
        curLimit = cur
        curMPIdx = i

        tmpPrevLatency = lrPair._1
        tmpPrevResource = lrPair._2
      }
    }

    curLimit
  }

  def applyLRPair(): Unit = {
    prevLatency = tmpPrevLatency
    prevResource = tmpPrevResource
  }

  def decreaseMPBatchNum(): Unit = {
    // if (mpCount == 3) {
    //   mpBatchNums.zipWithIndex.foreach(batchNumWithIndex => {
    //     printf(s"${batchNumWithIndex._2}: ${batchNumWithIndex._1}\t")
    //   })
    //   printf("\n")
    // }
    mpBatchNums(curMPIdx) -= mpDeltaSteps(curMPIdx)
  }

  def increaseMPBatchNum(): Unit = {
    mpBatchNums(curMPIdx) += mpDeltaSteps(curMPIdx)
  }

  def increaseDeltaStep(): Unit = {
    val exp_step = mpDeltaSteps(curMPIdx) * BATCHNUM_FACTOR
    val remain_step = MAX_BATCHNUM - mpBatchNums(curMPIdx)
    mpDeltaSteps(curMPIdx) =
      math.max(BATCHNUM_STEP,
        math.min(exp_step, remain_step))
  }

  def resetDeltaStep(): Unit = {
    mpDeltaSteps(curMPIdx) = BATCHNUM_STEP
  }

  // The following two are for random sampling and brute force
  def nextMPBatchNum(): Unit = {
    for (i <- 0 until mpCount) mpBatchNums(i) = rnd.nextInt(MAX_BATCHNUM) + 1
  }

  def maxBatchNum(): Boolean = {
    !mpBatchNums.exists(_ < MAX_BATCHNUM)
  }

  def putBatchNum(batchNums: Array[Int]): Unit = {
    for (i <- 0 until mpCount) {
      mpBatchNums(i) = batchNums(i)
    }
  }

  def getBatchNum(batchNums: Array[Int]): Unit = {
    for (i <- 0 until mpCount) {
      batchNums(i) = mpBatchNums(i)
    }
  }

  def findChildBatchNum(subPlanArray: Array[SlothSubPlan]): SlothSubPlan = {
    this.getChildDep().foreach(childIndex => {
      val childSubPlan = subPlanArray(childIndex)
      for (i <- 0 until this.mpCount) {
        if (!this.mpIsSource(i)) {
          val opIdx = this.mpOPIndex(i)
          if (this.blockingOperators(opIdx).id == childSubPlan.root.id) {
            this.mpChildBatchNums(i) = childSubPlan.mpBatchNums.max
          }
        }
      }
    })

   this
  }

  def findParentBatchNum(subPlanArray: Array[SlothSubPlan]): SlothSubPlan = {
    if (this.parentDep != -1) {
      val parentSubPlan = subPlanArray(this.parentDep)
      var parentBatchNum = -1
      parentSubPlan.mpBatchNums.zipWithIndex.foreach(pair => {
        val batchNum = pair._1
        val idx = pair._2
        if (!parentSubPlan.mpIsSource(idx)) {
          val blockingIdx = parentSubPlan.mpOPIndex(idx)
          val blockingOp = parentSubPlan.blockingOperators(blockingIdx)
          if (blockingOp.id == this.root.id) {
            parentBatchNum = batchNum
          }
        }
      })
      this.mpParentBatchNum = parentBatchNum
    }

    this
  }

  def validBatchNums(): Boolean = {
    for (i <- 0 until mpCount) {
      if (mpBatchNums(i) < mpParentBatchNum) return false
    }

    true
  }


  var antiOuterCase: Boolean = _
  var mpIsOuter: Array[Boolean] = _

  def findAntiOuterHelper(isOuterBuf: ArrayBuffer[Boolean],
                          op: OperatorCostModel,
                          parentOP: OperatorCostModel,
                          isLeft: Boolean): Unit = {
    // For finding outer join
    if (isJoinOperator(op)) {
      val joinOP = op.asInstanceOf[JoinCostModel]
      if (joinOP.joinType == LeftOuter ||
        joinOP.joinType == RightOuter ||
        joinOP.joinType == FullOuter ||
        joinOP.joinType == LeftAnti) {
        antiOuterCase = true
      }
    }

    if (isScanOperator(op)) {
      assert(op.mpIdx == isOuterBuf.length)
      if (isJoinOperator(parentOP)) {
        val joinOP = parentOP.asInstanceOf[JoinCostModel]
        if (
          (isLeft && (joinOP.joinType == LeftOuter ||
          joinOP.joinType == FullOuter ||
          joinOP.joinType == LeftAnti))
          ||
          (!isLeft && (joinOP.joinType == RightOuter ||
            joinOP.joinType == FullOuter))
        ) {
          isOuterBuf.append(true)
        } else {
          isOuterBuf.append(false)
        }
      } else {
        isOuterBuf.append(false)
      }
    } else if (isBlockingOperator(op)) {
      isOuterBuf.append(false)
    } else {
      if (isFilterOperator(op)) {
        findAntiOuterHelper(isOuterBuf, op.children(LEFT), parentOP, isLeft)
      } else {
        findAntiOuterHelper(isOuterBuf, op.children(LEFT), op, true)
        if (op.children.size > 1) {
          findAntiOuterHelper(isOuterBuf, op.children(RIGHT), op, false)
        }
      }
    }
  }

  def findAntiOuterCase(): Array[Boolean] = {
    val isOuterBuf = new ArrayBuffer[Boolean]()
    findAntiOuterHelper(isOuterBuf, root, null, true)
    return isOuterBuf.toArray
  }

  def initialize(root: OperatorCostModel, index: Int, parentDep: Int,
                 mpDecompose: Boolean, testOverhead: Boolean): Unit = {
    this.root = root
    this.index = index
    this.parentDep = parentDep
    this.mpDecompose = mpDecompose

    this.mpCount = 0

    findLeafNodes()

    // this.mpIsOuter = findAntiOuterCase()

    if (mpDecompose) {
      mpBatchNums = Array.fill(mpCount)(MAX_BATCHNUM)
      mpChildBatchNums = Array.fill(mpCount)(Int.MaxValue)
      mpDeltaSteps = Array.fill(mpCount)(BATCHNUM_STEP)
      mpBatchSteps = Array.fill(mpCount)(0)
      mpExecutable = Array.fill(mpCount)(false)
    }
  }

  def resetMPExecutable(): Unit = {
    for (i <- 0 until mpExecutable.length) {
      mpExecutable(i) = false
    }
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
    newSubPlan.index = index
    newSubPlan.parentDep = parentDep

    newSubPlan.childDeps = childDeps.clone()

    newSubPlan.mpNum = mpNum
    newSubPlan.mpDecompose = mpDecompose
    newSubPlan.mpCount = mpCount

    /* We comment this for efficiency */
    newSubPlan.sources = new Array[BaseStreamingSource](sources.length)
    for (i <- 0 until sources.length) {
      newSubPlan.sources(i) = sources(i)
    }

    newSubPlan.endOffsets = new Array[SlothOffset](endOffsets.length)
    for (i <- 0 until endOffsets.length) {
      newSubPlan.endOffsets(i) = endOffsets(i)
    }

    newSubPlan.blockingOperators = new Array[OperatorCostModel](blockingOperators.length)
    for (i <- 0 until blockingOperators.length) {
      newSubPlan.blockingOperators(i) = blockingOperators(i).copy()
    }

    newSubPlan.mpBatchNums = mpBatchNums.clone()
    newSubPlan.mpBatchSteps = mpBatchSteps.clone()
    newSubPlan.mpExecutable = mpExecutable.clone()
    newSubPlan.mpIsSource = mpIsSource.clone()
    newSubPlan.mpChildBatchNums = mpChildBatchNums.clone()
    newSubPlan.mpOPIndex = mpOPIndex.clone()

    newSubPlan.mpDeltaSteps = mpDeltaSteps.clone()
    newSubPlan.mpParentBatchNum = mpParentBatchNum

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


class SlothOffset(val name: String,
                  val partIndices: Array[Int],
                  val partOffsets: Array[Long]) {

  def getOffsetByIndex(batchNum: Int, batchIndex: Int): SlothOffset = {
    val newIndices = new ArrayBuffer[Int]()
    val newOffsets = new ArrayBuffer[Long]()
    partOffsets.zipWithIndex.foreach(offsetWithIndex => {
      val offset = offsetWithIndex._1
      val i = offsetWithIndex._2
      val newOffset =
        if (batchNum == batchIndex) offset
        else ((offset + batchNum - 1)/batchNum) * (batchIndex)
      newOffsets.append(newOffset)
      newIndices.append(partIndices(i))
    })

    new SlothOffset(name, newIndices.toArray, newOffsets.toArray)
  }
}

object SlothOffsetUtils {

  def jsonToOffset(json: String): SlothOffset = {
    val nameIndex = json.indexOf(":")
    val name = json.substring(2, nameIndex - 1)
    val offsetStr = json.substring(nameIndex + 2, json.length - 2)
    val offsets = parsePartOffset(offsetStr)

    new SlothOffset(name, offsets._1, offsets._2)
  }

  private def parsePartOffset(partOffsetStr: String): (Array[Int], Array[Long]) = {
    val partIndices = new ArrayBuffer[Int]()
    val partOffsets = new ArrayBuffer[Long]()

    val partOffsetArray = partOffsetStr.split(",")
    partOffsetArray.foreach(offsetStr => {
      val middleIndex = offsetStr.indexOf(":")
      val endIndex = offsetStr.length
      partIndices.append(offsetStr.substring(1, middleIndex - 1).toInt)
      partOffsets.append(offsetStr.substring(middleIndex + 1, endIndex).toLong)
    })

    (partIndices.toArray, partOffsets.toArray)
  }

  def offsetToJSON(offset: SlothOffset): String = {
    val json = new StringBuilder()
    json.append("{\"")
    json.append(offset.name)
    json.append("\":{")
    offset.partOffsets.zipWithIndex.foreach(offsetWithIndex => {
      val tmpOffset = offsetWithIndex._1
      val i = offsetWithIndex._2
      json.append("\"")
      json.append(offset.partIndices(i))
      json.append("\":")
      json.append(tmpOffset)
      if (i != offset.partIndices.length - 1) json.append(",")
    })
    json.append("}}")
    json.toString()
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

  // minimum batch size
  val MIN_STREAMING_SIZE = 50

  val MIN_RESOURCE_REDUCTION = 1

  // Cost functions
  // val TUPLECOST = 100L
  // val SCANCOST = 1L
  // val AGGSTATICCOST = 1000L
  // val JOINSTATICCOST = 200L

  val TUPLECOST = 1L
  val SCANCOST = 0L
  val AGGSTATICCOST = 10L
  val JOINSTATICCOST = 2L
  val SCANUNITCOST = 3L
  val OTHERUNITCOST = 1L

  val MIN_BATCHNUM = 1
  var MAX_BATCHNUM = 100
  val BATCHNUM_STEP = 1
  val BATCHNUM_FACTOR = 2
  val CHANGE_THRESHOLD = 0.01

  val MAX_INCREMENTABILITY = Double.MaxValue
  val MIN_INCREMENTABILITY = Double.MinValue
  val MAX_LATENCY = Double.MaxValue
  val MAX_RESOURCE = Double.MaxValue
  val MIN_LATENCY = Double.MinValue
  val MIN_RESOURCE = Double.MinValue

  val cardinalityOnly: Boolean = false

  val OPT_SAMPLE = 0
  val OPT_BRUTEFORCE = 1
  val OPT_GREEDY = 2
  val OPT_METHOD = OPT_BRUTEFORCE
  val NANOPERSECOND = 1000000000L

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

  def isFilterOperator(op: OperatorCostModel): Boolean = {
    op match {
      case _: SelectCostModel =>
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

  def isJoinOperator(op: OperatorCostModel): Boolean = {
    op match {
      case _: JoinCostModel =>
        true
      case _ =>
        false
    }
  }

  def computeResourceForSubPlans(subPlans: Array[SlothSubPlan],
                                 batchNums: Array[Int],
                                 subPlanAware: Boolean,
                                 cardinalityOnly: Boolean): Double = {
    subPlans.zipWithIndex.map(planWithIndex => {
      computeLatencyAndResource(batchNums(planWithIndex._2),
        planWithIndex._1.root,
        subPlanAware, cardinalityOnly)._2
    }).sum
  }

  def computeLatencyForSubPlans(subPlans: Array[SlothSubPlan],
                                batchNums: Array[Int],
                                subPlanAware: Boolean,
                                cardinalityOnly: Boolean): Double = {
    subPlans.zipWithIndex.map(planWithIndex => {
      computeLatencyAndResource(batchNums(planWithIndex._2),
        planWithIndex._1.root,
        subPlanAware, cardinalityOnly)._1
    }).sum
  }

  // The first item is latency and the second is resource
  def computeLatencyAndResource(batchNum: Int,
                                root: OperatorCostModel,
                                subPlanAware: Boolean,
                                cardinalityOnly: Boolean): (Double, Double) = {
    if (batchNum < MIN_BATCHNUM) {
      (MAX_LATENCY, MIN_RESOURCE)
    } else if (batchNum > MAX_BATCHNUM) {
      (MIN_LATENCY, MAX_RESOURCE)
    } else root.getLatencyAndResource(batchNum, subPlanAware, cardinalityOnly)
  }

  def computeLRbyBatchNums(batchNums: Array[Int],
                           mpIsSource: Array[Boolean],
                           root: OperatorCostModel,
                           subPlanAware: Boolean,
                           cardinalityOnly: Boolean,
                           larger: Boolean): (Double, Double) = {
    root.getLRByBatchNums(batchNums, mpIsSource, subPlanAware, cardinalityOnly, larger)
  }

  def computeLRbyBatchNumsSimple(batchNums: Array[Int],
                           mpIsSource: Array[Boolean],
                           root: OperatorCostModel,
                           subPlanAware: Boolean,
                           cardinalityOnly: Boolean): (Double, Double) = {
    root.getFastLRByBatchNums(batchNums, mpIsSource, subPlanAware, cardinalityOnly)
  }

  def computeLatencyForMPsSimple(subPlans: Array[SlothSubPlan],
                           subPlanAware: Boolean,
                           cardinalityOnly: Boolean): Double = {
    subPlans.map(plan => {
      computeLRbyBatchNumsSimple(plan.mpBatchNums, plan.mpIsSource.toArray,
        plan.root, subPlanAware, cardinalityOnly)._1
    }).sum
  }

  def computeResourceForMPsSimple(subPlans: Array[SlothSubPlan],
                           subPlanAware: Boolean,
                           cardinalityOnly: Boolean): Double = {
    subPlans.map(plan => {
      computeLRbyBatchNumsSimple(plan.mpBatchNums, plan.mpIsSource.toArray,
        plan.root, subPlanAware, cardinalityOnly)._2
    }).sum
  }

  def computeLatencyForMPs(subPlans: Array[SlothSubPlan],
                           subPlanAware: Boolean,
                           cardinalityOnly: Boolean): Double = {
    subPlans.map(plan => {
      computeLRbyBatchNums(plan.mpBatchNums, plan.mpIsSource.toArray,
        plan.root, subPlanAware, cardinalityOnly, false)._1
    }).sum
  }

  def computeResourceForMPs(subPlans: Array[SlothSubPlan],
                            subPlanAware: Boolean,
                            cardinalityOnly: Boolean): Double = {
    subPlans.map(plan => {
      computeLRbyBatchNums(plan.mpBatchNums, plan.mpIsSource.toArray,
        plan.root, subPlanAware, cardinalityOnly, false)._2
    }).sum
  }

}
