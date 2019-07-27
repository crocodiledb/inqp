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

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeReference, Expression, GenericInternalRow, JoinedRow, Literal, UnsafeProjection, UnsafeRow}
import org.apache.spark.sql.catalyst.plans._
import org.apache.spark.sql.catalyst.plans.logical.EventTimeWatermark._
import org.apache.spark.sql.catalyst.plans.physical._
import org.apache.spark.sql.execution.{BinaryExecNode, SlothMetricsTracker, SlothUtils, SparkPlan}
import org.apache.spark.sql.execution.metric.SQLMetrics
import org.apache.spark.sql.execution.streaming.StreamingSymmetricHashJoinHelper._
import org.apache.spark.sql.execution.streaming.state._
import org.apache.spark.sql.internal.SessionState
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.util.{CompletionIterator, SerializableConfiguration}


case class SlothSimpleHashJoinExec(
    leftKeys: Seq[Expression],
    rightKeys: Seq[Expression],
    joinType: JoinType,
    condition: JoinConditionSplitPredicates,
    stateInfo: Option[StatefulOperatorStateInfo],
    eventTimeWatermark: Option[Long],
    stateWatermarkPredicates: JoinStateWatermarkPredicates,
    keepLeft: Boolean,
    left: SparkPlan,
    right: SparkPlan) extends SparkPlan with BinaryExecNode with SlothMetricsTracker {

  def this(
      leftKeys: Seq[Expression],
      rightKeys: Seq[Expression],
      joinType: JoinType,
      condition: Option[Expression],
      keepLeft: Boolean,
      left: SparkPlan,
      right: SparkPlan) = {

    this(
      leftKeys, rightKeys, joinType, JoinConditionSplitPredicates(condition, left, right),
      stateInfo = None, eventTimeWatermark = None,
      stateWatermarkPredicates = JoinStateWatermarkPredicates(), keepLeft, left, right)
  }

  private def throwBadJoinTypeException(): Nothing = {
    throw new IllegalArgumentException(
      s"${getClass.getSimpleName} should not take $joinType as the JoinType")
  }

  require(
    joinType == Inner,
    s"${getClass.getSimpleName} should not take $joinType as the JoinType")
  require(leftKeys.map(_.dataType) == rightKeys.map(_.dataType))

  override lazy val metrics = Map(
    "numOutputRows" -> SQLMetrics.createMetric(sparkContext, "number of output rows"),
    "leftTimeMs" -> SQLMetrics.createTimingMetric(sparkContext, "left join time"),
    "rightTimeMs" -> SQLMetrics.createTimingMetric(sparkContext, "right join time"),
    "scanTimeMs" -> SQLMetrics.createTimingMetric(sparkContext, "scan time"),
    "commitTimeMs" -> SQLMetrics.createTimingMetric(sparkContext, "commit time"),
    "stateMemory" -> SQLMetrics.createSizeMetric(sparkContext, "peak memory")
  )

  private val storeConf = new StateStoreConf(sqlContext.conf)
  private val hadoopConfBcast = sparkContext.broadcast(
    new SerializableConfiguration(SessionState.newHadoopConf(
      sparkContext.hadoopConfiguration, sqlContext.conf)))

  override def requiredChildDistribution: Seq[Distribution] =
    HashClusteredDistribution(leftKeys, stateInfo.map(_.numPartitions)) ::
      HashClusteredDistribution(rightKeys, stateInfo.map(_.numPartitions)) :: Nil

  private def intermediateOutput: Seq[Attribute] =
    left.output.map(_.withNullability(true)) ++
      right.output.map(_.withNullability(true))

  override def output: Seq[Attribute] = joinType match {
    case _: InnerLike => left.output ++ right.output
    case _ => throwBadJoinTypeException()
  }

  override def outputPartitioning: Partitioning = joinType match {
    case _: InnerLike =>
      PartitioningCollection(Seq(left.outputPartitioning, right.outputPartitioning))
    case x =>
      throw new IllegalArgumentException(
        s"${getClass.getSimpleName} should not take $x as the JoinType")
  }

  private[this] var leftPropagateUpdate = true
  private[this] var rightPropagateUpdate = true
  private[this] var updateOutput = true

  override def setUpdateOutput(updateOutput: Boolean): Unit = {
    this.updateOutput = updateOutput
  }

  def setPropagateUpdate(parentProjOutput: Seq[Attribute]): Unit = {
    val leftKeyAttrs = leftKeys.map(expr => expr.asInstanceOf[Attribute])
    val rightKeyAttrs = rightKeys.map(expr => expr.asInstanceOf[Attribute])

    val leftNonKeyAttrs = SlothUtils.attrDiff(left.output, leftKeyAttrs)
    val rightNonKeyAttrs = SlothUtils.attrDiff(right.output, rightKeyAttrs)

    leftPropagateUpdate = SlothUtils.attrIntersect(leftNonKeyAttrs, parentProjOutput).nonEmpty
    rightPropagateUpdate = SlothUtils.attrIntersect(rightNonKeyAttrs, parentProjOutput).nonEmpty
  }

  private[this] var isFirstBatch = true

  def setIsFirstBatch(isFirstBatch: Boolean): Unit = {
    this.isFirstBatch = isFirstBatch
  }

  protected override def doExecute(): RDD[InternalRow] = {
    val stateStoreCoord = sqlContext.sessionState.streamingQueryManager.stateStoreCoordinator
    val stateStoreNames = SlothHashJoinStateManager.allStateStoreNames(LeftSide, RightSide)
    left.execute().stateStoreAwareZipPartitions(
      right.execute(), stateInfo.get, stateStoreNames, stateStoreCoord)(processPartitions)
  }

  private def processPartitions(
      leftInputIter: Iterator[InternalRow],
      rightInputIter: Iterator[InternalRow]): Iterator[InternalRow] = {
    if (stateInfo.isEmpty) {
      throw new IllegalStateException(s"Cannot execute join as state info was not specified\n$this")
    }

    val numOutputRows = longMetric("numOutputRows")
    val commitTimeMs = longMetric("commitTimeMs")
    val stateMemory = longMetric("stateMemory")

    val joinedRow1 = new JoinedRow
    val joinedRow2 = new JoinedRow

    val opRtId = new SlothRuntimeOpId(stateInfo.get.operatorId, stateInfo.get.queryRunId)
    val opRunTime = SlothRuntimeCache.get(opRtId)
    var hashRunTime: SlothHashJoinRuntime = null

    if (opRunTime == null) {
      val outputProj = UnsafeProjection.create(output, intermediateOutput)
      val postJoinFilter =
        newPredicate(condition.bothSides.
          getOrElse(Literal(true)), left.output ++ right.output).eval _

      val leftKeyProj = UnsafeProjection.create(leftKeys, left.output)
      val leftDeleteKeyProj = UnsafeProjection.create(leftKeys, left.output)
      val leftStateManager = if (keepLeft) {
        new SlothHashJoinStateManager(
          LeftSide, left.output, leftKeys, stateInfo, storeConf, hadoopConfBcast.value.value)
      } else null
      val leftRowGen = if (keepLeft) {
        UnsafeProjection.create(
          left.output, left.output :+ AttributeReference("counter", IntegerType)())
      } else null

      val rightKeyProj = UnsafeProjection.create(rightKeys, right.output)
      val rightDeleteKeyProj = UnsafeProjection.create(rightKeys, right.output)
      val rightStateManager = if (!keepLeft) {
        new SlothHashJoinStateManager(
          RightSide, right.output, rightKeys, stateInfo, storeConf, hadoopConfBcast.value.value)
      } else null
      val rightRowGen = if (!keepLeft) {
        UnsafeProjection.create(
          right.output, right.output :+ AttributeReference("counter", IntegerType)())
      } else null

      hashRunTime = new SlothHashJoinRuntime(
        outputProj, postJoinFilter, leftKeyProj, leftDeleteKeyProj, leftRowGen, leftStateManager,
        rightKeyProj, rightDeleteKeyProj, rightRowGen, rightStateManager)
    } else {
      hashRunTime = opRunTime.asInstanceOf[SlothHashJoinRuntime]

      if (keepLeft) {
        hashRunTime.leftStateManager.reInit(stateInfo, storeConf, hadoopConfBcast.value.value)
      } else {
        hashRunTime.rightStateManager.reInit(stateInfo, storeConf, hadoopConfBcast.value.value)
      }
    }

    val leftSideJoiner = new OneSideHashJoiner(
      LeftSide, left.output, leftKeys, leftInputIter, condition.leftSideOnly,
      hashRunTime.postFilterFunc, stateWatermarkPredicates.left, hashRunTime.leftKeyProj,
      hashRunTime.leftDeleteKeyProj, hashRunTime.leftStateManager, hashRunTime.leftRowGen)

    val rightSideJoiner = new OneSideHashJoiner(
      RightSide, right.output, rightKeys, rightInputIter, condition.rightSideOnly,
      hashRunTime.postFilterFunc, stateWatermarkPredicates.right, hashRunTime.rightKeyProj,
      hashRunTime.rightDeleteKeyProj, hashRunTime.rightStateManager, hashRunTime.rightRowGen)

    val leftInnerIter = if (keepLeft) {
      leftSideJoiner.populateState()
    } else {
      leftSideJoiner.storeAndJoinWithOtherSide(rightSideJoiner,
        (input: InternalRow, matched: InternalRow) => {
          joinedRow1.cleanStates()
          joinedRow1.withLeft(input).withRight(matched)
        },
        (input: InternalRow, matched: InternalRow) => {
          joinedRow2.cleanStates()
          joinedRow2.withLeft(input).withRight(matched)
        }
      )
    }

    val rightInnerIter = if (!keepLeft) {
      rightSideJoiner.populateState()
    } else {
      rightSideJoiner.storeAndJoinWithOtherSide(leftSideJoiner,
        (input: InternalRow, matched: InternalRow) => {
          joinedRow1.cleanStates()
          joinedRow1.withLeft(matched).withRight(input)
        },
        (input: InternalRow, matched: InternalRow) => {
          joinedRow2.cleanStates()
          joinedRow2.withLeft(matched).withRight(input)
        })
    }

    // TODO: remove stale state when a window is closed
    val outputIter: Iterator[InternalRow] = joinType match {
      case Inner =>
        if (keepLeft) rightInnerIter
        else leftInnerIter
      case _ => throwBadJoinTypeException()
    }

    val outputProjection = hashRunTime.outputProj
    val outputIterWithMetrics = outputIter.map { row =>
      numOutputRows += 1
      val projectedRow = outputProjection(row)
      projectedRow.setInsert(row.isInsert)
      projectedRow.setUpdate(row.isUpdate && updateOutput)
      projectedRow
    }

    // TODO: we do not consider removing old state now
    def onAllCompletion: Unit = {
      // Commit all state changes and update state store metrics
      commitTimeMs += timeTakenMs {
        if (keepLeft) {
          val leftSideMetrics = leftSideJoiner.commitStateAndGetMetrics()
          stateMemory += leftSideMetrics.memoryUsedBytes
          hashRunTime.leftStateManager.purgeState()
        } else {
          val rightSideMetrics = rightSideJoiner.commitStateAndGetMetrics()
          stateMemory += rightSideMetrics.memoryUsedBytes
          hashRunTime.rightStateManager.purgeState()
        }
      }

      SlothRuntimeCache.put(opRtId, hashRunTime)
    }

    CompletionIterator[InternalRow, Iterator[InternalRow]](
        outputIterWithMetrics, onAllCompletion)
  }

  private class OneSideHashJoiner(
      joinSide: JoinSide,
      inputAttributes: Seq[Attribute],
      joinKeys: Seq[Expression],
      inputIter: Iterator[InternalRow],
      preJoinFilterExpr: Option[Expression],
      postJoinFilter: InternalRow => Boolean,
      stateWatermarkPredicate: Option[JoinStateWatermarkPredicate],
      keyGenerator: UnsafeProjection,
      deleteKeyGenerator: UnsafeProjection,
      stateManager: SlothHashJoinStateManager,
      rowGenerator: UnsafeProjection) {

    // Filter the joined rows based on the given condition.
    val preJoinFilter =
      newPredicate(preJoinFilterExpr.getOrElse(Literal(true)), inputAttributes).eval _

    val joinStateManager = stateManager

    private[this] val stateKeyWatermarkPredicateFunc = stateWatermarkPredicate match {
      case Some(JoinStateKeyWatermarkPredicate(expr)) =>
        // inputSchema can be empty as expr should only have BoundReferences and does not require
        // the schema to generated predicate. See [[StreamingSymmetricHashJoinHelper]].
        newPredicate(expr, Seq.empty).eval _
      case _ =>
        newPredicate(Literal(false), Seq.empty).eval _ // false = do not remove if no predicate
    }

    // private[this] val stateValueWatermarkPredicateFunc = stateWatermarkPredicate match {
    //   case Some(JoinStateValueWatermarkPredicate(expr)) =>
    //     throw new IllegalArgumentException("We do not support watermarks on non-key columns")
    //   case _ =>
    //     newPredicate(Literal(false), Seq.empty).eval _  // false = do not remove if no predicate
    // }

    private[this] var updatedStateRowsCount = 0

    private[this] def generateUpdateIter(deleteIter: Iterator[JoinedRow],
      insertIter: Iterator[JoinedRow],
      postJoinFilter: (InternalRow) => Boolean): Iterator[JoinedRow] = {

      deleteIter.zip(insertIter).
        flatMap((rowPair) => {
          val deleteJoinedRow = rowPair._1
          val insertJoinedRow = rowPair._2
          deleteJoinedRow.cleanStates()
          insertJoinedRow.cleanStates()
          deleteJoinedRow.setInsert(false)
          insertJoinedRow.setInsert(true)

          val deletePass = postJoinFilter(deleteJoinedRow)
          val insertPass = postJoinFilter(insertJoinedRow)
          if (deletePass && !insertPass) {
            Seq(deleteJoinedRow)
          } else if (!deletePass && insertPass) {
            Seq(insertJoinedRow)

          } else if (deletePass && insertPass &&
            ((joinSide == LeftSide && leftPropagateUpdate) ||
              (joinSide == RightSide && rightPropagateUpdate))) {
            deleteJoinedRow.setUpdate(true)
            Seq(deleteJoinedRow, insertJoinedRow)
          } else {
            Iterator()
          }
        })
    }

    private val InnerPlusOuterJoinOneRow = (thisRow: UnsafeRow,
                   deleteRow: UnsafeRow,
                   isInsert: Boolean,
                   updateCase: Boolean,
                   otherSideJoiner: OneSideHashJoiner,
                   generateJoinedRow1: (InternalRow, InternalRow) => JoinedRow,
                   generateJoinedRow2: (InternalRow, InternalRow) => JoinedRow) =>
    {
      val key = keyGenerator(thisRow)

      // generate iterator over matched tuple pairs for inner join parts
      // Note that the iterator is executed after the states are updated
      // (i.e. insert to/delete from the hash table)
      var recentRow: UnsafeRow = null
      val innerIter = otherSideJoiner.joinStateManager.getAll(key).map{thatRowWithCounter => {
        val joinedRow =
          generateJoinedRow1(thisRow, otherSideJoiner.getRawValue(thatRowWithCounter))
        joinedRow.setInsert(isInsert)
        joinedRow
      }}
      var outerIter: Iterator[JoinedRow] = Iterator()

      if (updateCase) {
        val insertKey = key
        val insertIter = innerIter

        val deleteKey = deleteKeyGenerator(deleteRow)
        val deleteIter = otherSideJoiner.joinStateManager.getAll(deleteKey).
          map{thatRowWithCounter => {
            val joinedRow =
              generateJoinedRow2(deleteRow, otherSideJoiner.getRawValue(thatRowWithCounter))
            joinedRow.setInsert(false)
            otherSideJoiner.decCounter(thatRowWithCounter)
            joinedRow
        }}
        // Updates on keys, concatenate the two lists together
        if (!insertKey.equals(deleteKey)) {
          // Include the inner join parts
          outerIter = outerIter ++ (deleteIter ++ insertIter)
          outerIter = outerIter.filter(postJoinFilter)

        } else {
          // Updates on non-key columns, counters unchanged
          outerIter = outerIter ++ generateUpdateIter(deleteIter, insertIter, postJoinFilter)
        }

      } else {
        if (isInsert) {
          outerIter = outerIter ++ innerIter.filter(postJoinFilter)
        } else {
          outerIter = outerIter ++ innerIter.filter(postJoinFilter)
        }
      }

      outerIter
    }: Iterator[JoinedRow]


    def generateJoinOneRow(): (UnsafeRow, UnsafeRow, Boolean, Boolean, OneSideHashJoiner,
      (InternalRow, InternalRow) => JoinedRow, (InternalRow, InternalRow) => JoinedRow)
      => Iterator[JoinedRow] = {
      joinType match {
        case Inner =>
          InnerPlusOuterJoinOneRow
        case _ => throwBadJoinTypeException()
      }
    }

    private val joinOneRow = generateJoinOneRow()

    /**
     * Generate joined rows by consuming input from this side, and matching it with the buffered
     * rows (i.e. state) of the other side.
     * @param otherSideJoiner   Joiner of the other side
     * @param generateJoinedRow1 Function to generate the joined row from the
     *                          input row from this side and the matched row from the other side
     * @param generateJoinedRow2 Function to generate the joined row from the
     *                          input row from this side and the matched row from the other side
     */
    def storeAndJoinWithOtherSide(
        otherSideJoiner: OneSideHashJoiner,
        generateJoinedRow1: (InternalRow, InternalRow) => JoinedRow,
        generateJoinedRow2: (InternalRow, InternalRow) => JoinedRow):
    Iterator[InternalRow] = {
      val watermarkAttribute = inputAttributes.find(_.metadata.contains(delayKey))
      val nonLateRows =
        WatermarkSupport.watermarkExpression(watermarkAttribute, eventTimeWatermark) match {
          case Some(watermarkExpr) =>
            val predicate = newPredicate(watermarkExpr, inputAttributes)
            inputIter.filter { row => !predicate.eval(row) }
          case None =>
            inputIter
        }

      var updateCase = false
      var deleteRow: UnsafeRow = null

      nonLateRows.flatMap { row =>
        val thisRow = row.asInstanceOf[UnsafeRow]
        // If this row fails the pre join filter, that means it can never satisfy the full join
        // condition no matter what other side row it's matched with. This allows us to avoid
        // adding it to the state, and generate an outer join row immediately (or do nothing in
        // the case of inner join).

        val isInsert = thisRow.isInsert
        val isUpdate = thisRow.isUpdate

        if (preJoinFilter(thisRow)) {
          thisRow.cleanStates()

          // In update case
          if (updateCase) {
            assert(isInsert,
              s"On ${joinSide} with ${rightKeys} of ${condition}: " +
                s"in the update case, the current row must be an insert")
            updateCase = false
            joinOneRow(thisRow, deleteRow, isInsert, true, otherSideJoiner,
              generateJoinedRow1, generateJoinedRow2)
          } else {
            // A delete row that indicates a insert follows; begin an update case
            if (!isInsert && isUpdate) {
              updateCase = true
              if (deleteRow == null) deleteRow = thisRow.copy()
              else deleteRow.copyFrom(thisRow)
              Iterator()
            } else {
              joinOneRow(thisRow, deleteRow, isInsert, false, otherSideJoiner,
                generateJoinedRow1, generateJoinedRow2)
            }
          }
        } else {
          // When an update is modelled as a delete and insert,
          // and the delete passes the filter, but the insert does not,
          // so we need to process the delete as a single delete
          if (updateCase) {
            assert(isInsert, "In the update case, the current row must be an insert")
            updateCase = false
            joinOneRow(deleteRow, deleteRow, false, false, otherSideJoiner,
              generateJoinedRow1, generateJoinedRow2)
          } else Iterator()
          // joinSide match {
          // case LeftSide if joinType == LeftOuter =>
          //   Iterator(generateJoinedRow(thisRow, nullRight))
          // case RightSide if joinType == RightOuter =>
          //   Iterator(generateJoinedRow(thisRow, nullLeft))
          // case _ => Iterator()
          //         }
        }
      }
    }

    def populateState(): Iterator[InternalRow] = {
      if (isFirstBatch) {
        inputIter.foreach(row => {
          val thisRow = row.asInstanceOf[UnsafeRow]
          thisRow.cleanStates()
          val key = keyGenerator(thisRow)
          joinStateManager.append(key, thisRow)
        })
      }
      Iterator.empty
    }

    /**
     * Get an iterator over the values stored in this joiner's state manager for the given key.
     *
     * Should not be interleaved with mutations.
     */
    def get(key: UnsafeRow): Iterator[UnsafeRow] = {
      joinStateManager.getAll(key)
    }

    /**
     * Builds an iterator to remove old state and return all non-removed states
     *
     * @note This iterator must be consumed fully before any other operations are made
     * against this joiner's join state manager. For efficiency reasons, the intermediate states of
     * the iterator leave the state manager in an undefined state.
     *
     */
    def removeOldState(): Iterator[KeyWithIndexAndValueWithCounter] = {
      stateWatermarkPredicate match {
        case Some(JoinStateKeyWatermarkPredicate(expr)) =>
          joinStateManager.scanAndremoveByKeyCondition(stateKeyWatermarkPredicateFunc, false)
        // case Some(JoinStateValueWatermarkPredicate(expr)) =>
        //   joinStateManager.removeByValueCondition(stateValueWatermarkPredicateFunc)
        case _ => Iterator.empty
      }
    }

    def scanAndRemoveOldState(): Iterator[KeyWithIndexAndValueWithCounter] = {
      joinStateManager.scanAndremoveByKeyCondition(stateKeyWatermarkPredicateFunc, true)
    }

    /** Commit changes to the buffer state and return the state store metrics */
    def commitStateAndGetMetrics(): StateStoreMetrics = {
      joinStateManager.commit()
      joinStateManager.metrics
    }

    def numUpdatedStateRows: Long = updatedStateRowsCount

    // Fields and functions for counters

    private val counterIndex = inputAttributes.size
    private val valueRowGenerator = rowGenerator

    def getCounter(value: UnsafeRow): Int = {
      value.getInt(counterIndex)
    }

    def incCounter(value: UnsafeRow): Unit = {
      val newCounter = value.getInt(counterIndex) + 1
      value.setInt(counterIndex, newCounter)
    }

    def decCounter(value: UnsafeRow): Unit = {
      val newCounter = value.getInt(counterIndex) - 1
      value.setInt(counterIndex, newCounter)
    }

    def getRawValue(valueWithCounter: UnsafeRow): UnsafeRow = {
      valueRowGenerator(valueWithCounter)
    }

  }
}
