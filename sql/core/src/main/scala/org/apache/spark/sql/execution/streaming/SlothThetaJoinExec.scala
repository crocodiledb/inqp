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

import java.io.{BufferedReader, FileReader}

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SlothDBCostModel._
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Attribute, Expression, JoinedRow, Literal, UnsafeProjection, UnsafeRow}
import org.apache.spark.sql.catalyst.plans._
import org.apache.spark.sql.catalyst.plans.logical.EventTimeWatermark._
import org.apache.spark.sql.catalyst.plans.physical._
import org.apache.spark.sql.execution.{BinaryExecNode, SlothMetricsTracker, SlothUtils, SparkPlan}
import org.apache.spark.sql.execution.metric.SQLMetrics
import org.apache.spark.sql.execution.streaming.StreamingSymmetricHashJoinHelper._
import org.apache.spark.sql.execution.streaming.state._
import org.apache.spark.sql.internal.{SessionState, SQLConf}
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.util.{CompletionIterator, SerializableConfiguration}

/** Assuming we broadcast the right sub-tree
 *
 * @param leftKeys If not empty, they indicate the window ID
 * @param rightKeys If not empty, they indicate the window ID
 * @param joinType Must be Cross (i.e. ThetaJoin)
 * @param condition
 * @param stateInfo
 * @param eventTimeWatermark
 * @param stateWatermarkPredicates
 * @param left
 * @param right
 */
case class SlothThetaJoinExec (
    leftKeys: Seq[Expression],
    rightKeys: Seq[Expression],
    joinType: JoinType,
    condition: JoinConditionSplitPredicates,
    stateInfo: Option[StatefulOperatorStateInfo],
    eventTimeWatermark: Option[Long],
    stateWatermarkPredicates: JoinStateWatermarkPredicates,
    left: SparkPlan,
    right: SparkPlan) extends SparkPlan with BinaryExecNode with SlothMetricsTracker {

  def this(
      leftKeys: Seq[Expression],
      rightKeys: Seq[Expression],
      joinType: JoinType,
      condition: Option[Expression],
      left: SparkPlan,
      right: SparkPlan) = {

    this(
      leftKeys, rightKeys, joinType, JoinConditionSplitPredicates(condition, left, right),
      stateInfo = None, eventTimeWatermark = None,
      stateWatermarkPredicates = JoinStateWatermarkPredicates(), left, right)
  }

  private def throwBadJoinTypeException(): Nothing = {
    throw new IllegalArgumentException(
      s"${getClass.getSimpleName} should not take $joinType as the JoinType")
  }

  val leftOutput = left.output

  require(
    joinType == Cross,
    s"${getClass.getSimpleName} should not take $joinType as the JoinType in ThetaJoin")
  require(leftKeys.map(_.dataType) == rightKeys.map(_.dataType))

  override lazy val metrics = Map(
    "numOutputRows" -> SQLMetrics.createMetric(sparkContext, "number of output rows"),
    "deleteRows" -> SQLMetrics.createMetric(sparkContext, "number of deletes output"),
    "updateRows" -> SQLMetrics.createMetric(sparkContext, "number of updates output"),
    "commitTimeMs" -> SQLMetrics.createTimingMetric(sparkContext, "commit time"),
    "stateMemory" -> SQLMetrics.createSizeMetric(sparkContext, "peak memory"),
    "left_insert_to_insert" -> SQLMetrics
      .createAverageMetric(sparkContext, "insert to insert match"),
    "left_delete_to_delete" -> SQLMetrics
      .createAverageMetric(sparkContext, "delete to delete match"),
    "left_update_to_update" -> SQLMetrics
      .createAverageMetric(sparkContext, "update to update match"),
    "right_insert_to_insert" -> SQLMetrics
      .createAverageMetric(sparkContext, "insert to insert match"),
    "right_delete_to_delete" -> SQLMetrics
      .createAverageMetric(sparkContext, "delete to delete match"),
    "right_update_to_update" -> SQLMetrics
      .createAverageMetric(sparkContext, "update to update match")
  )

  private val storeConf = new StateStoreConf(sqlContext.conf)
  private val hadoopConfBcast = sparkContext.broadcast(
    new SerializableConfiguration(SessionState.newHadoopConf(
      sparkContext.hadoopConfiguration, sqlContext.conf)))

  private val enableIOLAP =
    sqlContext.sparkSession.conf.get(SQLConf.SLOTHDB_IOLAP).getOrElse(false)
  private val query_name =
    sqlContext.sparkSession.conf.get(SQLConf.SLOTHDB_QUERYNAME).get
  private val statRoot =
    sqlContext.sparkSession.conf.get(SQLConf.SLOTHDB_STAT_DIR).get

  override def requiredChildDistribution: Seq[Distribution] = {
    UnspecifiedDistribution :: new SlothBroadcastDistribution() :: Nil
  }

  override def output: Seq[Attribute] = joinType match {
    case Cross => left.output ++ right.output
    case _ => throwBadJoinTypeException()
  }

  override def outputPartitioning: Partitioning = joinType match {
    case Cross =>
      PartitioningCollection(Seq(left.outputPartitioning))
    case x =>
      throw new IllegalArgumentException(
        s"${getClass.getSimpleName} should not take $x as the JoinType")
  }

  private[this] var leftPropagateUpdate = true
  private[this] var rightPropagateUpdate = true

  def setPropagateUpdate(parentProjOutput: Seq[Attribute]): Unit = {
    val leftKeyAttrs = leftKeys.map(expr => expr.asInstanceOf[Attribute])
    val rightKeyAttrs = rightKeys.map(expr => expr.asInstanceOf[Attribute])

    val leftNonKeyAttrs = SlothUtils.attrDiff(left.output, leftKeyAttrs)
    val rightNonKeyAttrs = SlothUtils.attrDiff(right.output, rightKeyAttrs)

    leftPropagateUpdate = SlothUtils.attrIntersect(leftNonKeyAttrs, parentProjOutput).nonEmpty
    rightPropagateUpdate = SlothUtils.attrIntersect(rightNonKeyAttrs, parentProjOutput).nonEmpty
  }

  protected override def doExecute(): RDD[InternalRow] = {
    val stateStoreCoord = sqlContext.sessionState.streamingQueryManager.stateStoreCoordinator
    val stateStoreNames = SlothThetaJoinStateManager.allStateStoreNames(LeftSide, RightSide)
    left.execute().stateStoreAwareZipPartitions(
      right.execute(), stateInfo.get, stateStoreNames, stateStoreCoord)(processPartitions)
  }

  private def processPartitions(
      leftInputIter: Iterator[InternalRow],
      rightInputIter: Iterator[InternalRow]): Iterator[InternalRow] = {
    if (stateInfo.isEmpty) {
      throw new IllegalStateException(s"Cannot execute join as state info was not specified\n$this")
    }

    iOLAPFilter = getIOLAPFilter()

    val numOutputRows = longMetric("numOutputRows")
    val deleteRows = longMetric("deleteRows")
    val updateRows = longMetric("updateRows")
    val commitTimeMs = longMetric("commitTimeMs")
    val stateMemory = longMetric("stateMemory")
    val left_insert_to_insert = longMetric("left_insert_to_insert")
    val left_delete_to_delete = longMetric("left_delete_to_delete")
    val left_update_to_update = longMetric("left_update_to_update")
    val right_insert_to_insert = longMetric("right_insert_to_insert")
    val right_delete_to_delete = longMetric("right_delete_to_delete")
    val right_update_to_update = longMetric("right_update_to_update")

    val joinedRow1 = new JoinedRow
    val joinedRow2 = new JoinedRow

    val opRtId = new SlothRuntimeOpId(stateInfo.get.operatorId, stateInfo.get.queryRunId)
    val opRunTime = SlothRuntimeCache.get(opRtId)
    var thetaRunTime: SlothThetaJoinRuntime = null

    if (opRunTime == null) {
      val outputProj = UnsafeProjection.create(left.output ++ right.output, output)
      val postJoinFilter =
        newPredicate(condition.bothSides.getOrElse(Literal(true)),
          left.output ++ right.output).eval _
      val leftStateManager = new SlothThetaJoinStateManager(
        LeftSide, left.output, stateInfo, storeConf, hadoopConfBcast.value.value)
      val rightStateManager = new SlothThetaJoinStateManager(
        RightSide, right.output, stateInfo, storeConf, hadoopConfBcast.value.value)

      thetaRunTime = new SlothThetaJoinRuntime(
        outputProj, postJoinFilter, leftStateManager, rightStateManager)
    } else {
      thetaRunTime = opRunTime.asInstanceOf[SlothThetaJoinRuntime]

      thetaRunTime.leftStateManager.reInit(stateInfo, storeConf, hadoopConfBcast.value.value)
      thetaRunTime.rightStateManager.reInit(stateInfo, storeConf, hadoopConfBcast.value.value)
    }

    val leftSideJoiner = new OneSideThetaJoiner(
      LeftSide, left.output, leftKeys, leftInputIter, condition.leftSideOnly,
      thetaRunTime.postFilterFunc, stateWatermarkPredicates.left, thetaRunTime.leftStateManager)
    val rightSideJoiner = new OneSideThetaJoiner(
      RightSide, right.output, rightKeys, rightInputIter, condition.rightSideOnly,
      thetaRunTime.postFilterFunc, stateWatermarkPredicates.right, thetaRunTime.rightStateManager)

    //  Join one side input using the other side's buffered/state rows. Here is how it is done.
    //
    //  - `leftJoiner.joinWith(rightJoiner)` generates all rows from matching new left input with
    //    stored right input, and also stores all the left input
    //
    //  - `rightJoiner.joinWith(leftJoiner)` generates all rows from matching new right input with
    //    stored left input, and also stores all the right input. It also generates all rows from
    //    matching new left input with new right input, since the new left input has become stored
    //    by that point. This tiny asymmetry is necessary to avoid duplication.
    val leftOutputIter = leftSideJoiner.storeAndJoinWithOtherSide(rightSideJoiner,
      (input: InternalRow, matched: InternalRow) => {
        joinedRow1.cleanStates()
        joinedRow1.withLeft(input).withRight(matched)
      },
      (input: InternalRow, matched: InternalRow) => {
        joinedRow2.cleanStates()
        joinedRow2.withLeft(input).withRight(matched)
      }
    )

    val rightOutputIter = rightSideJoiner.storeAndJoinWithOtherSide(leftSideJoiner,
      (input: InternalRow, matched: InternalRow) => {
        joinedRow1.cleanStates()
        joinedRow1.withLeft(matched).withRight(input)
      },
      (input: InternalRow, matched: InternalRow) => {
        joinedRow2.cleanStates()
        joinedRow2.withLeft(matched).withRight(input)
      })

    def onLeftCompletion: Unit = {
      left_insert_to_insert.set(leftSideJoiner.getInsertProb(rightSideJoiner))
      left_delete_to_delete.set(leftSideJoiner.getDeleteProb(rightSideJoiner))
      left_update_to_update.set(leftSideJoiner.getUpdateProb(rightSideJoiner))
    }

    def onRightCompletion: Unit = {
      right_insert_to_insert.set(rightSideJoiner.getInsertProb(leftSideJoiner))
      right_delete_to_delete.set(rightSideJoiner.getDeleteProb(leftSideJoiner))
      right_update_to_update.set(rightSideJoiner.getUpdateProb(leftSideJoiner))
    }

    val outputIter = CompletionIterator[InternalRow, Iterator[InternalRow]](
          leftOutputIter, onLeftCompletion) ++
      CompletionIterator[InternalRow, Iterator[InternalRow]](
        rightOutputIter, onRightCompletion)

    val outputProjection = thetaRunTime.outputProj
    val outputIterWithMetrics = outputIter.map { row =>
      numOutputRows += 1
      if (row.isUpdate) updateRows += 2
      else if (!row.isInsert) deleteRows += 1
      val projectedRow = outputProjection(row)
      projectedRow.setInsert(row.isInsert)
      projectedRow.setUpdate(row.isUpdate)
      projectedRow
    }

    // TODO: we do not consider removing old state now
    def onAllCompletion: Unit = {
      // Commit all state changes and update state store metrics
      commitTimeMs += timeTakenMs {
        val leftSideMetrics = leftSideJoiner.commitStateAndGetMetrics()
        val rightSideMetrics = rightSideJoiner.commitStateAndGetMetrics()
        val combinedMetrics = StateStoreMetrics.combine(Seq(leftSideMetrics, rightSideMetrics))
        stateMemory += combinedMetrics.memoryUsedBytes
      }

      thetaRunTime.leftStateManager.purgeState()
      thetaRunTime.rightStateManager.purgeState()
      SlothRuntimeCache.put(opRtId, thetaRunTime)
    }

    CompletionIterator[InternalRow, Iterator[InternalRow]](
      outputIterWithMetrics, onAllCompletion)
  }

  private var iOLAPFilter: (UnsafeRow => Boolean) = _
  private var minValue: Double = _
  private var maxValue: Double = _
  val Q11_KEY = 1
  val Q11_OP_ID = 0
  val Q22_KEY = 1
  val Q22_OP_ID = 1

  private def getIOLAPFilter(): (UnsafeRow => Boolean) = {

    if (enableIOLAP) {
      query_name match {
        case "q11" if Q11_OP_ID == stateInfo.get.operatorId =>
          loadIOLAPDoubleTable(statRoot + "/iOLAP/q11_config.csv")
          (row: UnsafeRow) => {
            val key = row.getDouble(Q11_KEY)
            if (key <= maxValue && key >= minValue) true
            else false
          }
        case "q22" if Q22_OP_ID == stateInfo.get.operatorId =>
          loadIOLAPDoubleTable(statRoot + "/iOLAP/q22_config.csv")
          (row: UnsafeRow) => {
            val key = row.getDouble(Q22_KEY)
            if (key >= minValue) true
            else false
          }
        case _ =>
          (row: UnsafeRow) => {true}
      }

    } else {
      (row: UnsafeRow) => {true}
    }

  }

  private def loadIOLAPDoubleTable(path: String): Unit = {
    val reader = new BufferedReader(new FileReader((path)))
    minValue = reader.readLine().toDouble
    maxValue = reader.readLine().toDouble
    reader.close()
  }

  /**
   * Internal helper class to consume input rows, generate join output rows using other sides
   * buffered state rows, and finally clean up this sides buffered state rows
   *
   * @param joinSide The JoinSide - either left or right.
   * @param inputAttributes The input attributes for this side of the join.
   * @param joinKeys The join keys.
   * @param inputIter The iterator of input rows on this side to be joined.
   * @param preJoinFilterExpr A filter over rows on this side. This filter rejects rows that could
   *                          never pass the overall join condition no matter what other side row
   *                          they're joined with.
   * @param postJoinFilter A filter over joined rows. This filter completes the application of
   *                       the overall join condition, assuming that preJoinFilter on both sides
   *                       of the join has already been passed.
   *                       Passed as a function rather than expression to avoid creating the
   *                       predicate twice; we also need this filter later on in the parent exec.
   * @param stateWatermarkPredicate The state watermark predicate. See
   *                                [[StreamingSymmetricHashJoinExec]] for further description of
   *                                state watermarks.
   */
  private class OneSideThetaJoiner(
      joinSide: JoinSide,
      inputAttributes: Seq[Attribute],
      joinKeys: Seq[Expression],
      inputIter: Iterator[InternalRow],
      preJoinFilterExpr: Option[Expression],
      postJoinFilter: InternalRow => Boolean,
      stateWatermarkPredicate: Option[JoinStateWatermarkPredicate],
      stateManager: SlothThetaJoinStateManager) {

    // Filter the joined rows based on the given condition.
    val preJoinFilter: InternalRow => Boolean =
      newPredicate(preJoinFilterExpr.getOrElse(Literal(true)), inputAttributes).eval _

    private val joinStateManager = stateManager

    private[this] var insertOutput = 0L
    private[this] var deleteOutput = 0L
    private[this] var updateOutput = 0L
    private[this] var insertInput = 0L
    private[this] var deleteInput = 0L
    private[this] var updateInput = 0L

    private[this] val thisWIDGenerator = UnsafeProjection.create(joinKeys, inputAttributes)
    private[this] var thisWID: UnsafeRow = _
    private[this] val thisDeleteWIDGenerator = UnsafeProjection.create(joinKeys, inputAttributes)
    private[this] var thisDeleteWID: UnsafeRow = _
    private[this] val thatWIDGenerator = UnsafeProjection.create(joinKeys, inputAttributes)

    private[this] val stateKeyWatermarkPredicateFunc = stateWatermarkPredicate match {
      case Some(JoinStateKeyWatermarkPredicate(expr)) =>
        // inputSchema can be empty as expr should only have BoundReferences and does not require
        // the schema to generated predicate. See [[StreamingSymmetricHashJoinHelper]].
        newPredicate(expr, Seq.empty).eval _
      case _ =>
        newPredicate(Literal(false), Seq.empty).eval _ // false = do not remove if no predicate
    }

    private[this] val keyRow = genEmptyKeyRow()
    private[this] val deleteKeyRow = genEmptyKeyRow()

    private[this] val keyGenerator = (row: UnsafeRow) => {
      keyRow.setInt(0, row.hashCode())
      keyRow}: UnsafeRow

    private[this] val deleteKeyGenerator = (row: UnsafeRow) => {
      deleteKeyRow.setInt(0, row.hashCode())
      deleteKeyRow}: UnsafeRow

    private[this] val removeCondition = (row: UnsafeRow) => {
      if (joinKeys.isEmpty) false
      else {
        val thatWID = thatWIDGenerator(row)
        stateKeyWatermarkPredicateFunc(thatWID)
      }
    }: Boolean

    private[this] val internalWindowCondition = (row: UnsafeRow) => {
      val thatWID = thatWIDGenerator(row)
      thisWID.equals(thatWID)
    }: Boolean

    private[this] val internalDeleteWindowCondition = (row: UnsafeRow) => {
      val thatWID = thatWIDGenerator(row)
      thisDeleteWID.equals(thatWID)
    }: Boolean

    private[this] val noWindowCondition = (_: UnsafeRow) => {true}

    private[this] def getWindowCondition(thisRow: UnsafeRow):
      (UnsafeRow) => Boolean = {
      if (joinKeys.isEmpty) return noWindowCondition
      else {
        thisWID = thisWIDGenerator(thisRow)
        return internalWindowCondition
      }
    }

    private[this] def getDeleteWindowCondition(thisRow: UnsafeRow):
      (UnsafeRow) => Boolean = {
      if (joinKeys.isEmpty) return noWindowCondition
      else {
        thisDeleteWID = thisDeleteWIDGenerator(thisRow)
        return internalDeleteWindowCondition
      }
    }

    private[this] var updatedStateRowsCount = 0

    private def genEmptyKeyRow(): UnsafeRow = {
      val numFields = 1
      val rowSize = UnsafeRow.calculateBitSetWidthInBytes(numFields) + IntegerType.defaultSize
      UnsafeRow.createFromByteArray(rowSize, numFields)
    }

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

    private val thetaJoinOneRow = (thisRow: UnsafeRow,
                   deleteRow: UnsafeRow,
                   isInsert: Boolean,
                   updateCase: Boolean,
                   otherSideJoiner: OneSideThetaJoiner,
                   generateJoinedRow1: (InternalRow, InternalRow) => JoinedRow,
                   generateJoinedRow2: (InternalRow, InternalRow) => JoinedRow) =>
    {
      val key = keyGenerator(thisRow)
      val windowCondition = getWindowCondition(thisRow)
      var outputIter = otherSideJoiner.joinStateManager.
        getAllAndRemove(windowCondition, removeCondition).map(thatRow => {
        if (isInsert && !updateCase) insertOutput += 1
        else if (!isInsert) deleteOutput += 1
        val joinedRow = generateJoinedRow1(thisRow, thatRow)
        joinedRow.setInsert(isInsert)
        joinedRow
      })

      if (updateCase) {
        val insertIter = outputIter
        val insertRow = thisRow
        val insertKey = key

        val deleteKey = deleteKeyGenerator(deleteRow)
        val deleteWindowCondition = getDeleteWindowCondition(deleteRow)
        val deleteIter = otherSideJoiner.joinStateManager.
          getAllAndRemove(deleteWindowCondition, removeCondition).
          map(thatRow => {
            val joinedRow = generateJoinedRow2(deleteRow, thatRow)
            joinedRow
          })

        outputIter = generateUpdateIter(deleteIter, insertIter, postJoinFilter)
            .map(row => {
              updateOutput += 1
              row})

        if ((joinSide == LeftSide && iOLAPFilter(deleteRow) ||
             joinSide == RightSide)) {
          updatedStateRowsCount += 1
          joinStateManager.remove(deleteKey, deleteRow)
        }

        if ((joinSide == LeftSide && iOLAPFilter(insertRow) ||
             joinSide == RightSide)) {
          updatedStateRowsCount += 1
          joinStateManager.append(insertKey, insertRow)
        }

      } else {
        outputIter = outputIter.filter(postJoinFilter)

        if ((joinSide == LeftSide && iOLAPFilter(thisRow) ||
             joinSide == RightSide)) {
          updatedStateRowsCount += 1
          if (isInsert) joinStateManager.append(key, thisRow)
          else joinStateManager.remove(key, thisRow)
        }
      }

      outputIter
    }: Iterator[JoinedRow]

    def generateJoinOneRow(): (UnsafeRow, UnsafeRow, Boolean, Boolean, OneSideThetaJoiner,
      (InternalRow, InternalRow) => JoinedRow, (InternalRow, InternalRow) => JoinedRow)
      => Iterator[JoinedRow] = {
      joinType match {
        case Cross =>
          thetaJoinOneRow
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
        otherSideJoiner: OneSideThetaJoiner,
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
              s"On ${joinSide}: in the update case, the current row must be an insert")
            updateCase = false
            updateInput += 1
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
              if (isInsert) insertInput += 1
              else deleteInput += 1
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

        }
      }
    }

    /** Commit changes to the buffer state and return the state store metrics */
    def commitStateAndGetMetrics(): StateStoreMetrics = {
      joinStateManager.commit()
      joinStateManager.metrics
    }

    def numUpdatedStateRows: Long = updatedStateRowsCount

    def getInsertProb(otherSideJoiner: OneSideThetaJoiner): Long = {
      if (insertOutput == 0 || insertInput == 0 ||
        otherSideJoiner.joinStateManager.getStateSize == 0) {
        return 0
      } else {
        return (insertOutput * SF)/(insertInput * otherSideJoiner.joinStateManager.getStateSize)
      }
    }

    def getDeleteProb(otherSideJoiner: OneSideThetaJoiner): Long = {
      if (deleteOutput == 0 || deleteInput == 0 ||
        otherSideJoiner.joinStateManager.getStateSize == 0) {
        return 0
      } else {
        return (deleteOutput * SF)/(deleteInput * otherSideJoiner.joinStateManager.getStateSize)
      }
    }

    def getUpdateProb(otherSideJoiner: OneSideThetaJoiner): Long = {
      if (updateOutput == 0 || updateInput == 0 ||
        otherSideJoiner.joinStateManager.getStateSize == 0) {
        return 0
      } else {
        return (updateOutput * SF)/(2 * updateInput * otherSideJoiner.joinStateManager.getStateSize)
      }
    }

  }
}

case class SlothThetaJoinRuntime (
  outputProj: UnsafeProjection,
  postFilterFunc: InternalRow => Boolean,
  leftStateManager: SlothThetaJoinStateManager,
  rightStateManager: SlothThetaJoinStateManager) extends SlothRuntime {}
