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

import java.util.concurrent.TimeUnit.NANOSECONDS

import scala.collection.mutable.ListBuffer

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Attribute, Expression, GenericInternalRow, JoinedRow, Literal, UnsafeProjection, UnsafeRow}
import org.apache.spark.sql.catalyst.plans._
import org.apache.spark.sql.catalyst.plans.logical.EventTimeWatermark._
import org.apache.spark.sql.catalyst.plans.physical._
import org.apache.spark.sql.execution.{BinaryExecNode, SparkPlan}
import org.apache.spark.sql.execution.streaming.StreamingSymmetricHashJoinHelper._
import org.apache.spark.sql.execution.streaming.state._
import org.apache.spark.sql.internal.SessionState
import org.apache.spark.util.{CompletionIterator, SerializableConfiguration}

/**
 * Performs stream-stream join using symmetric hash join algorithm. It works as follows.
 *
 *                             /-----------------------\
 *   left side input --------->|    left side state    |------\
 *                             \-----------------------/      |
 *                                                            |--------> joined output
 *                             /-----------------------\      |
 *   right side input -------->|    right side state   |------/
 *                             \-----------------------/
 *
 * Each join side buffers past input rows as streaming state so that the past input can be joined
 * with future input on the other side. This buffer state is effectively a multi-map:
 *    equi-join key -> list of past input rows received with the join key
 *
 * For each input row in each side, the following operations take place.
 * - Calculate join key from the row.
 * - Use the join key to append the row to the buffer state of the side that the row came from.
 * - Find past buffered values for the key from the other side. For each such value, emit the
 *   "joined row" (left-row, right-row)
 * - Apply the optional condition to filter the joined rows as the final output.
 *
 * If a timestamp column with event time watermark is present in the join keys or in the input
 * data, then the it uses the watermark figure out which rows in the buffer will not join with
 * and the new data, and therefore can be discarded. Depending on the provided query conditions, we
 * can define thresholds on both state key (i.e. joining keys) and state value (i.e. input rows).
 * There are three kinds of queries possible regarding this as explained below.
 * Assume that watermark has been defined on both `leftTime` and `rightTime` columns used below.
 *
 * 1. When timestamp/time-window + watermark is in the join keys. Example (pseudo-SQL):
 *
 *      SELECT * FROM leftTable, rightTable
 *      ON
 *        leftKey = rightKey AND
 *        window(leftTime, "1 hour") = window(rightTime, "1 hour")    // 1hr tumbling windows
 *
 *    In this case, this operator will join rows newer than watermark which fall in the same
 *    1 hour window. Say the event-time watermark is "12:34" (both left and right input).
 *    Then input rows can only have time > 12:34. Hence, they can only join with buffered rows
 *    where window >= 12:00 - 1:00 and all buffered rows with join window < 12:00 can be
 *    discarded. In other words, the operator will discard all state where
 *    window in state key (i.e. join key) < event time watermark. This threshold is called
 *    State Key Watermark.
 *
 * 2. When timestamp range conditions are provided (no time/window + watermark in join keys). E.g.
 *
 *      SELECT * FROM leftTable, rightTable
 *      ON
 *        leftKey = rightKey AND
 *        leftTime > rightTime - INTERVAL 8 MINUTES AND leftTime < rightTime + INTERVAL 1 HOUR
 *
 *   In this case, the event-time watermark and the BETWEEN condition can be used to calculate a
 *   state watermark, i.e., time threshold for the state rows that can be discarded.
 *   For example, say each join side has a time column, named "leftTime" and
 *   "rightTime", and there is a join condition "leftTime > rightTime - 8 min".
 *   While processing, say the watermark on right input is "12:34". This means that from henceforth,
 *   only right inputs rows with "rightTime > 12:34" will be processed, and any older rows will be
 *   considered as "too late" and therefore dropped. Then, the left side buffer only needs
 *   to keep rows where "leftTime > rightTime - 8 min > 12:34 - 8m > 12:26".
 *   That is, the left state watermark is 12:26, and any rows older than that can be dropped from
 *   the state. In other words, the operator will discard all state where
 *   timestamp in state value (input rows) < state watermark. This threshold is called
 *   State Value Watermark (to distinguish from the state key watermark).
 *
 *   Note:
 *   - The event watermark value of one side is used to calculate the
 *     state watermark of the other side. That is, a condition ~ "leftTime > rightTime + X" with
 *     right side event watermark is used to calculate the left side state watermark. Conversely,
 *     a condition ~ "left < rightTime + Y" with left side event watermark is used to calculate
 *     right side state watermark.
 *   - Depending on the conditions, the state watermark maybe different for the left and right
 *     side. In the above example, leftTime > 12:26 AND rightTime > 12:34 - 1 hour = 11:34.
 *   - State can be dropped from BOTH sides only when there are conditions of the above forms that
 *     define time bounds on timestamp in both directions.
 *
 * 3. When both window in join key and time range conditions are present, case 1 + 2.
 *    In this case, since window equality is a stricter condition than the time range, we can
 *    use the the State Key Watermark = event time watermark to discard state (similar to case 1).
 *
 * @param leftKeys  Expression to generate key rows for joining from left input
 * @param rightKeys Expression to generate key rows for joining from right input
 * @param joinType  Type of join (inner, left outer, etc.)
 * @param condition Conditions to filter rows, split by left, right, and joined. See
 *                  [[JoinConditionSplitPredicates]]
 * @param stateInfo Version information required to read join state (buffered rows)
 * @param eventTimeWatermark Watermark of input event, same for both sides
 * @param stateWatermarkPredicates Predicates for removal of state, see
 *                                 [[JoinStateWatermarkPredicates]]
 * @param left      Left child plan
 * @param right     Right child plan
 */
case class SlothSymmetricHashJoinExec(
    leftKeys: Seq[Expression],
    rightKeys: Seq[Expression],
    joinType: JoinType,
    condition: JoinConditionSplitPredicates,
    stateInfo: Option[StatefulOperatorStateInfo],
    eventTimeWatermark: Option[Long],
    stateWatermarkPredicates: JoinStateWatermarkPredicates,
    left: SparkPlan,
    right: SparkPlan) extends SparkPlan with BinaryExecNode with StateStoreWriter {

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

  require(
    joinType == Inner ||
    joinType == LeftOuter || joinType == RightOuter || joinType == FullOuter ||
    joinType == LeftSemi || joinType == LeftAnti,
    s"${getClass.getSimpleName} should not take $joinType as the JoinType")
  require(leftKeys.map(_.dataType) == rightKeys.map(_.dataType))

  private val storeConf = new StateStoreConf(sqlContext.conf)
  private val hadoopConfBcast = sparkContext.broadcast(
    new SerializableConfiguration(SessionState.newHadoopConf(
      sparkContext.hadoopConfiguration, sqlContext.conf)))

  val nullLeft = new GenericInternalRow(left.output.map(_.withNullability(true)).length)
  val nullRight = new GenericInternalRow(right.output.map(_.withNullability(true)).length)

  override def requiredChildDistribution: Seq[Distribution] =
    HashClusteredDistribution(leftKeys, stateInfo.map(_.numPartitions)) ::
      HashClusteredDistribution(rightKeys, stateInfo.map(_.numPartitions)) :: Nil

  private def intermediateOutput: Seq[Attribute] =
    left.output.map(_.withNullability(true)) ++
      right.output.map(_.withNullability(true))

  override def output: Seq[Attribute] = joinType match {
    case _: InnerLike => left.output ++ right.output
    case LeftOuter => left.output ++ right.output.map(_.withNullability(true))
    case RightOuter => left.output.map(_.withNullability(true)) ++ right.output
    case FullOuter => left.output.map(_.withNullability(true)) ++
      right.output.map(_.withNullability(true))
    case LeftSemi => left.output
    case LeftAnti => left.output
    case _ => throwBadJoinTypeException()
  }

  override def outputPartitioning: Partitioning = joinType match {
    case _: InnerLike =>
      PartitioningCollection(Seq(left.outputPartitioning, right.outputPartitioning))
    case LeftOuter => PartitioningCollection(Seq(left.outputPartitioning))
    case RightOuter => PartitioningCollection(Seq(right.outputPartitioning))
    case FullOuter => PartitioningCollection(Seq(left.outputPartitioning, right.outputPartitioning))
    case LeftSemi => PartitioningCollection(Seq(left.outputPartitioning))
    case LeftAnti => PartitioningCollection(Seq(left.outputPartitioning))
    case x =>
      throw new IllegalArgumentException(
        s"${getClass.getSimpleName} should not take $x as the JoinType")
  }

  override def shouldRunAnotherBatch(newMetadata: OffsetSeqMetadata): Boolean = {
    val watermarkUsedForStateCleanup =
      stateWatermarkPredicates.left.nonEmpty || stateWatermarkPredicates.right.nonEmpty

    // Latest watermark value is more than that used in this previous executed plan
    val watermarkHasChanged =
      eventTimeWatermark.isDefined && newMetadata.batchWatermarkMs > eventTimeWatermark.get

    watermarkUsedForStateCleanup && watermarkHasChanged
  }

  private[this] var leftPropagateUpdate = true
  private[this] var rightPropagateUpdate = true

  private[this] def attrExist(attr: Attribute, attrSet: Seq[Attribute]): Boolean = {
    attrSet.exists(thisAttr => thisAttr.semanticEquals(attr))
  }

  private[this] def attrDiff(attrSetA: Seq[Attribute], attrSetB: Seq[Attribute]):
  Seq[Attribute] = {
    val retSet = new ListBuffer[Attribute]
    attrSetA.foreach(attrA => {
      if (!attrExist(attrA, attrSetB)) retSet += attrA
    })

    retSet
  }

  private[this] def attrIntersect(attrSetA: Seq[Attribute], attrSetB: Seq[Attribute]):
  Seq[Attribute] = {
    val retSet = new ListBuffer[Attribute]
    attrSetA.foreach(attrA => {
      if (attrExist(attrA, attrSetB)) retSet += attrA
    })

    retSet
  }

  def setPropagateUpdate(parentProjOutput: Seq[Attribute]): Unit = {
    val leftKeyAttrs = leftKeys.map(expr => expr.asInstanceOf[Attribute])
    val rightKeyAttrs = rightKeys.map(expr => expr.asInstanceOf[Attribute])

    val leftNonKeyAttrs = attrDiff(left.output, leftKeyAttrs)
    val rightNonKeyAttrs = attrDiff(right.output, rightKeyAttrs)

    leftPropagateUpdate = attrIntersect(leftNonKeyAttrs, parentProjOutput).nonEmpty
    rightPropagateUpdate = attrIntersect(rightNonKeyAttrs, parentProjOutput).nonEmpty
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
    val numUpdatedStateRows = longMetric("numUpdatedStateRows")
    val numTotalStateRows = longMetric("numTotalStateRows")
    val allUpdatesTimeMs = longMetric("allUpdatesTimeMs")
    val allRemovalsTimeMs = longMetric("allRemovalsTimeMs")
    val commitTimeMs = longMetric("commitTimeMs")
    val stateMemory = longMetric("stateMemory")

    val updateStartTimeNs = System.nanoTime
    val joinedRow1 = new JoinedRow
    val joinedRow2 = new JoinedRow

    val postJoinFilter =
      newPredicate(condition.bothSides.getOrElse(Literal(true)), left.output ++ right.output).eval _
    val leftSideJoiner = new OneSideHashJoiner(
      LeftSide, left.output, leftKeys, leftInputIter,
      condition.leftSideOnly, postJoinFilter, stateWatermarkPredicates.left)
    val rightSideJoiner = new OneSideHashJoiner(
      RightSide, right.output, rightKeys, rightInputIter,
      condition.rightSideOnly, postJoinFilter, stateWatermarkPredicates.right)

    //  Join one side input using the other side's buffered/state rows. Here is how it is done.
    //
    //  - `leftJoiner.joinWith(rightJoiner)` generates all rows from matching new left input with
    //    stored right input, and also stores all the left input
    //
    //  - `rightJoiner.joinWith(leftJoiner)` generates all rows from matching new right input with
    //    stored left input, and also stores all the right input. It also generates all rows from
    //    matching new left input with new right input, since the new left input has become stored
    //    by that point. This tiny asymmetry is necessary to avoid duplication.
    val leftInnerIter = leftSideJoiner.storeAndJoinWithOtherSide(rightSideJoiner,
      (input: InternalRow, matched: InternalRow) => {
        joinedRow1.cleanStates()
        joinedRow1.withLeft(input).withRight(matched)
      },
      (input: InternalRow, matched: InternalRow) => {
        joinedRow2.cleanStates()
        joinedRow2.withLeft(input).withRight(matched)
      }
    )

    val rightInnerIter = rightSideJoiner.storeAndJoinWithOtherSide(leftSideJoiner,
      (input: InternalRow, matched: InternalRow) => {
        joinedRow1.cleanStates()
        joinedRow1.withLeft(matched).withRight(input)
      },
      (input: InternalRow, matched: InternalRow) => {
        joinedRow2.cleanStates()
        joinedRow2.withLeft(matched).withRight(input)
      })

    // We need to save the time that the inner join output iterator completes, since outer join
    // output counts as both update and removal time.
    var innerOutputCompletionTimeNs: Long = 0
    def onInnerOutputCompletion: Unit = {
      innerOutputCompletionTimeNs = System.nanoTime
    }
    // This is the iterator which produces the inner join rows. For outer joins, this will be
    // prepended to a second iterator producing outer join rows; for inner joins, this is the full
    // output.
    // val innerOutputIter = CompletionIterator[InternalRow, Iterator[InternalRow]](
    //   leftOutputIter ++ rightOutputIter, onInnerOutputCompletion)


    // TODO: remove stale state when a window is closed
    val outputIter: Iterator[InternalRow] = joinType match {
      case Inner | LeftSemi =>
        leftInnerIter ++ rightInnerIter
      case LeftOuter | LeftAnti =>
        // Run right iterator first
        rightInnerIter ++ leftInnerIter
      case RightOuter =>
        // Run left iterator first
        leftInnerIter ++ rightInnerIter
      case FullOuter =>
        // Run left iterator first, but does not output tuples with nulls
        // When right iterator finished, scan left state to output tuples with nulls
        val leftStateIter = leftSideJoiner.scanAndRemoveOldState()
        val leftOuterIter = leftStateIter
          .filter(kvRow => kvRow.valueWithCounter.counter == 0)
          .map(kvRow => {
            kvRow.valueWithCounter.decCounter
            joinedRow1.cleanStates()
            joinedRow1.setInsert(true)
            joinedRow1.withLeft(kvRow.valueWithCounter.value).withRight(nullRight)
          })
        leftInnerIter ++ rightInnerIter ++ leftOuterIter
      case _ => throwBadJoinTypeException()
    }

    val outputProjection = UnsafeProjection.create(output, intermediateOutput)
    val outputIterWithMetrics = outputIter.map { row =>
      numOutputRows += 1
      val projectedRow = outputProjection(row)
      projectedRow.setInsert(row.isInsert)
      projectedRow.setUpdate(row.isUpdate)
      projectedRow
    }

    // Function to remove old state after all the input has been consumed and output generated
    def onOutputCompletion = {
      // All processing time counts as update time.
      allUpdatesTimeMs += math.max(NANOSECONDS.toMillis(System.nanoTime - updateStartTimeNs), 0)

      // Processing time between inner output completion and here comes from the outer portion of a
      // join, and thus counts as removal time as we remove old state from one side while iterating.
      if (innerOutputCompletionTimeNs != 0) {
        allRemovalsTimeMs +=
          math.max(NANOSECONDS.toMillis(System.nanoTime - innerOutputCompletionTimeNs), 0)
      }

      allRemovalsTimeMs += timeTakenMs {
        // Remove any remaining state rows which aren't needed because they're below the watermark.
        //
        // For inner joins, we have to remove unnecessary state rows from both sides if possible.
        // For outer joins, we have already removed unnecessary state rows from the outer side
        // (e.g., left side for left outer join) while generating the outer "null" outputs. Now, we
        // have to remove unnecessary state rows from the other side (e.g., right side for the left
        // outer join) if possible. In all cases, nothing needs to be outputted, hence the removal
        // needs to be done greedily by immediately consuming the returned iterator.
        // TODO: we uncomment this for now
        // val cleanupIter = joinType match {
        //   case Inner | LeftSemi =>
        //     leftSideJoiner.removeOldState() ++ rightSideJoiner.removeOldState()
        //   case LeftOuter | RightOuter | FullOuter | LeftAnti => Iterator.empty
        //   case _ => throwBadJoinTypeException()
        // }
        // while (cleanupIter.hasNext) {
        //   cleanupIter.next()
        // }
      }

      // Commit all state changes and update state store metrics
      commitTimeMs += timeTakenMs {
        val leftSideMetrics = leftSideJoiner.commitStateAndGetMetrics()
        val rightSideMetrics = rightSideJoiner.commitStateAndGetMetrics()
        val combinedMetrics = StateStoreMetrics.combine(Seq(leftSideMetrics, rightSideMetrics))

        // Update SQL metrics
        numUpdatedStateRows +=
          (leftSideJoiner.numUpdatedStateRows + rightSideJoiner.numUpdatedStateRows)
        numTotalStateRows += combinedMetrics.numKeys
        stateMemory += combinedMetrics.memoryUsedBytes
        combinedMetrics.customMetrics.foreach { case (metric, value) =>
          longMetric(metric.name) += value
        }
      }
    }

    CompletionIterator[InternalRow, Iterator[InternalRow]](
      outputIterWithMetrics, onOutputCompletion)
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
  private class OneSideHashJoiner(
      joinSide: JoinSide,
      inputAttributes: Seq[Attribute],
      joinKeys: Seq[Expression],
      inputIter: Iterator[InternalRow],
      preJoinFilterExpr: Option[Expression],
      postJoinFilter: InternalRow => Boolean,
      stateWatermarkPredicate: Option[JoinStateWatermarkPredicate]) {

    // Filter the joined rows based on the given condition.
    val preJoinFilter =
      newPredicate(preJoinFilterExpr.getOrElse(Literal(true)), inputAttributes).eval _

    private val joinStateManager = new SlothHashJoinStateManager(
      joinSide, inputAttributes, joinKeys, stateInfo, storeConf, hadoopConfBcast.value.value)
    private[this] val keyGenerator = UnsafeProjection.create(joinKeys, inputAttributes)
    private[this] val deleteKeyGenerator = UnsafeProjection.create(joinKeys, inputAttributes)

    private[this] val stateKeyWatermarkPredicateFunc = stateWatermarkPredicate match {
      case Some(JoinStateKeyWatermarkPredicate(expr)) =>
        // inputSchema can be empty as expr should only have BoundReferences and does not require
        // the schema to generated predicate. See [[StreamingSymmetricHashJoinHelper]].
        newPredicate(expr, Seq.empty).eval _
      case _ =>
        newPredicate(Literal(false), Seq.empty).eval _ // false = do not remove if no predicate
    }

    private[this] val stateValueWatermarkPredicateFunc = stateWatermarkPredicate match {
      case Some(JoinStateValueWatermarkPredicate(expr)) =>
        throw new IllegalArgumentException("We do not support watermarks on non-key columns")
      case _ =>
        newPredicate(Literal(false), Seq.empty).eval _  // false = do not remove if no predicate
    }

    private[this] var updatedStateRowsCount = 0

    private[this] val nullDeleteJoinedRow = new JoinedRow()
    private[this] val nullInsertJoinedRow = new JoinedRow()

    var deleteThisNullRow: (InternalRow) => JoinedRow = null
    var deleteThatNullRow: (InternalRow) => JoinedRow = null
    var insertThisNullRow: (InternalRow) => JoinedRow = null
    var insertThatNullRow: (InternalRow) => JoinedRow = null
    if (joinSide == LeftSide) {
      deleteThisNullRow = (row: InternalRow) =>
        nullDeleteJoinedRow.withLeft(nullLeft).withRight(row)
      insertThisNullRow = (row: InternalRow) =>
        nullInsertJoinedRow.withLeft(nullLeft).withRight(row)
      deleteThatNullRow = (row: InternalRow) =>
        nullDeleteJoinedRow.withLeft(row).withRight(nullRight)
      insertThatNullRow = (row: InternalRow) =>
        nullInsertJoinedRow.withLeft(row).withRight(nullRight)
    } else {
      deleteThisNullRow = (row: InternalRow) =>
        nullDeleteJoinedRow.withRight(nullRight).withLeft(row)
      insertThisNullRow = (row: InternalRow) =>
        nullInsertJoinedRow.withRight(nullRight).withLeft(row)
      deleteThatNullRow = (row: InternalRow) =>
        nullDeleteJoinedRow.withRight(row).withLeft(nullLeft)
      insertThatNullRow = (row: InternalRow) =>
        nullInsertJoinedRow.withRight(row).withLeft(nullLeft)
    }

    private[this] def deleteGenNullIter(otherSideJoiner: OneSideHashJoiner,
                                        deleteKey: UnsafeRow):
    Iterator[JoinedRow] = {

      // Note we do not generate nullrows for FullOuter for the left side
      // We will scan the left state later
      if ((joinSide == LeftSide
        && (joinType == RightOuter))
        ||
        (joinSide == RightSide
          && (joinType == LeftOuter || joinType == FullOuter || joinType == LeftAnti))) {

        otherSideJoiner.joinStateManager.getAll(deleteKey)
          .filter(_.counter == 0)
          .map(valueWithCounter => {
            valueWithCounter.decCounter
            val joinedRow = deleteThisNullRow(valueWithCounter.value)
            joinedRow.cleanStates()
            joinedRow.setInsert(true)
            joinedRow
          })
      } else Iterator()
    }

    private[this] def insertGenNullIter(insertKey: UnsafeRow):
    Iterator[JoinedRow] = {

      // Note we do not generate nullrows for FullOuter for the left side
      // We will scan the left state later
      if ((joinSide == LeftSide
        && (joinType == LeftOuter || joinType == LeftAnti))
        ||
        (joinSide == RightSide
          && (joinType == RightOuter || joinType == FullOuter))) {

        Seq(joinStateManager.getMostRecent(insertKey)).
          toIterator.filter(_.counter == 0).map(valueWithCounter => {
          valueWithCounter.decCounter
          val joinedRow = insertThatNullRow(valueWithCounter.value)
          joinedRow.cleanStates()
          joinedRow.setInsert(true)
          joinedRow
        })

      } else Iterator()
    }

    private[this] def deleteNegateNullIter(deleteRow: UnsafeRow):
    Iterator[JoinedRow] = {

      if (((joinType == LeftOuter || joinType == LeftAnti || joinType == FullOuter)
        && joinSide == LeftSide)
        || ((joinType == RightOuter || joinType == FullOuter)
        && joinSide == RightSide)) {

        Seq(deleteRow).toIterator.map(deleteRow => {
            val joinedRow = deleteThatNullRow(deleteRow)
            joinedRow.cleanStates()
            joinedRow.setInsert(false)
            joinedRow
          })
      } else {
        Iterator()
      }
    }

    private[this] def insertNegateNullIter(otherSideJoiner: OneSideHashJoiner,
                                           insertKey: UnsafeRow):
    Iterator[JoinedRow] = {
      if (((joinType == LeftOuter || joinType == FullOuter || joinType == LeftAnti)
        && joinSide == RightSide)
        || ((joinType == RightOuter || joinType == FullOuter)
        && joinSide == LeftSide)) {

        otherSideJoiner.joinStateManager.getAll(insertKey)
          .filter(valueWithCounter => valueWithCounter.counter == -1)
          .map(valueWithCounter => {
            valueWithCounter.incCounter
            val joinedRow = insertThisNullRow(valueWithCounter.value)
            joinedRow.cleanStates()
            joinedRow.setInsert(false)
            joinedRow
          })
      } else {
        Iterator()
      }
    }

    private[this] def genOuterJoinUpdateIter(key: UnsafeRow,
                                           insertRow: UnsafeRow,
                                           deleteRow: UnsafeRow): Iterator[JoinedRow] = {
      if (((joinType == LeftOuter || joinType == FullOuter || joinType == LeftAnti)
        && joinSide == LeftSide)
        || ((joinType == RightOuter || joinType == FullOuter)
        && joinSide == RightSide)) {

        Seq(deleteRow).toIterator.zip(Seq(insertRow).toIterator).flatMap(pair => {
          val tmpDeleteRow = pair._1
          val tmpInsertRow = pair._2
          val deleteJoinedRow = deleteThatNullRow(tmpDeleteRow)
          val insertJoinedRow = insertThatNullRow(tmpInsertRow)
          deleteJoinedRow.cleanStates()
          insertJoinedRow.cleanStates()
          deleteJoinedRow.setInsert(false)
          deleteJoinedRow.setUpdate(true)
          insertJoinedRow.setInsert(true)
          Seq(deleteJoinedRow, insertJoinedRow)
        })
      } else {
        Iterator()
      }
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
      var recentRow: ValueWithCounter = null
      val innerIter = otherSideJoiner.joinStateManager.getAll(key).map{thatRowWithCounter => {
         val joinedRow = generateJoinedRow1(thisRow, thatRowWithCounter.value)
         joinedRow.setInsert(isInsert)
         if (isInsert) {
           if (recentRow == null) recentRow = joinStateManager.getMostRecent(key)
           recentRow.incCounter
           thatRowWithCounter.incCounter
         } else thatRowWithCounter.decCounter
        joinedRow
      }}
      var outerIter: Iterator[JoinedRow] = Iterator()

      if (updateCase) {
        val insertKey = key
        val insertIter = innerIter
        val insertRow = thisRow

        val deleteKey = deleteKeyGenerator(deleteRow)
        val deleteIter = otherSideJoiner.joinStateManager.getAll(deleteKey).
          map{thatRowWithCounter => {
            val joinedRow = generateJoinedRow2(deleteRow, thatRowWithCounter.value)
            joinedRow.setInsert(false)
            thatRowWithCounter.decCounter
            joinedRow
        }}
        // Updates on keys, concatenate the two lists together
        if (!insertKey.equals(deleteKey)) {

          // Generate negations
          val delValueWithCounter = joinStateManager.get(deleteKey, deleteRow)
          if (delValueWithCounter.counter == -1) {
            outerIter = outerIter ++ deleteNegateNullIter(deleteRow)
          }
          outerIter = outerIter ++ insertNegateNullIter(otherSideJoiner, insertKey)

          // Include the inner join parts
          outerIter = outerIter ++ (deleteIter ++ insertIter)

          // Generate rows with nulls
          outerIter = outerIter ++ deleteGenNullIter(otherSideJoiner, deleteKey)
          outerIter = outerIter ++ insertGenNullIter(insertKey)
          outerIter = outerIter.filter(postJoinFilter)

        } else { // Updates on non-key columns, counters unchanged
          val valueWithCounter = joinStateManager.get(deleteKey, deleteRow)
          if (valueWithCounter.counter == -1) {
            outerIter = outerIter ++ genOuterJoinUpdateIter(insertKey, insertRow, deleteRow)
          }
          outerIter = outerIter ++ generateUpdateIter(deleteIter, insertIter, postJoinFilter)
        }

        updatedStateRowsCount += 2
        joinStateManager.remove(deleteKey, deleteRow)
        joinStateManager.append(insertKey, insertRow)

      } else {
        if (isInsert) {
          outerIter = insertNegateNullIter(otherSideJoiner, key)
          outerIter = outerIter ++ innerIter.filter(postJoinFilter)
          outerIter = outerIter ++ insertGenNullIter(key)

          joinStateManager.append(key, thisRow)
        } else {
          val valueWithCounter = joinStateManager.get(key, thisRow)
          if (valueWithCounter.counter == -1) {
            outerIter = deleteNegateNullIter(thisRow)
          }

          outerIter = outerIter ++ innerIter.filter(postJoinFilter)
          outerIter = outerIter ++ deleteGenNullIter(otherSideJoiner, key)

          joinStateManager.remove(key, thisRow)
        }

        updatedStateRowsCount += 1
      }

      outerIter
    }: Iterator[JoinedRow]

    private def isLastMatch (otherSideJoiner: OneSideHashJoiner,
                             key: UnsafeRow, thisRow: UnsafeRow,
                             generateRow: (InternalRow, InternalRow) => JoinedRow): Boolean = {
      otherSideJoiner.joinStateManager.getAll(key).exists(valueWithCounter => {
        val joinedRow = generateRow(thisRow, valueWithCounter.value)
        postJoinFilter(joinedRow) && valueWithCounter.counter == 1})
    }

    private def isFirstMatch (otherSideJoiner: OneSideHashJoiner,
                              key: UnsafeRow, thisRow: UnsafeRow,
                              generateRow: (InternalRow, InternalRow) => JoinedRow): Boolean = {
      otherSideJoiner.joinStateManager.getAll(key).exists(valueWithCounter => {
        val joinedRow = generateRow(thisRow, valueWithCounter.value)
        postJoinFilter(joinedRow) && valueWithCounter.counter == 0})
    }

    private def hasMatch (otherSideJoiner: OneSideHashJoiner,
                          key: UnsafeRow, thisRow: UnsafeRow,
                          generateRow: (InternalRow, InternalRow) => JoinedRow): Boolean = {
      otherSideJoiner.joinStateManager.getAll(key).exists(valueWithCounter => {
        val joinedRow = generateRow(thisRow, valueWithCounter.value)
        postJoinFilter(joinedRow)
      })
    }

    private def insertAndUpdateCounter (otherSideJoiner: OneSideHashJoiner,
                                    insertKey: UnsafeRow, insertRow: UnsafeRow,
                                    generateRow: (InternalRow, InternalRow) => JoinedRow  ):
    Unit = {
      joinStateManager.append(insertKey, insertRow)
      val insertedRow = joinStateManager.getMostRecent(insertKey)
      otherSideJoiner.joinStateManager.getAll(insertKey).foreach(valueWithCounter => {
        val joinedRow = generateRow(insertedRow.value, valueWithCounter.value)
        if (postJoinFilter(joinedRow)) {
          insertedRow.incCounter
          valueWithCounter.incCounter
        }
      })
    }

    private def deleteAndUpdateCounter (otherSideJoiner: OneSideHashJoiner,
                                        deleteKey: UnsafeRow, deleteRow: UnsafeRow,
                                        generateRow: (InternalRow, InternalRow) => JoinedRow):
    Unit = {
      joinStateManager.remove(deleteKey, deleteRow)
      otherSideJoiner.joinStateManager.getAll(deleteKey).foreach(valueWithCounter => {
        val joinedRow = generateRow(deleteRow, valueWithCounter.value)
        if (postJoinFilter(joinedRow)) valueWithCounter.decCounter
      })
    }

    private val complexJoinedRow = new JoinedRow

    private val leftGenerateRow = (thisRow: InternalRow, thatRow: InternalRow) => {
      complexJoinedRow.withLeft(thisRow).withRight(thatRow)
    }: JoinedRow

    private val rightGenerateRow = (thisRow: InternalRow, thatRow: InternalRow) => {
      complexJoinedRow.withRight(thisRow).withLeft(thatRow)
    }: JoinedRow

    private val LeftSemiJoinOneRow = (thisRow: UnsafeRow,
               deleteRow: UnsafeRow,
               isInsert: Boolean,
               updateCase: Boolean,
               otherSideJoiner: OneSideHashJoiner,
               generateJoinedRow1: (InternalRow, InternalRow) => JoinedRow,
               generateJoinedRow2: (InternalRow, InternalRow) => JoinedRow) => {
      var outputIter: Iterator[JoinedRow] = Iterator()
      val key = keyGenerator(thisRow)

      val nullThisRow = if (joinSide == LeftSide) nullLeft else nullRight
      val nullThatRow = if (joinSide == LeftSide) nullRight else nullLeft

      if (joinSide == LeftSide) {
        if (updateCase) {
          val insertKey = key
          val insertRow = thisRow
          val deleteKey = deleteKeyGenerator(deleteRow)

          // Update key, perform delete and insert separately
          if (!insertKey.equals(deleteKey)) {
            if (hasMatch(otherSideJoiner, deleteKey, deleteRow, leftGenerateRow)) {
               outputIter = outputIter ++ Seq(deleteRow).toIterator.map{row => {
                val joinedRow = generateJoinedRow2(row, nullThatRow)
                joinedRow.setInsert(false)
                joinedRow
              }}
            }

            if (hasMatch(otherSideJoiner, insertKey, insertRow, leftGenerateRow)) {
              outputIter = outputIter ++ Seq(insertRow).toIterator.map{row => {
                val joinedRow = generateJoinedRow1(row, nullThatRow)
                joinedRow.setInsert(true)
                joinedRow
              }}
            }

          } else {
            // TODO: There is a bug here
            if (hasMatch(otherSideJoiner, insertKey, deleteRow, leftGenerateRow)) {
              val insertJoinedRow = generateJoinedRow1(insertRow, nullThatRow)
              val deleteJoinedRow = generateJoinedRow2(deleteRow, nullThatRow)
              outputIter = outputIter ++ Seq(deleteJoinedRow).
                toIterator.zip(Seq(insertJoinedRow).toIterator).flatMap{pair => {
                  val tmpDeleteRow = pair._1
                  val tmpInsertRow = pair._2
                  tmpDeleteRow.setInsert(false)
                  tmpDeleteRow.setUpdate(true)
                  tmpInsertRow.setInsert(true)
                  tmpInsertRow.setUpdate(false)
                  Seq(tmpDeleteRow, tmpInsertRow)
              }}
            }
          }

          deleteAndUpdateCounter(otherSideJoiner, deleteKey, deleteRow, leftGenerateRow)
          insertAndUpdateCounter(otherSideJoiner, insertKey, insertRow, leftGenerateRow)

        } else {
          if (isInsert) {
            if (hasMatch(otherSideJoiner, key, thisRow, leftGenerateRow)) {
              outputIter = Seq(thisRow).toIterator.map{row => {
                val joinedRow = generateJoinedRow2(row, nullThatRow)
                joinedRow.setInsert(isInsert)
                joinedRow
              }}
            }
            insertAndUpdateCounter(otherSideJoiner, key, thisRow, leftGenerateRow)
          } else {
            if (hasMatch(otherSideJoiner, key, thisRow, leftGenerateRow)) {
              outputIter = Seq(thisRow).toIterator.map{row => {
                val joinedRow = generateJoinedRow2(row, nullThatRow)
                joinedRow.setInsert(isInsert)
                joinedRow
              }}
            }
            deleteAndUpdateCounter(otherSideJoiner, key, thisRow, leftGenerateRow)
          }
        }
      } else {  // Right Side
        if (updateCase) {
          val insertKey = key
          val insertRow = thisRow
          val deleteKey = deleteKeyGenerator(deleteRow)

          if (!insertKey.equals(deleteKey)) {
            // Last match, negate
            if (isLastMatch(otherSideJoiner, deleteKey, deleteRow, rightGenerateRow)) {
              outputIter = outputIter ++ otherSideJoiner.joinStateManager.getAll(deleteKey).
                map{thatRowWithCounter => {
                  val joinedRow = generateJoinedRow2(deleteRow, thatRowWithCounter.value)
                  joinedRow.setInsert(false)
                  joinedRow
              }}.filter(postJoinFilter)
            }

            if (isFirstMatch(otherSideJoiner, insertKey, insertRow, rightGenerateRow)) {
              outputIter = outputIter ++ otherSideJoiner.joinStateManager.getAll(insertKey).
                map{thatRowWithCounter => {
                  val joinedRow = generateJoinedRow1(insertRow, thatRowWithCounter.value)
                  joinedRow.setInsert(true)
                  joinedRow
              }}.filter(postJoinFilter)
            }

            deleteAndUpdateCounter(otherSideJoiner, deleteKey, deleteRow, rightGenerateRow)
            insertAndUpdateCounter(otherSideJoiner, insertKey, insertRow, rightGenerateRow)
          } else {
            // TODO: We do not consider this case right now
            throw new IllegalArgumentException("Semi join will not accept updates" +
              "on non-key columns from right subtree")
          }
        } else if (isInsert) {
          // First match, output
          if (isFirstMatch(otherSideJoiner, key, thisRow, rightGenerateRow)) {
            outputIter = outputIter ++ otherSideJoiner.joinStateManager.getAll(key).
              map(thatRowWithCounter => {
                val joinedRow = generateJoinedRow1(thisRow, thatRowWithCounter.value)
                joinedRow.setInsert(true)
                joinedRow
              }).filter(postJoinFilter)
          }

          insertAndUpdateCounter(otherSideJoiner, key, thisRow, rightGenerateRow)
        } else {
          if (isLastMatch(otherSideJoiner, key, thisRow, rightGenerateRow)) {
            outputIter = outputIter ++ otherSideJoiner.joinStateManager.getAll(key).
              map{thatRowWithCounter => {
                val joinedRow = generateJoinedRow2(thisRow, thatRowWithCounter.value)
                joinedRow.setInsert(false)
                joinedRow
            }}.filter(postJoinFilter)
          }

          deleteAndUpdateCounter(otherSideJoiner, key, thisRow, rightGenerateRow)
        }
      }

      outputIter

    }: Iterator[JoinedRow]

    // insertRow is from the right side
    private def antiIsFirstMatch (otherSideJoiner: OneSideHashJoiner,
                                  key: UnsafeRow, thisRow: UnsafeRow,
                                  generateRow: (InternalRow, InternalRow) => JoinedRow):
    Boolean = {
      otherSideJoiner.joinStateManager.getAll(key).exists(valueWithCounter => {
        val joinedRow = generateRow(thisRow, valueWithCounter.value)
        postJoinFilter(joinedRow) && valueWithCounter.counter == -1})
    }

    private[this] val antiJoinedRow = new JoinedRow

    // insertRow is from the right side, for negating rows with nulls
    private def genAntiJoinInsertIter(otherSideJoiner: OneSideHashJoiner,
                                      insertKey: UnsafeRow, insertRow: UnsafeRow):
    Iterator[JoinedRow] = {
      otherSideJoiner.joinStateManager.getAll(insertKey)
        .flatMap(valueWithCounter => {
          val joinedRow = antiJoinedRow.withLeft(valueWithCounter.value)
              .withRight(insertRow)
          if (postJoinFilter(joinedRow) && valueWithCounter.counter == 0) {
            valueWithCounter.incCounter
            joinedRow.cleanStates()
            joinedRow.setInsert(false)
            Seq(joinedRow)
          } else Iterator.empty
        })
    }

    // deleteRow is from the right side, for generating rows with nulls
    private def genAntiJoinDeleteIter(otherSideJoiner: OneSideHashJoiner,
                                      deleteKey: UnsafeRow, deleteRow: UnsafeRow):
    Iterator[JoinedRow] = {
      otherSideJoiner.joinStateManager.getAll(deleteKey)
        .flatMap(valueWithCounter => {
          val joinedRow = antiJoinedRow.withLeft(valueWithCounter.value)
              .withRight(deleteRow)
          if (postJoinFilter(joinedRow) && valueWithCounter.counter == 0) {
            valueWithCounter.decCounter
            joinedRow.cleanStates()
            joinedRow.setInsert(true)
            Seq(joinedRow)
          } else Iterator.empty
        })
    }

    private val LeftAntiJoinOneRow = (thisRow: UnsafeRow,
               deleteRow: UnsafeRow,
               isInsert: Boolean,
               updateCase: Boolean,
               otherSideJoiner: OneSideHashJoiner,
               generateJoinedRow1: (InternalRow, InternalRow) => JoinedRow,
               generateJoinedRow2: (InternalRow, InternalRow) => JoinedRow) => {
      val key = keyGenerator(thisRow)
      var outerIter: Iterator[JoinedRow] = Iterator()

      val generateRow = if (joinSide == LeftSide) leftGenerateRow
                        else rightGenerateRow

      if (updateCase) {
        val insertKey = key
        val insertRow = thisRow
        val deleteKey = deleteKeyGenerator(deleteRow)

        // Updates on keys, concatenate the two lists together
        if (!insertKey.equals(deleteKey)) {

          // Generate negations
          val valueWithCounter = joinStateManager.get(deleteKey, deleteRow)
          if (valueWithCounter.counter == -1) {
            outerIter = outerIter ++ deleteNegateNullIter(deleteRow)
          }

          if (joinSide == RightSide) {
            outerIter = outerIter ++
              genAntiJoinInsertIter(otherSideJoiner, insertKey, insertRow)
          }

          // Generate rows with nulls
          outerIter = outerIter ++ genAntiJoinDeleteIter(otherSideJoiner, deleteKey, deleteRow)
          outerIter = outerIter ++ insertGenNullIter(insertKey)

        } else { // Updates on non-key columns, counters unchanged
          val valueWithCounter = joinStateManager.get(insertKey, deleteRow)
          if (valueWithCounter.counter == -1) {
            outerIter = genOuterJoinUpdateIter(insertKey, insertRow, deleteRow)
          }
        }

        updatedStateRowsCount += 2
        deleteAndUpdateCounter(otherSideJoiner, deleteKey, deleteRow, generateRow)
        insertAndUpdateCounter(otherSideJoiner, insertKey, insertRow, generateRow)

      } else {
        if (isInsert) {
          if (joinSide == RightSide) {
            outerIter = genAntiJoinInsertIter(otherSideJoiner, key, thisRow)
          }
          outerIter = outerIter ++ insertGenNullIter(key)

          insertAndUpdateCounter(otherSideJoiner, key, thisRow, generateRow)
        }
        else {
          val valueWithCounter = joinStateManager.get(key, thisRow)
          if (valueWithCounter.counter == -1) {
            outerIter = deleteNegateNullIter(thisRow)
          }
          outerIter = outerIter ++ genAntiJoinDeleteIter(otherSideJoiner, key, thisRow)

          deleteAndUpdateCounter(otherSideJoiner, key, thisRow, generateRow)
        }

        updatedStateRowsCount += 1
      }

      outerIter

    }: Iterator[JoinedRow]

    def generateJoinOneRow(): (UnsafeRow, UnsafeRow, Boolean, Boolean, OneSideHashJoiner,
      (InternalRow, InternalRow) => JoinedRow, (InternalRow, InternalRow) => JoinedRow)
      => Iterator[JoinedRow] = {
      joinType match {
        case Inner | LeftOuter | RightOuter | FullOuter =>
          InnerPlusOuterJoinOneRow
        case LeftSemi =>
          LeftSemiJoinOneRow
        case LeftAnti =>
          LeftAntiJoinOneRow
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
              s"On ${joinSide} of ${condition}: " +
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

    /**
     * Get an iterator over the values stored in this joiner's state manager for the given key.
     *
     * Should not be interleaved with mutations.
     */
    def get(key: UnsafeRow): Iterator[ValueWithCounter] = {
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
  }
}
