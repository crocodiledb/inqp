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

import java.io.{FileWriter, IOException, PrintWriter}
import java.util.Optional

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.collection.mutable.{Map => MutableMap}

import org.apache.spark.sql.{Dataset, SlothDBContext, SlothDBCostModel, SparkSession}
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.catalyst.expressions.{Alias, CurrentBatchTimestamp, CurrentDate, CurrentTimestamp}
import org.apache.spark.sql.catalyst.plans.logical.{LocalRelation, LogicalPlan, Project}
import org.apache.spark.sql.execution.{SlothFilterExec, SlothProjectExec, SQLExecution}
import org.apache.spark.sql.execution.datasources.v2.{StreamingDataSourceV2Relation, WriteToDataSourceV2}
import org.apache.spark.sql.execution.streaming.sources.MicroBatchWriter
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.sources.v2.{DataSourceOptions, DataSourceV2, MicroBatchReadSupport, StreamWriteSupport}
import org.apache.spark.sql.sources.v2.reader.streaming.{MicroBatchReader, Offset => OffsetV2}
import org.apache.spark.sql.streaming.{OutputMode, ProcessingTime, Trigger}
import org.apache.spark.util.{Clock, Utils}


class MicroBatchExecution(
    sparkSession: SparkSession,
    name: String,
    checkpointRoot: String,
    analyzedPlan: LogicalPlan,
    sink: BaseStreamingSink,
    trigger: Trigger,
    triggerClock: Clock,
    outputMode: OutputMode,
    extraOptions: Map[String, String],
    deleteCheckpointOnStop: Boolean)
  extends StreamExecution(
    sparkSession, name, checkpointRoot, analyzedPlan, sink,
    trigger, triggerClock, outputMode, deleteCheckpointOnStop) {

  import SlothDBContext._

  @volatile protected var sources: Seq[BaseStreamingSource] = Seq.empty

  private val readerToDataSourceMap =
    MutableMap.empty[MicroBatchReader, (DataSourceV2, Map[String, String])]

  private val triggerExecutor = trigger match {
    case t: ProcessingTime => ProcessingTimeExecutor(t, triggerClock)
    case OneTimeTrigger => OneTimeExecutor()
    case _ => throw new IllegalStateException(s"Unknown type of trigger: $trigger")
  }

  private var watermarkTracker: WatermarkTracker = _

  // SlothDB: Adding Projection Id
  val projArray = new mutable.ArrayBuffer[Tuple2[SlothProjectExec, Long]]()
  var projId = 100
  var isLastBatch: Boolean = _

  val filterArray = new mutable.ArrayBuffer[Tuple2[SlothFilterExec, Long]]()
  var filterId = 200

  var finalAggStartId = 300

  // SlothDB: Cost model
  val slothCostModel = new SlothDBCostModel();

  override lazy val logicalPlan: LogicalPlan = {
    assert(queryExecutionThread eq Thread.currentThread,
      "logicalPlan must be initialized in QueryExecutionThread " +
        s"but the current thread was ${Thread.currentThread}")
    var nextSourceId = 0L
    val toExecutionRelationMap = MutableMap[StreamingRelation, StreamingExecutionRelation]()
    val v2ToExecutionRelationMap = MutableMap[StreamingRelationV2, StreamingExecutionRelation]()
    // We transform each distinct streaming relation into a StreamingExecutionRelation, keeping a
    // map as we go to ensure each identical relation gets the same StreamingExecutionRelation
    // object. For each microbatch, the StreamingExecutionRelation will be replaced with a logical
    // plan for the data within that batch.
    // Note that we have to use the previous `output` as attributes in StreamingExecutionRelation,
    // since the existing logical plan has already used those attributes. The per-microbatch
    // transformation is responsible for replacing attributes with their final values.

    val disabledSources =
      sparkSession.sqlContext.conf.disabledV2StreamingMicroBatchReaders.split(",")

    val _logicalPlan = analyzedPlan.transform {
      case streamingRelation@StreamingRelation(dataSourceV1, sourceName, output) =>
        toExecutionRelationMap.getOrElseUpdate(streamingRelation, {
          // Materialize source to avoid creating it in every batch
          val metadataPath = s"$resolvedCheckpointRoot/sources/$nextSourceId"
          val source = dataSourceV1.createSource(metadataPath)
          nextSourceId += 1
          logInfo(s"Using Source [$source] from DataSourceV1 named '$sourceName' [$dataSourceV1]")
          StreamingExecutionRelation(source, output)(sparkSession)
        })
      case s @ StreamingRelationV2(
        dataSourceV2: MicroBatchReadSupport, sourceName, options, output, _) if
          !disabledSources.contains(dataSourceV2.getClass.getCanonicalName) =>
        v2ToExecutionRelationMap.getOrElseUpdate(s, {
          // Materialize source to avoid creating it in every batch
          val metadataPath = s"$resolvedCheckpointRoot/sources/$nextSourceId"
          val reader = dataSourceV2.createMicroBatchReader(
            Optional.empty(), // user specified schema
            metadataPath,
            new DataSourceOptions(options.asJava))
          nextSourceId += 1
          readerToDataSourceMap(reader) = dataSourceV2 -> options
          logInfo(s"Using MicroBatchReader [$reader] from " +
            s"DataSourceV2 named '$sourceName' [$dataSourceV2]")
          StreamingExecutionRelation(reader, output)(sparkSession)
        })
      case s @ StreamingRelationV2(dataSourceV2, sourceName, _, output, v1Relation) =>
        v2ToExecutionRelationMap.getOrElseUpdate(s, {
          // Materialize source to avoid creating it in every batch
          val metadataPath = s"$resolvedCheckpointRoot/sources/$nextSourceId"
          if (v1Relation.isEmpty) {
            throw new UnsupportedOperationException(
              s"Data source $sourceName does not support microbatch processing.")
          }
          val source = v1Relation.get.dataSource.createSource(metadataPath)
          nextSourceId += 1
          logInfo(s"Using Source [$source] from DataSourceV2 named '$sourceName' [$dataSourceV2]")
          StreamingExecutionRelation(source, output)(sparkSession)
        })
    }
    sources = _logicalPlan.collect { case s: StreamingExecutionRelation => s.source }
    uniqueSources = sources.distinct
    _logicalPlan
  }

  /**
   * Signifies whether current batch (i.e. for the batch `currentBatchId`) has been constructed
   * (i.e. written to the offsetLog) and is ready for execution.
   */
  private var isCurrentBatchConstructed = false

  /**
   * Signals to the thread executing micro-batches that it should stop running after the next
   * batch. This method blocks until the thread stops running.
   */
  override def stop(): Unit = {
    // Set the state to TERMINATED so that the batching thread knows that it was interrupted
    // intentionally
    state.set(TERMINATED)
    if (queryExecutionThread.isAlive) {
      sparkSession.sparkContext.cancelJobGroup(runId.toString)
      queryExecutionThread.interrupt()
      queryExecutionThread.join()
      // microBatchThread may spawn new jobs, so we need to cancel again to prevent a leak
      sparkSession.sparkContext.cancelJobGroup(runId.toString)
    }
    logInfo(s"Query $prettyIdString was stopped")
  }

  private def getFinalOffsets(): Map[BaseStreamingSource, String] = {
    uniqueSources.map(source => {
      (source, source.asInstanceOf[MicroBatchReader].getFinalOffset.json())
    }).toMap
  }

  private def reportSlothOverhead(maxStep: Int, overhead: Double): Unit = {
    var overheadFile: String = null
    try {
      val dir = slothdbStatDir
      if (dir != null) {
        overheadFile = dir + "/slothoverhead.stat"
        val constraint =
          if (sparkSession.conf.get(SQLConf.SLOTHDB_LATENCY_CONSTRAINT).isDefined) {
            sparkSession.conf.get(SQLConf.SLOTHDB_LATENCY_CONSTRAINT).get
          } else if (sparkSession.conf.get(SQLConf.SLOTHDB_RESOURCE_CONSTRAINT).isDefined) {
            sparkSession.conf.get(SQLConf.SLOTHDB_RESOURCE_CONSTRAINT).get
          } else {
            -1
          }
        val pw = new PrintWriter(new FileWriter(overheadFile, true))

        pw.print(f"${name}\t${constraint}\t${maxStep}\t${overhead}\n")

        pw.close()

      }
    } catch {
      case _: NoSuchElementException =>
        logInfo("SlothDB Stats Dir is not set")
      case _: IOException =>
        logError(s"Writing ${overheadFile} error")
    }
  }

  /**
   * Repeatedly attempts to run batches as data arrives.
   */
  protected def runActivatedStream(sparkSessionForStream: SparkSession): Unit = {

    val noDataBatchesEnabled =
      sparkSessionForStream.sessionState.conf.streamingNoDataMicroBatchesEnabled

    if (enable_slothdb) {
      execution_mode = sparkSession.conf.get(SQLConf.SLOTHDB_EXECUTION_MODE).getOrElse(-1)

      if (execution_mode != SLOTHTRAINING) {
        val incAware =
          if (execution_mode == INCAWARE_SUBPLAN ||
            execution_mode == INCAWARE_PATH ||
            execution_mode == SLOTHOVERHEAD) {
            true
          } else {
            false
          }

        val mpDecompose =
          if (execution_mode == INCAWARE_PATH || execution_mode == SLOTHOVERHEAD) true
          else false

        val costBias = sparkSession.conf.get(SQLConf.SLOTHDB_COST_MODEL_BIAS).getOrElse(1.0)
        val maxStep = sparkSession.conf.get(SQLConf.SLOTHDB_MAX_STEP).getOrElse(100)

        slothCostModel.initialize(logicalPlan,
          name,
          slothdbStatDir,
          sparkSession.conf.get(SQLConf.SHUFFLE_PARTITIONS.key).toInt,
          mpDecompose,
          incAware,
          costBias,
          maxStep,
          execution_mode == SLOTHOVERHEAD)

        if (execution_mode != SLOTHINCSTAT) {
          slothCostModel.loadEndOffsets(getFinalOffsets())
          val start = System.nanoTime()
          if (sparkSession.conf.get(SQLConf.SLOTHDB_LATENCY_CONSTRAINT).isDefined) {
            val latency_constraint =
              sparkSession.conf.get(SQLConf.SLOTHDB_LATENCY_CONSTRAINT).get
            val inc_percentage =
              sparkSession.conf.get(SQLConf.SLOTHDB_INC_PERCENTAGE).getOrElse(1.0)
            val sample_time =
              sparkSession.conf.get(SQLConf.SLOTHDB_SAMPLE_TIME).getOrElse(1.0)
            slothCostModel.genTriggerPlanForLatencyConstraint(latency_constraint,
              inc_percentage,
              sample_time)
          } else {
            val resource_constraint =
              sparkSession.conf.get(SQLConf.SLOTHDB_RESOURCE_CONSTRAINT).get
            val inc_percentage =
              sparkSession.conf.get(SQLConf.SLOTHDB_INC_PERCENTAGE).getOrElse(1.0)
            slothCostModel.genTriggerPlanForResourceConstraint(resource_constraint, inc_percentage)
          }
          val overhead = ((System.nanoTime() - start)/1000000).toDouble
          if (execution_mode == SLOTHOVERHEAD) {
            reportSlothOverhead(maxStep, overhead)
            return
          }
        }
      }

    }

    triggerExecutor.execute(() => {
      if (isActive) {
        var currentBatchHasNewData = false // Whether the current batch had new data

        startTrigger()

        reportTimeTaken("triggerExecution") {
          // We'll do this initialization only once every start / restart
          if (currentBatchId < 0) {
            populateStartOffsets(sparkSessionForStream)
            logInfo(s"Stream started from $committedOffsets")
          }

          // Set this before calling constructNextBatch() so any Spark jobs executed by sources
          // while getting new data have the correct description
          sparkSession.sparkContext.setJobDescription(getBatchDescriptionString)

          // Try to construct the next batch. This will return true only if the next batch is
          // ready and runnable. Note that the current batch may be runnable even without
          // new data to process as `constructNextBatch` may decide to run a batch for
          // state cleanup, etc. `isNewDataAvailable` will be updated to reflect whether new data
          // is available or not.
          if (!isCurrentBatchConstructed) {
            isCurrentBatchConstructed = constructNextBatch(noDataBatchesEnabled)
          }

          // Record the trigger offset range for progress reporting *before* processing the batch
          recordTriggerOffsets(from = committedOffsets, to = availableOffsets)

          // Remember whether the current batch has data or not. This will be required later
          // for bookkeeping after running the batch, when `isNewDataAvailable` will have changed
          // to false as the batch would have already processed the available data.
          currentBatchHasNewData = isNewDataAvailable

          currentStatus = currentStatus.copy(isDataAvailable = isNewDataAvailable)
          if (isCurrentBatchConstructed) {
            if (currentBatchHasNewData) updateStatusMessage("Processing new data")
            else updateStatusMessage("No new data but cleaning up state")
            runBatch(sparkSessionForStream)
          } else {
            updateStatusMessage("Waiting for data to arrive")
          }
        }

        // Must be outside reportTimeTaken so it is recorded
        val currentStep =
          if (slothCostModel != null) slothCostModel.currentStep
          else 0
        finishTrigger(currentBatchHasNewData, slothCostModel.currentStep)

        // Signal waiting threads. Note this must be after finishTrigger() to ensure all
        // activities (progress generation, etc.) have completed before signaling.
        withProgressLocked { awaitProgressLockCondition.signalAll() }

        // If the current batch has been executed, then increment the batch id and reset flag.
        // Otherwise, there was no data to execute the batch and sleep for some time
        if (isCurrentBatchConstructed) {
          currentBatchId += 1
          isCurrentBatchConstructed = false
        } else Thread.sleep(pollingDelayMs)

        if (!currentBatchHasNewData && SlothDBContext.enable_slothdb) {
          slothDBWriteStats(slothCostModel)
          stop()
        }
      }
      updateStatusMessage("Waiting for next trigger")
      isActive
    })
  }

  /**
   * Populate the start offsets to start the execution at the current offsets stored in the sink
   * (i.e. avoid reprocessing data that we have already processed). This function must be called
   * before any processing occurs and will populate the following fields:
   *  - currentBatchId
   *  - committedOffsets
   *  - availableOffsets
   *  The basic structure of this method is as follows:
   *
   *  Identify (from the offset log) the offsets used to run the last batch
   *  IF last batch exists THEN
   *    Set the next batch to be executed as the last recovered batch
   *    Check the commit log to see which batch was committed last
   *    IF the last batch was committed THEN
   *      Call getBatch using the last batch start and end offsets
   *      // ^^^^ above line is needed since some sources assume last batch always re-executes
   *      Setup for a new batch i.e., start = last batch end, and identify new end
   *    DONE
   *  ELSE
   *    Identify a brand new batch
   *  DONE
   */
  private def populateStartOffsets(sparkSessionToRunBatches: SparkSession): Unit = {
    offsetLog.getLatest() match {
      case Some((latestBatchId, nextOffsets)) =>
        /* First assume that we are re-executing the latest known batch
         * in the offset log */
        currentBatchId = latestBatchId
        isCurrentBatchConstructed = true
        availableOffsets = nextOffsets.toStreamProgress(sources)
        /* Initialize committed offsets to a committed batch, which at this
         * is the second latest batch id in the offset log. */
        if (latestBatchId != 0) {
          val secondLatestBatchId = offsetLog.get(latestBatchId - 1).getOrElse {
            throw new IllegalStateException(s"batch ${latestBatchId - 1} doesn't exist")
          }
          committedOffsets = secondLatestBatchId.toStreamProgress(sources)
        }

        // update offset metadata
        nextOffsets.metadata.foreach { metadata =>
          OffsetSeqMetadata.setSessionConf(metadata, sparkSessionToRunBatches.conf)
          offsetSeqMetadata = OffsetSeqMetadata(
            metadata.batchWatermarkMs, metadata.batchTimestampMs, sparkSessionToRunBatches.conf)
          watermarkTracker = WatermarkTracker(sparkSessionToRunBatches.conf)
          watermarkTracker.setWatermark(metadata.batchWatermarkMs)
        }

        /* identify the current batch id: if commit log indicates we successfully processed the
         * latest batch id in the offset log, then we can safely move to the next batch
         * i.e., committedBatchId + 1 */
        commitLog.getLatest() match {
          case Some((latestCommittedBatchId, commitMetadata)) =>
            if (latestBatchId == latestCommittedBatchId) {
              /* The last batch was successfully committed, so we can safely process a
               * new next batch but first:
               * Make a call to getBatch using the offsets from previous batch.
               * because certain sources (e.g., KafkaSource) assume on restart the last
               * batch will be executed before getOffset is called again. */
              availableOffsets.foreach {
                case (source: Source, end: Offset) =>
                  val start = committedOffsets.get(source)
                  source.getBatch(start, end)
                case nonV1Tuple =>
                  // The V2 API does not have the same edge case requiring getBatch to be called
                  // here, so we do nothing here.
              }
              currentBatchId = latestCommittedBatchId + 1
              isCurrentBatchConstructed = false
              committedOffsets ++= availableOffsets
              watermarkTracker.setWatermark(
                math.max(watermarkTracker.currentWatermark, commitMetadata.nextBatchWatermarkMs))
            } else if (latestCommittedBatchId < latestBatchId - 1) {
              logWarning(s"Batch completion log latest batch id is " +
                s"${latestCommittedBatchId}, which is not trailing " +
                s"batchid $latestBatchId by one")
            }
          case None => logInfo("no commit log present")
        }
        logInfo(s"Resuming at batch $currentBatchId with committed offsets " +
          s"$committedOffsets and available offsets $availableOffsets")
      case None => // We are starting this stream for the first time.
        logInfo(s"Starting new streaming query.")
        currentBatchId = 0
        watermarkTracker = WatermarkTracker(sparkSessionToRunBatches.conf)
    }
  }

  /**
   * Returns true if there is any new data available to be processed.
   */
  private def isNewDataAvailable: Boolean = {
    availableOffsets.exists {
      case (source, available) =>
        committedOffsets
          .get(source)
          .map(committed => committed != available)
          .getOrElse(true)
    }
  }

  /**
   * Attempts to construct a batch according to:
   *  - Availability of new data
   *  - Need for timeouts and state cleanups in stateful operators
   *
   * Returns true only if the next batch should be executed.
   *
   * Here is the high-level logic on how this constructs the next batch.
   * - Check each source whether new data is available
   * - Updated the query's metadata and check using the last execution whether there is any need
   *   to run another batch (for state clean up, etc.)
   * - If either of the above is true, then construct the next batch by committing to the offset
   *   log that range of offsets that the next batch will process.
   */
  private def constructNextBatch(noDataBatchesEnabled: Boolean): Boolean = withProgressLocked {
    if (isCurrentBatchConstructed) return true

    // SlothDB: construct the next batch
    val nextBatch =
      if (execution_mode == INCAWARE_SUBPLAN ||
        execution_mode == INCAWARE_PATH ||
        execution_mode == INCOBLIVIOUS) {
        slothCostModel.constructNewData()
      } else {
        null
      }

    // Generate a map from each unique source to the next available offset.
    val latestOffsets: Map[BaseStreamingSource, Option[Offset]] = uniqueSources.map {
      case s: Source =>
        updateStatusMessage(s"Getting offsets from $s")
        reportTimeTaken("getOffset") {
          (s, s.getOffset)
        }
      case s: MicroBatchReader =>
        updateStatusMessage(s"Getting offsets from $s")
        reportTimeTaken("setOffsetRange") {
          // Once v1 streaming source execution is gone, we can refactor this away.
          // For now, we set the range here to get the source to infer the available end offset,
          // get that offset, and then set the range again when we later execute.
          if ((execution_mode == INCAWARE_SUBPLAN ||
            execution_mode == INCAWARE_PATH ||
            execution_mode == INCOBLIVIOUS) &&
            nextBatch != null) {
            val endOffset = toJava(nextBatch.get(s).map(off => s.deserializeOffset(off)))
            s.setOffsetRange(
              toJava(availableOffsets.get(s).map(off => s.deserializeOffset(off.json))),
              endOffset)
          } else {
            s.setOffsetRange(
              toJava(availableOffsets.get(s).map(off => s.deserializeOffset(off.json))),
              Optional.empty())
          }
        }

        val currentOffset = reportTimeTaken("getEndOffset") { s.getEndOffset() }
        (s, Option(currentOffset))
    }.toMap
    availableOffsets ++= latestOffsets.filter { case (_, o) => o.nonEmpty }.mapValues(_.get)

    isLastBatch = !uniqueSources.exists(s =>
      s match {
        case reader: MicroBatchReader =>
          reader.hasMoreData
        case _ =>
          true
      }
    )

    // Update the query metadata
    offsetSeqMetadata = offsetSeqMetadata.copy(
      batchWatermarkMs = watermarkTracker.currentWatermark,
      batchTimestampMs = triggerClock.getTimeMillis())

    // Check whether next batch should be constructed
    val lastExecutionRequiresAnotherBatch = noDataBatchesEnabled &&
      Option(lastExecution).exists(_.shouldRunAnotherBatch(offsetSeqMetadata))
    val shouldConstructNextBatch = isNewDataAvailable || lastExecutionRequiresAnotherBatch
    logTrace(
      s"noDataBatchesEnabled = $noDataBatchesEnabled, " +
      s"lastExecutionRequiresAnotherBatch = $lastExecutionRequiresAnotherBatch, " +
      s"isNewDataAvailable = $isNewDataAvailable, " +
      s"shouldConstructNextBatch = $shouldConstructNextBatch")

    if (shouldConstructNextBatch) {
      // Commit the next batch offset range to the offset log
      updateStatusMessage("Writing offsets to log")
      reportTimeTaken("walCommit") {
        assert(offsetLog.add(currentBatchId,
          availableOffsets.toOffsetSeq(sources, offsetSeqMetadata)),
          s"Concurrent update to the log. Multiple streaming jobs detected for $currentBatchId")
        logInfo(s"Committed offsets for batch $currentBatchId. " +
          s"Metadata ${offsetSeqMetadata.toString}")

        // NOTE: The following code is correct because runStream() processes exactly one
        // batch at a time. If we add pipeline parallelism (multiple batches in flight at
        // the same time), this cleanup logic will need to change.

        // Now that we've updated the scheduler's persistent checkpoint, it is safe for the
        // sources to discard data from the previous batch.
        if (currentBatchId != 0) {
          val prevBatchOff = offsetLog.get(currentBatchId - 1)
          if (prevBatchOff.isDefined) {
            prevBatchOff.get.toStreamProgress(sources).foreach {
              case (src: Source, off) => src.commit(off)
              case (reader: MicroBatchReader, off) =>
                reader.commit(reader.deserializeOffset(off.json))
              case (src, _) =>
                throw new IllegalArgumentException(
                  s"Unknown source is found at constructNextBatch: $src")
            }
          } else {
            throw new IllegalStateException(s"batch ${currentBatchId - 1} doesn't exist")
          }
        }

        // It is now safe to discard the metadata beyond the minimum number to retain.
        // Note that purge is exclusive, i.e. it purges everything before the target ID.
        if (minLogEntriesToMaintain < currentBatchId) {
          offsetLog.purge(currentBatchId - minLogEntriesToMaintain)
          commitLog.purge(currentBatchId - minLogEntriesToMaintain)
        }
      }
      noNewData = false
    } else {
      noNewData = true
      awaitProgressLockCondition.signalAll()
    }
    shouldConstructNextBatch
  }

  /**
   * Processes any data available between `availableOffsets` and `committedOffsets`.
   * @param sparkSessionToRunBatch Isolated [[SparkSession]] to run this batch with.
   */
  private def runBatch(sparkSessionToRunBatch: SparkSession): Unit = {
    logDebug(s"Running batch $currentBatchId")

    // Request unprocessed data from all sources.
    newData = reportTimeTaken("getBatch") {
      availableOffsets.flatMap {
        case (source: Source, available)
          if committedOffsets.get(source).map(_ != available).getOrElse(true) =>
          val current = committedOffsets.get(source)
          val batch = source.getBatch(current, available)
          assert(batch.isStreaming,
            s"DataFrame returned by getBatch from $source did not have isStreaming=true\n" +
              s"${batch.queryExecution.logical}")
          logDebug(s"Retrieving data from $source: $current -> $available")
          Some(source -> batch.logicalPlan)
        case (reader: MicroBatchReader, available)
          if committedOffsets.get(reader).map(_ != available).getOrElse(true) =>
          val current = committedOffsets.get(reader).map(off => reader.deserializeOffset(off.json))
          val availableV2: OffsetV2 = available match {
            case v1: SerializedOffset => reader.deserializeOffset(v1.json)
            case v2: OffsetV2 => v2
          }
          reader.setOffsetRange(
            toJava(current),
            Optional.of(availableV2))
          logDebug(s"Retrieving data from $reader: $current -> $availableV2")
          // printf(s"Retrieving data from $reader: $current -> $availableV2\n")

          val (source, options) = reader match {
            // `MemoryStream` is special. It's for test only and doesn't have a `DataSourceV2`
            // implementation. We provide a fake one here for explain.
            case _: MemoryStream[_] => MemoryStreamDataSource -> Map.empty[String, String]
            // Provide a fake value here just in case something went wrong, e.g. the reader gives
            // a wrong `equals` implementation.
            case _ => readerToDataSourceMap.getOrElse(reader, {
              FakeDataSourceV2 -> Map.empty[String, String]
            })
          }
          Some(reader -> StreamingDataSourceV2Relation(
            reader.readSchema().toAttributes, source, options, reader))
        case _ => None
      }
    }

    // Replace sources in the logical plan with data that has arrived since the last batch.
    val newBatchesPlan = logicalPlan transform {
      case StreamingExecutionRelation(source, output) =>
        newData.get(source).map { dataPlan =>
          assert(output.size == dataPlan.output.size,
            s"Invalid batch: ${Utils.truncatedString(output, ",")} != " +
              s"${Utils.truncatedString(dataPlan.output, ",")}")

          val aliases = output.zip(dataPlan.output).map { case (to, from) =>
            Alias(from, to.name)(exprId = to.exprId, explicitMetadata = Some(from.metadata))
          }
          Project(aliases, dataPlan)
        }.getOrElse {
          LocalRelation(output, isStreaming = true)
        }
    }

    // Rewire the plan to use the new attributes that were returned by the source.
    val newAttributePlan = newBatchesPlan transformAllExpressions {
      case ct: CurrentTimestamp =>
        CurrentBatchTimestamp(offsetSeqMetadata.batchTimestampMs,
          ct.dataType)
      case cd: CurrentDate =>
        CurrentBatchTimestamp(offsetSeqMetadata.batchTimestampMs,
          cd.dataType, cd.timeZoneId)
    }

    val triggerLogicalPlan = sink match {
      case _: Sink => newAttributePlan
      case s: StreamWriteSupport =>
        val writer = s.createStreamWriter(
          s"$runId",
          newAttributePlan.schema,
          outputMode,
          new DataSourceOptions(extraOptions.asJava))
        WriteToDataSourceV2(new MicroBatchWriter(currentBatchId, writer), newAttributePlan)
      case _ => throw new IllegalArgumentException(s"unknown sink type for $sink")
    }

    sparkSessionToRunBatch.sparkContext.setLocalProperty(
      MicroBatchExecution.BATCH_ID_KEY, currentBatchId.toString)
    sparkSessionToRunBatch.sparkContext.setLocalProperty(
      StreamExecution.IS_CONTINUOUS_PROCESSING, false.toString)

    reportTimeTaken("queryPlanning") {
      lastExecution = new IncrementalExecution(
        sparkSessionToRunBatch,
        triggerLogicalPlan,
        outputMode,
        checkpointFile("state"),
        runId,
        currentBatchId,
        offsetSeqMetadata)
      lastExecution.executedPlan // Force the lazy generation of execution plan
      // SlothDB: some SlothDB optimizations
      if (SlothDBContext.enable_slothdb) {
        lastExecution.slothdbOptimization(runId, this)
        if (execution_mode == INCAWARE_SUBPLAN ||
            execution_mode == INCAWARE_PATH ||
            execution_mode == INCOBLIVIOUS) {
          slothCostModel.triggerBlockingExecution(lastExecution.executedPlan)
        } else {
          lastExecution.setRepairMode(lastExecution.executedPlan, true)
        }
      }
    }

    val nextBatch =
      new Dataset(sparkSessionToRunBatch, lastExecution, RowEncoder(lastExecution.analyzed.schema))

    reportTimeTaken("addBatch") {
      SQLExecution.withNewExecutionId(sparkSessionToRunBatch, lastExecution) {
        sink match {
          case s: Sink => s.addBatch(currentBatchId, nextBatch)
          case _: StreamWriteSupport =>
            // This doesn't accumulate any data - it just forces execution of the microbatch writer.
            nextBatch.collect()
        }
      }
    }

    withProgressLocked {
      watermarkTracker.updateWatermark(lastExecution.executedPlan)
      commitLog.add(currentBatchId, CommitMetadata(watermarkTracker.currentWatermark))
      committedOffsets ++= availableOffsets
    }
    logDebug(s"Completed batch ${currentBatchId}")
  }

  /** Execute a function while locking the stream from making an progress */
  private[sql] def withProgressLocked[T](f: => T): T = {
    awaitProgressLock.lock()
    try {
      f
    } finally {
      awaitProgressLock.unlock()
    }
  }

  private def toJava(scalaOption: Option[OffsetV2]): Optional[OffsetV2] = {
    Optional.ofNullable(scalaOption.orNull)
  }
}

object MicroBatchExecution {
  val BATCH_ID_KEY = "streaming.sql.batchId"
}

object MemoryStreamDataSource extends DataSourceV2

object FakeDataSourceV2 extends DataSourceV2
