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

import com.sun.management.OperatingSystemMXBean
import java.io.{FileWriter, IOException, PrintWriter}
import java.lang.management.ManagementFactory
import java.text.SimpleDateFormat
import java.util.{Date, NoSuchElementException, UUID}
import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

import org.apache.spark.internal.Logging
import org.apache.spark.sql.{SlothDBContext, SlothDBCostModel, SparkSession}
import org.apache.spark.sql.SlothDBCostModel._
import org.apache.spark.sql.catalyst.plans.logical.{EventTimeWatermark, LogicalPlan}
import org.apache.spark.sql.catalyst.util.DateTimeUtils
import org.apache.spark.sql.execution._
import org.apache.spark.sql.execution.datasources.v2.{DataSourceV2ScanExec, WriteToDataSourceV2Exec}
import org.apache.spark.sql.execution.exchange.ReusedExchangeExec
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.sources.v2.reader.streaming.MicroBatchReader
import org.apache.spark.sql.streaming._
import org.apache.spark.sql.streaming.StreamingQueryListener.QueryProgressEvent
import org.apache.spark.util.Clock


/**
 * Responsible for continually reporting statistics about the amount of data processed as well
 * as latency for a streaming query.  This trait is designed to be mixed into the
 * [[StreamExecution]], who is responsible for calling `startTrigger` and `finishTrigger`
 * at the appropriate times. Additionally, the status can updated with `updateStatusMessage` to
 * allow reporting on the streams current state (i.e. "Fetching more data").
 */
trait ProgressReporter extends Logging {

  case class ExecutionStats(
    inputRows: Map[BaseStreamingSource, Long],
    stateOperators: Seq[StateOperatorProgress],
    eventTimeStats: Map[String, String])

  // Internal state of the stream, required for computing metrics.
  protected def id: UUID
  protected def runId: UUID
  protected def name: String
  protected def triggerClock: Clock
  protected def logicalPlan: LogicalPlan
  protected def lastExecution: QueryExecution
  protected def newData: Map[BaseStreamingSource, LogicalPlan]
  protected def sources: Seq[BaseStreamingSource]
  protected def sink: BaseStreamingSink
  protected def offsetSeqMetadata: OffsetSeqMetadata
  protected def currentBatchId: Long
  protected def sparkSession: SparkSession
  protected def postEvent(event: StreamingQueryListener.Event): Unit

  protected def slothdbStatDir: String

  // Local timestamps and counters.
  private var currentTriggerStartTimestamp = -1L
  private var currentTriggerEndTimestamp = -1L
  private var currentTriggerStartOffsets: Map[BaseStreamingSource, String] = _
  private var currentTriggerEndOffsets: Map[BaseStreamingSource, String] = _
  // TODO: Restore this from the checkpoint when possible.
  private var lastTriggerStartTimestamp = -1L

  private var totalTimeSec = 0.0
  private var lastTimeSec = 0.0

  private val rowsArray: ArrayBuffer[Long] = new ArrayBuffer[Long]()
  private var totalRows = 0L
  private var totalScanRows = 0L
  private var finalRows = 0L
  private var finalScanRows = 0L

  private val cpuLoadArray: ArrayBuffer[(Int, Array[Double])] =
    new ArrayBuffer[(Int, Array[Double])]()

  private val osBean = ManagementFactory.getPlatformMXBean(
      classOf[OperatingSystemMXBean])

  private val currentDurationsMs = new mutable.HashMap[String, Long]()

  /** Flag that signals whether any error with input metrics have already been logged */
  private var metricWarningLogged: Boolean = false

  /** Holds the most recent query progress updates.  Accesses must lock on the queue itself. */
  private val progressBuffer = new mutable.Queue[StreamingQueryProgress]()

  private val noDataProgressEventInterval =
    sparkSession.sessionState.conf.streamingNoDataProgressEventInterval

  // The timestamp we report an event that has no input data
  private var lastNoDataProgressEventTime = Long.MinValue

  private val timestampFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'") // ISO8601
  timestampFormat.setTimeZone(DateTimeUtils.getTimeZone("UTC"))

  private var slothSummarizedMetrics: SlothSummarizedMetrics = _

  @volatile
  protected var currentStatus: StreamingQueryStatus = {
    new StreamingQueryStatus(
      message = "Initializing StreamExecution",
      isDataAvailable = false,
      isTriggerActive = false)
  }

  /** Returns the current status of the query. */
  def status: StreamingQueryStatus = currentStatus

  /** Returns an array containing the most recent query progress updates. */
  def recentProgress: Array[StreamingQueryProgress] = progressBuffer.synchronized {
    progressBuffer.toArray
  }

  /** Returns the most recent query progress update or null if there were no progress updates. */
  def lastProgress: StreamingQueryProgress = progressBuffer.synchronized {
    progressBuffer.lastOption.orNull
  }

  /** Begins recording statistics about query progress for a given trigger. */
  protected def startTrigger(): Unit = {
    logDebug("Starting Trigger Calculation")
    lastTriggerStartTimestamp = currentTriggerStartTimestamp
    currentTriggerStartTimestamp = triggerClock.getTimeMillis()
    currentStatus = currentStatus.copy(isTriggerActive = true)
    currentTriggerStartOffsets = null
    currentTriggerEndOffsets = null
    currentDurationsMs.clear()
  }

  /**
   * Record the offsets range this trigger will process. Call this before updating
   * `committedOffsets` in `StreamExecution` to make sure that the correct range is recorded.
   */
  protected def recordTriggerOffsets(from: StreamProgress, to: StreamProgress): Unit = {
    currentTriggerStartOffsets = from.mapValues(_.json)
    currentTriggerEndOffsets = to.mapValues(_.json)
  }

  private def updateProgress(newProgress: StreamingQueryProgress): Unit = {
    progressBuffer.synchronized {
      progressBuffer += newProgress
      while (progressBuffer.length >= sparkSession.sqlContext.conf.streamingProgressRetention) {
        progressBuffer.dequeue()
      }
    }
    postEvent(new QueryProgressEvent(newProgress))
    logInfo(s"Streaming query made progress: $newProgress")
  }

  private def getCPULoad(): Double = {
    osBean.getSystemCpuLoad
  }

  private def writeCPULoads(): Unit = {
    val execution_mode = sparkSession.conf.get(SQLConf.SLOTHDB_EXECUTION_MODE).getOrElse(-1)
    if (execution_mode == SlothDBContext.INCAWARE_PATH ||
      execution_mode == SlothDBContext.INCOBLIVIOUS) {

      val dir = slothdbStatDir
      val cpuFile = dir + "/cpu.stat"
      val cpuWriter = new PrintWriter(new FileWriter(cpuFile, true))

      val total_time = sparkSession.conf.get(SQLConf.SLOTHDB_TOTAL_TIME).getOrElse(600000.0)
      val max_batch_step = cpuLoadArray.last._1.toDouble

      val time_step = 100.0
      val max_time_step = total_time / (max_batch_step * time_step)

      var curBatchIdx = 0
      var curCPUIdx = 0
      var executed: Boolean = false
      var overallIdx: Int = 0
      for (i <- 0 until max_batch_step.toInt) {
        if (!executed && i >= cpuLoadArray(curBatchIdx)._1) {
          executed = true
          curCPUIdx = 0
        }

        for (j <- 0 until max_time_step.toInt) {
          if (executed) {
            cpuWriter.print(s"${overallIdx}\t${cpuLoadArray(curBatchIdx)._2(curCPUIdx)}\n")
            curCPUIdx += 1

            if (curCPUIdx == cpuLoadArray(curBatchIdx)._2.length) {
              curBatchIdx += 1
              executed = false
            }

          } else {
            val cpuLoad = getCPULoad()
            cpuWriter.print(s"${overallIdx}\t${cpuLoad}\n")
          }
          overallIdx += 1
        }
      }

      // Last batch
      val cpuArray = cpuLoadArray.last._2
      for (i <- 0 until cpuArray.size) {
        cpuWriter.print(s"${overallIdx}\t${cpuArray(i)}\n")
        overallIdx += 1
      }

      cpuWriter.close()
    }
  }

  protected def slothDBWriteStats(slothDBCostModel: SlothDBCostModel): Unit = {
    var statFile: String = null
    var modelFile: String = null
    try {
      val dir = slothdbStatDir
      if (dir != null) {
        statFile = dir + "/slothdb.stat"
        val shuffleNum = sparkSession.conf.get(SQLConf.SHUFFLE_PARTITIONS.key)
        val execution_mode = sparkSession.conf.get(SQLConf.SLOTHDB_EXECUTION_MODE).getOrElse(-1)
        val constraint =
          if (sparkSession.conf.get(SQLConf.SLOTHDB_LATENCY_CONSTRAINT).isDefined) {
            sparkSession.conf.get(SQLConf.SLOTHDB_LATENCY_CONSTRAINT).get
          } else if (sparkSession.conf.get(SQLConf.SLOTHDB_RESOURCE_CONSTRAINT).isDefined) {
            sparkSession.conf.get(SQLConf.SLOTHDB_RESOURCE_CONSTRAINT).get
          } else {
            -1
          }
        val inc_aware =
          if (execution_mode == SlothDBContext.INCAWARE_PATH) "INCAWARE(PATH)"
          else if (execution_mode == SlothDBContext.INCAWARE_SUBPLAN) "INCAWARE(SUBPLAN)"
          else if (execution_mode == SlothDBContext.INCOBLIVIOUS) "INCOBLIVIOUS"
          else if (execution_mode == SlothDBContext.SLOTHINCSTAT) "INCSTAT"
          else "SLOTHTRAINING"
        val pw = new PrintWriter(new FileWriter(statFile, true))

        val inc_percentage =
          sparkSession.conf.get(SQLConf.SLOTHDB_INC_PERCENTAGE).getOrElse(1.0)

        val cost_bias =
          sparkSession.conf.get(SQLConf.SLOTHDB_COST_MODEL_BIAS).getOrElse(1.0)

        val max_step =
          sparkSession.conf.get(SQLConf.SLOTHDB_MAX_STEP).getOrElse(100)

        // if (execution_mode != SlothDBContext.SLOTHINCSTAT) {
        pw.print(f"${name}\t${constraint}\t${max_step}\t${inc_aware}" +
            f"\t${shuffleNum}\t${inc_percentage}\t${totalTimeSec}%.2f\t${lastTimeSec}%.2f" +
            f"\t${totalRows}\n")
        // } else {
        //   val batchNum = sparkSession.conf.get(SQLConf.SLOTHDB_BATCH_NUM).getOrElse(1)
        //   val estimatedPair = slothDBCostModel.estimateLatencyAndResource(batchNum)
        //   val estimatedCardinality = slothDBCostModel.estimateCardinality(batchNum)
        //   val realPair = slothDBCostModel.groundTruthLatencyAndResource(totalRows,
        //     totalScanRows, finalRows, finalScanRows, batchNum)
        //   pw.print(s"${name}\t${constraint}\t${currentBatchId}\t${inc_aware}" +
        //     s"\t${estimatedPair._1}\t${estimatedPair._2}\t${estimatedCardinality}" +
        //     s"\t${realPair._1}\t${realPair._2}\t${totalRows}\n")

        //   // Stats for PostgreSQL
        //   val postgreStatFile = dir + "/postgre.stat"
        //   val postgreWriter = new PrintWriter(new FileWriter(postgreStatFile, true))
        //   val estimatedCardArray = slothDBCostModel.estimateCardArray(batchNum)
        //   val realCardArray = rowsArray.toArray
        //   estimatedCardArray.zipWithIndex.foreach(pair => {
        //     val idx = pair._2
        //     val estimatedCard = pair._1
        //     val realCard = realCardArray(idx)
        //     postgreWriter
        //       .print(s"${name}\t${currentBatchId}\t${idx}\t" +
        //         s"${estimatedCard.toLong}\t${realCard}\n")
        //   })
        //   postgreWriter.close()
        // }

        pw.close()

        // Write cost model information
        modelFile = dir + f"/${name}_model.stat"
        val modelPW = new PrintWriter(new FileWriter(modelFile, false))
        modelPW.print(getCostModelInfo(slothSummarizedMetrics))
        modelPW.close()

        // writeCPULoads()
      }
    } catch {
      case e: NoSuchElementException =>
        logInfo("SlothDB Stats Dir is not set")
      case e: IOException =>
        logError(s"Writing ${statFile} error")
    }

    print(getSummarizedMetrics(slothSummarizedMetrics, ""))
  }

  private def getProcessTime(curBatchStep: Int): Double = {
    lastTimeSec =
      if (lastExecution.executedPlan.isInstanceOf[WriteToDataSourceV2Exec]) {
        val sink = lastExecution.executedPlan.asInstanceOf[WriteToDataSourceV2Exec]
        cpuLoadArray.append((curBatchStep, sink.cpuLoad.toArray))

        sink.getProcessTime().toDouble - sink.getStartUpTime().toDouble
      } else {
        (currentTriggerEndTimestamp - currentTriggerStartTimestamp).toDouble / 1000
      }
    lastTimeSec
  }

  /** Finalizes the query progress and adds it to list of recent status updates. */
  protected def finishTrigger(hasNewData: Boolean, curBatchStep: Int): Unit = {
    assert(currentTriggerStartOffsets != null && currentTriggerEndOffsets != null)
    currentTriggerEndTimestamp = triggerClock.getTimeMillis()

    val executionStats = extractExecutionStats(hasNewData)

    val processingTimeSec = getProcessTime(curBatchStep)

    val inputTimeSec = if (lastTriggerStartTimestamp >= 0) {
      (currentTriggerStartTimestamp - lastTriggerStartTimestamp).toDouble / 1000
    } else {
      Double.NaN
    }
    logDebug(s"Execution stats: $executionStats")

    val sourceProgress = sources.distinct.map { source =>
      val numRecords = executionStats.inputRows.getOrElse(source, 0L)
      new SourceProgress(
        description = source.toString,
        startOffset = currentTriggerStartOffsets.get(source).orNull,
        endOffset = currentTriggerEndOffsets.get(source).orNull,
        numInputRows = numRecords,
        inputRowsPerSecond = numRecords / inputTimeSec,
        processedRowsPerSecond = numRecords / processingTimeSec
      )
    }

    val sinkProgress = new SinkProgress(sink.toString)

    val newProgress = new StreamingQueryProgress(
      id = id,
      runId = runId,
      name = name,
      timestamp = formatTimestamp(currentTriggerStartTimestamp),
      batchId = currentBatchId,
      durationMs = new java.util.HashMap(currentDurationsMs.toMap.mapValues(long2Long).asJava),
      eventTime = new java.util.HashMap(executionStats.eventTimeStats.asJava),
      stateOperators = executionStats.stateOperators.toArray,
      sources = sourceProgress.toArray,
      sink = sinkProgress)

    // Print out SlothDB Progress to console

    if (hasNewData) {

      totalTimeSec += processingTimeSec

      val slothProgress = new SlothDBProgress(
        id = id,
        runId = runId,
        name = name,
        timestamp = formatTimestamp(currentTriggerStartTimestamp),
        batchId = currentBatchId,
        processTime = processingTimeSec,
        totalProcessTime = totalTimeSec,
        operatorProgress = slothExtractOperatorMetrics().toArray,
        sources = sourceProgress.toArray,
        sink = sinkProgress)

      printf(slothProgress.toString + "\n")
      // printf(slothExtractRowMetricsWithStructure() + "\n")

      slothSummarizeRowMetrics()

      val batchNum = sparkSession.conf.get(SQLConf.SLOTHDB_BATCH_NUM).getOrElse(-1)
      if (batchNum - 2 == currentBatchId) { // The one before the last batch
        val triple = outputRowsStat(slothSummarizedMetrics)
        totalRows = triple._1
        totalScanRows = triple._2

      } else if (batchNum - 1 == currentBatchId) { // The last batch
        val triple = outputRowsStat(slothSummarizedMetrics)
        finalRows = triple._1 - totalRows
        finalScanRows = triple._2 - totalScanRows

        totalRows = triple._1
        totalScanRows = triple._2
      }

      rowsArray.append(outputRowsPostgres(slothSummarizedMetrics))

    }

    if (hasNewData) {
      // Reset noDataEventTimestamp if we processed any data
      lastNoDataProgressEventTime = Long.MinValue
      updateProgress(newProgress)
    } else {
      val now = triggerClock.getTimeMillis()
      if (now - noDataProgressEventInterval >= lastNoDataProgressEventTime) {
        lastNoDataProgressEventTime = now
        updateProgress(newProgress)
      }
    }

    currentStatus = currentStatus.copy(isTriggerActive = false)
  }

  private def slothRowProgress(plan: SparkPlan, indent: String): String = {
    var retString: String = ""
    plan match {
      case metricsTracker: SlothMetricsTracker =>
        retString = indent + metricsTracker.getRowProgress()
        plan.children.foreach(child => retString += slothRowProgress(child, indent + "\t"))
        retString
      case _ =>
        if (plan.children.nonEmpty) slothRowProgress(plan.children(0), indent)
        else ""
    }
  }

  private def slothExtractRowMetricsWithStructure(): String = {
    if (lastExecution == null) return ""
    slothRowProgress(lastExecution.executedPlan, "")
  }

  private def populateSummarizedRowMetrics(plan: SparkPlan,
                                           summarizedMetrics: SlothSummarizedMetrics):
  Unit = {
    plan match {
      case _: SlothMetricsTracker =>
        val childrenMetrics = plan.children.map(child => {
          val childMetrics = new SlothSummarizedMetrics()
          populateSummarizedRowMetrics(child, childMetrics)
          childMetrics
        }).filter(_.hasMetrics)

        summarizedMetrics.nodeType = findNodeType(plan)
        summarizedMetrics.nodeName = plan.nodeName
        summarizedMetrics.numPart = sparkSession.conf.get(SQLConf.SHUFFLE_PARTITIONS.key).toInt
        summarizedMetrics.hasMetrics = true
        summarizedMetrics.children = childrenMetrics

        plan match {
          case hashJoin: SlothSymmetricHashJoinExec =>
            summarizedMetrics.joinType = hashJoin.joinType.toString
          case thetaJoin: SlothThetaJoinExec =>
            summarizedMetrics.joinType = thetaJoin.joinType.toString
          case _ =>
        }

      case _ =>
        if (plan.children.isEmpty) {
          if (plan.isInstanceOf[ReusedExchangeExec]) {
            populateSummarizedRowMetrics(
              plan.asInstanceOf[ReusedExchangeExec].child,
              summarizedMetrics)
          } else {
            summarizedMetrics.hasMetrics = false
          }
        } else populateSummarizedRowMetrics(plan.children(0), summarizedMetrics)
    }
  }

  private def updateSummarizedRowMetrics(plan: SparkPlan,
                                         summarizedMetrics: SlothSummarizedMetrics):
  Unit = {
     plan match {
      case metricsTracker: SlothMetricsTracker =>

        summarizedMetrics.updateMetrics(metricsTracker)
        plan.children.zipWithIndex.foreach(pair => {
          val childPlan = pair._1
          val childIndex = pair._2
          if (childIndex < summarizedMetrics.children.size) {
            updateSummarizedRowMetrics(childPlan, summarizedMetrics.children(childIndex))
          }
        })

      case _ =>
        if (!plan.children.isEmpty) {
          updateSummarizedRowMetrics(plan.children(0), summarizedMetrics)
        } else if (plan.isInstanceOf[ReusedExchangeExec]) {
          updateSummarizedRowMetrics(
            plan.asInstanceOf[ReusedExchangeExec].child,
            summarizedMetrics)
        }
    }
  }

  private def slothSummarizeRowMetrics(): Unit = {
    if (lastExecution == null) return

    val rootPlan = lastExecution.executedPlan
    if (currentBatchId == 0) {
      slothSummarizedMetrics = new SlothSummarizedMetrics()
      populateSummarizedRowMetrics(rootPlan, slothSummarizedMetrics)
    }

    updateSummarizedRowMetrics(rootPlan, slothSummarizedMetrics)

    print(getSummarizedMetrics(slothSummarizedMetrics, ""))
  }

  private def getCostModelInfo(summarizedMetrics: SlothSummarizedMetrics): String = {
    var retString = summarizedMetrics.getCostModelInfo()
    summarizedMetrics.children.foreach(child => {
      retString += getCostModelInfo(child)
    })

    retString
  }

  private def getSummarizedMetrics(summarizedMetrics: SlothSummarizedMetrics,
                                   indent: String): String = {

    var retString = indent + summarizedMetrics.getFormattedMetrics()
    summarizedMetrics.children.foreach(child => {
      retString += getSummarizedMetrics(child, indent + "\t")
    })

    retString
  }

  private def outputRowsStat(summarizedMetrics: SlothSummarizedMetrics): (Long, Long, Long) = {
    var innerTotalRows = 0L
    var innerScanRows = 0L
    var perOperatorRows = 0L

    summarizedMetrics.children.foreach(child => {
      val pair = outputRowsStat(child)
      innerTotalRows += pair._1
      innerScanRows += pair._2
      perOperatorRows += pair._3
    })

    val curOpRows = summarizedMetrics.getNumOfRows()
    innerTotalRows += curOpRows

    if (summarizedMetrics.nodeType == SLOTHSCAN) {
      innerScanRows = curOpRows
    }

    if (summarizedMetrics.children.size == 1 &&
      summarizedMetrics.children(0).nodeType == SLOTHSELECT &&
      curOpRows == perOperatorRows) {
      innerTotalRows -= perOperatorRows
    }

    (innerTotalRows, innerScanRows, curOpRows)
  }

  private def outputRowsPostgres(summarizedMetrics: SlothSummarizedMetrics): Long = {
    var totalRows = summarizedMetrics.getNumOfRows
    if (summarizedMetrics.nodeType == SLOTHSELECT) {
      summarizedMetrics.children(0).children.foreach(child => {
        val childTotal = outputRowsPostgres(child)
        totalRows += childTotal
      })
    } else {
      summarizedMetrics.children.foreach(child => {
        val childTotal = outputRowsPostgres(child)
        totalRows += childTotal
      })
    }
    totalRows
  }

  private def slothExtractOperatorMetrics(): Seq[SlothProgressMetrics] = {
    if (lastExecution == null) return Nil
    // lastExecution could belong to one of the previous triggers if `!hasNewData`.
    // Walking the plan again should be inexpensive.
    lastExecution.executedPlan.collect {
      case p if p.isInstanceOf[SlothMetricsTracker] =>
        p.asInstanceOf[SlothMetricsTracker].getProgress()
    }
  }

  /** Extract statistics about stateful operators from the executed query plan. */
  private def extractStateOperatorMetrics(hasNewData: Boolean): Seq[StateOperatorProgress] = {
    if (lastExecution == null) return Nil
    // lastExecution could belong to one of the previous triggers if `!hasNewData`.
    // Walking the plan again should be inexpensive.
    lastExecution.executedPlan.collect {
      case p if p.isInstanceOf[StateStoreWriter] =>
        val progress = p.asInstanceOf[StateStoreWriter].getProgress()
        if (hasNewData) progress else progress.copy(newNumRowsUpdated = 0)
    }
  }

  /** Extracts statistics from the most recent query execution. */
  private def extractExecutionStats(hasNewData: Boolean): ExecutionStats = {
    val hasEventTime = logicalPlan.collect { case e: EventTimeWatermark => e }.nonEmpty
    val watermarkTimestamp =
      if (hasEventTime) Map("watermark" -> formatTimestamp(offsetSeqMetadata.batchWatermarkMs))
      else Map.empty[String, String]

    // SPARK-19378: Still report metrics even though no data was processed while reporting progress.
    val stateOperators = extractStateOperatorMetrics(hasNewData)

    if (!hasNewData) {
      return ExecutionStats(Map.empty, stateOperators, watermarkTimestamp)
    }

    val numInputRows = extractSourceToNumInputRows()

    val eventTimeStats = lastExecution.executedPlan.collect {
      case e: EventTimeWatermarkExec if e.eventTimeStats.value.count > 0 =>
        val stats = e.eventTimeStats.value
        Map(
          "max" -> stats.max,
          "min" -> stats.min,
          "avg" -> stats.avg.toLong).mapValues(formatTimestamp)
    }.headOption.getOrElse(Map.empty) ++ watermarkTimestamp

    ExecutionStats(numInputRows, stateOperators, eventTimeStats)
  }

  /** Extract number of input sources for each streaming source in plan */
  private def extractSourceToNumInputRows(): Map[BaseStreamingSource, Long] = {

    import java.util.IdentityHashMap
    import scala.collection.JavaConverters._

    def sumRows(tuples: Seq[(BaseStreamingSource, Long)]): Map[BaseStreamingSource, Long] = {
      tuples.groupBy(_._1).mapValues(_.map(_._2).sum) // sum up rows for each source
    }

    val onlyDataSourceV2Sources = {
      // Check whether the streaming query's logical plan has only V2 data sources
      val allStreamingLeaves =
        logicalPlan.collect { case s: StreamingExecutionRelation => s }
      allStreamingLeaves.forall { _.source.isInstanceOf[MicroBatchReader] }
    }

    if (onlyDataSourceV2Sources) {
      // DataSourceV2ScanExec is the execution plan leaf that is responsible for reading data
      // from a V2 source and has a direct reference to the V2 source that generated it. Each
      // DataSourceV2ScanExec records the number of rows it has read using SQLMetrics. However,
      // just collecting all DataSourceV2ScanExec nodes and getting the metric is not correct as
      // a DataSourceV2ScanExec instance may be referred to in the execution plan from two (or
      // even multiple times) points and considering it twice will lead to double counting. We
      // can't dedup them using their hashcode either because two different instances of
      // DataSourceV2ScanExec can have the same hashcode but account for separate sets of
      // records read, and deduping them to consider only one of them would be undercounting the
      // records read. Therefore the right way to do this is to consider the unique instances of
      // DataSourceV2ScanExec (using their identity hash codes) and get metrics from them.
      // Hence we calculate in the following way.
      //
      // 1. Collect all the unique DataSourceV2ScanExec instances using IdentityHashMap.
      //
      // 2. Extract the source and the number of rows read from the DataSourceV2ScanExec instanes.
      //
      // 3. Multiple DataSourceV2ScanExec instance may refer to the same source (can happen with
      //    self-unions or self-joins). Add up the number of rows for each unique source.
      val uniqueStreamingExecLeavesMap =
        new IdentityHashMap[DataSourceV2ScanExec, DataSourceV2ScanExec]()

      lastExecution.executedPlan.collectLeaves().foreach {
        case s: DataSourceV2ScanExec if s.reader.isInstanceOf[BaseStreamingSource] =>
          uniqueStreamingExecLeavesMap.put(s, s)
        case _ =>
      }

      val sourceToInputRowsTuples =
        uniqueStreamingExecLeavesMap.values.asScala.map { execLeaf =>
          val numRows = execLeaf.metrics.get("numOutputRows").map(_.value).getOrElse(0L)
          val source = execLeaf.reader.asInstanceOf[BaseStreamingSource]
          source -> numRows
        }.toSeq
      logDebug("Source -> # input rows\n\t" + sourceToInputRowsTuples.mkString("\n\t"))
      sumRows(sourceToInputRowsTuples)
    } else {

      // Since V1 source do not generate execution plan leaves that directly link with source that
      // generated it, we can only do a best-effort association between execution plan leaves to the
      // sources. This is known to fail in a few cases, see SPARK-24050.
      //
      // We want to associate execution plan leaves to sources that generate them, so that we match
      // the their metrics (e.g. numOutputRows) to the sources. To do this we do the following.
      // Consider the translation from the streaming logical plan to the final executed plan.
      //
      // streaming logical plan (with sources) <==> trigger's logical plan <==> executed plan
      //
      // 1. We keep track of streaming sources associated with each leaf in trigger's logical plan
      //  - Each logical plan leaf will be associated with a single streaming source.
      //  - There can be multiple logical plan leaves associated with a streaming source.
      //  - There can be leaves not associated with any streaming source, because they were
      //      generated from a batch source (e.g. stream-batch joins)
      //
      // 2. Assuming that the executed plan has same number of leaves in the same order as that of
      //    the trigger logical plan, we associate executed plan leaves with corresponding
      //    streaming sources.
      //
      // 3. For each source, we sum the metrics of the associated execution plan leaves.
      //
      val logicalPlanLeafToSource = newData.flatMap { case (source, logicalPlan) =>
        logicalPlan.collectLeaves().map { leaf => leaf -> source }
      }
      val allLogicalPlanLeaves = lastExecution.logical.collectLeaves() // includes non-streaming
      val allExecPlanLeaves = lastExecution.executedPlan.collectLeaves()
      if (allLogicalPlanLeaves.size == allExecPlanLeaves.size) {
        val execLeafToSource = allLogicalPlanLeaves.zip(allExecPlanLeaves).flatMap {
          case (lp, ep) => logicalPlanLeafToSource.get(lp).map { source => ep -> source }
        }
        val sourceToInputRowsTuples = execLeafToSource.map { case (execLeaf, source) =>
          val numRows = execLeaf.metrics.get("numOutputRows").map(_.value).getOrElse(0L)
          source -> numRows
        }
        sumRows(sourceToInputRowsTuples)
      } else {
        if (!metricWarningLogged) {
          def toString[T](seq: Seq[T]): String = s"(size = ${seq.size}), ${seq.mkString(", ")}"

          logWarning(
            "Could not report metrics as number leaves in trigger logical plan did not match that" +
              s" of the execution plan:\n" +
              s"logical plan leaves: ${toString(allLogicalPlanLeaves)}\n" +
              s"execution plan leaves: ${toString(allExecPlanLeaves)}\n")
          metricWarningLogged = true
        }
        Map.empty
      }
    }
  }

  /** Records the duration of running `body` for the next query progress update. */
  protected def reportTimeTaken[T](triggerDetailKey: String)(body: => T): T = {
    val startTime = triggerClock.getTimeMillis()
    val result = body
    val endTime = triggerClock.getTimeMillis()
    val timeTaken = math.max(endTime - startTime, 0)

    val previousTime = currentDurationsMs.getOrElse(triggerDetailKey, 0L)
    currentDurationsMs.put(triggerDetailKey, previousTime + timeTaken)
    logDebug(s"$triggerDetailKey took $timeTaken ms")
    result
  }

  private def formatTimestamp(millis: Long): String = {
    timestampFormat.format(new Date(millis))
  }

  /** Updates the message returned in `status`. */
  protected def updateStatusMessage(message: String): Unit = {
    currentStatus = currentStatus.copy(message = message)
  }
}
