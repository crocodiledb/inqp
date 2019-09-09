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

import scala.collection.mutable

import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.aggregate.SlothHashAggregateExec
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2ScanExec
import org.apache.spark.sql.sources.v2.reader.streaming.MicroBatchReader

class SlothDBQueryExecution(plan: SparkPlan) {

  // private val sourceToMeta: mutable.HashMap[MicroBatchReader, SlothSourceMetaData] =
  //   mutable.HashMap.empty

  // private val aggtoMeta:
  //   mutable.ArrayBuffer[Tuple2[SlothHashAggregateExec, SlothSourceMetaData]] =
  //   mutable.ArrayBuffer.empty

  // def updateIncrementability(newPlan: SparkPlan): Unit = {
  //   newPlan.collect{case sourceExec: DataSourceV2ScanExec => sourceExec}
  //     .foreach{source => {
  //       sourceToMeta(source.reader.asInstanceOf[MicroBatchReader])
  //         .updateIncrementabilityAndNextBatch(newPlan)
  //     }}

  //   newPlan.collect()
  // }

  // def getSourceOffset(s: MicroBatchReader): Offset = {
  //   sourceToMeta(s).getSourceOffset()
  // }

  // def getIsAggExecute(): Boolean = {
  //   false
  // }

  // private def buildIncPlan(plan: SparkPlan): SlothIncPlan = {
  //   plan match {
  //     case hashJoin: SlothThetaJoinExec =>
  //       val rawPlan = hashJoin.copy()
  //       val children = plan.children.map(child => buildIncPlan(child))
  //       new SlothIncPlan(rawPlan, children)
  //   }
  // }

  // private class SlothSourceMetaData {
  //   private var incrementability: Double = _
  //   private var offsetPerBatch: Offset = _
  //   private var delayedOffset: Offset = _
  //   private var delayedBatch: Int = _

  //   def getSourceOffset(): Offset = {
  //     null
  //   }

  //   def updateIncrementabilityAndNextBatch(plan: SparkPlan): Unit = {

  //   }
  // }

  // private class SlothIncPlan (plan: SparkPlan,
  //                             children: Seq[SlothIncPlan]) {
  // }

}

