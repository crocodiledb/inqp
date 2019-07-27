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

package totem.middleground.tpch

import java.util.concurrent.TimeUnit

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.avro.from_avro
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.Trigger

object DataUtils {

  var bootstrap: String = null

  def loadStreamTable(spark: SparkSession,
                      tableName: String,
                      alias: String): DataFrame = {
    val (_, avroSchema, _, _, topics, offsetPerTrigger) = TPCHSchema.GetMetaData(tableName).get

    return spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", bootstrap)
      .option("subscribe", topics)
      .option("startingOffsets", "earliest")
      .option("maxOffsetsPerTrigger", offsetPerTrigger)
      .load().select(from_avro(col("value"), avroSchema).as(alias))
      .selectExpr(alias + ".*")
  }

  def loadStaticTable(spark: SparkSession, tableName: String, alias: String): DataFrame = {
    val (schema, _, _, staticPath, _, _) = TPCHSchema.GetMetaData(tableName).get

    return spark
      .read
      .format("csv")
      .option("sep", "|")
      .schema(schema)
      .load(staticPath)
  }

  def writeToSink(query_result: DataFrame, query_name: String): Unit = {
    val q = query_result
      .writeStream
      .outputMode("append")
      .format("console")
      .trigger(Trigger.ProcessingTime(100, TimeUnit.MILLISECONDS))
       // .option("checkpointLocation", TPCHSchema.checkpointLocation + "/" + "query")
      .queryName(query_name)
      .start()

    q.awaitTermination()
  }
}
