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

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.Trigger

object DataUtils {

  var bootstrap: String = null

  def loadStreamTable(spark: SparkSession,
                      tableName: String,
                      alias: String): DataFrame = {
    val (schema, _, topics, offsetPerTrigger) = TPCHSchema.GetMetaData(tableName).get

    val ts_col = alias + "_ts"
    return spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", bootstrap)
      .option("subscribe", topics)
      .option("startingOffsets", "earliest")
      .option("maxOffsetsPerTrigger", offsetPerTrigger)
      .load().select(from_json(col("value").
        cast("string"), schema).as(alias), col("timestamp").as(ts_col))
      .selectExpr(alias + ".*", ts_col)
      .withWatermark(ts_col, "10 seconds")

  }

  def loadStaticTable(spark: SparkSession, tableName: String, alias: String): DataFrame = {
    val (schema, path, _, _) = TPCHSchema.GetMetaData(tableName).get

    return spark
      .read
      .format("csv")
      .option("sep", "|")
      .schema(schema)
      .load(path)
  }

  def writeToSink(query_result: DataFrame): Unit = {
    val q = query_result
      .writeStream
      .outputMode("append")
      .format("console")
      .trigger(Trigger.ProcessingTime("2 seconds"))
      .start()

    q.awaitTermination()
  }
}
