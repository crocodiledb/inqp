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

// scalastyle:off println
package totem.middleground.avro

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.avro.from_avro
import org.apache.spark.sql.functions._

class ConsumeKafka (bootstrap: String) {
  def readAvro(): Unit = {
        val spark = SparkSession.builder()
      .appName("Read Avro")
      .getOrCreate()

    val user = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", bootstrap)
      .option("subscribe", KafkaMeta.userTopics)
      .option("startingOffsets", "earliest")
      .load()

    val query =
      user.select(from_avro(col("value"), KafkaMeta.userAvroSchema.toString).as("user"))
      .selectExpr("user.*")
      .writeStream
      .format("console")
      .outputMode("append")
      .start()

    query.awaitTermination()
  }
}

object ConsumeKafka {
  def main(args: Array[String]): Unit = {

    if (args.length < 1) {
      System.err.println("Usage: QueryKafka <bootstrap-servers>")
      System.exit(1)
    }

    val query = new ConsumeKafka(args(0))
    query.readAvro()
  }
}
