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
package totem.middleground.kafka

// import org.apache.spark.sql.Dataset
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.Trigger

class Query (bootstrap: String)
{
  def joinall(): Unit = {

    val spark = SparkSession.builder()
      .appName("Join All")
      .getOrCreate()

    val user = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", bootstrap)
      .option("subscribe", KafkaSchema.userTopics)
      .option("startingOffsets", "earliest")
      .load().select(from_json(col("value").
        cast("string"), KafkaSchema.userschema).as("user"), col("timestamp").as("u_ts"))
      .selectExpr("user.*", "u_ts").where("u_n_id >= 2")
      .withWatermark("u_ts", "20 seconds")


    val nation = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", bootstrap)
      .option("subscribe", KafkaSchema.nationTopics)
      .option("startingOffsets", "earliest")
      .load()
      .select(from_json(col("value").
        cast("string"), KafkaSchema.nationschema).as("nation"), col("timestamp").as("n_ts"))
      .selectExpr("nation.*", "n_ts").withWatermark("n_ts", "10 seconds")


    val joined_result = user.join(nation,
      expr(
        """
          u_n_id = n_id AND
          u_ts >= n_ts AND
          u_ts <= n_ts + interval 2 seconds
          """),
      joinType = "rightOuter"
    )

    val group_result = joined_result.groupBy(col("u_name")).count()
      // .select(col("u_name"), col("n_name"))

    val q = group_result
      .writeStream
      .outputMode("complete")
      .format("console")
      .trigger(Trigger.ProcessingTime("2 seconds"))
      .start()

    q.awaitTermination()



    /*
    val region = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", bootstrap)
      .option("subscribe", KafkaSchema.regionTopics)
      .load()
      .select(from_json(col("value").cast("string"), KafkaSchema.regionschema).as("region"), col("timestamp"))
      .selectExpr("region.*", "timestamp")
    */



  }
}

object QueryKafka {
  def main(args: Array[String]): Unit = {

    if (args.length < 1) {
      System.err.println("Usage: QueryKafka <bootstrap-servers>")
      System.exit(1)
    }

    val query = new Query(args(0))
    query.joinall();
  }
}

