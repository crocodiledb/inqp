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

import org.apache.spark.sql.SparkSession

class KafkaStreamProducer (bootstrap: String)
{

  def ProduceUser(): Unit =
  {

    val spark = SparkSession.builder()
      .appName("Produce User")
      .getOrCreate()

    val rows = spark.readStream
        .format("csv")
        .option("sep", "|")
        .schema(KafkaSchema.userschema)
        .load(KafkaSchema.userPath)

    val query = rows.selectExpr("to_json(struct(*)) as value")
      .writeStream
      .format("kafka")
      .option("topic", KafkaSchema.userTopics)
      .option("kafka.bootstrap.servers", bootstrap)
      .option("checkpointLocation", KafkaSchema.checkpointLocation + "/user")
      .start()

    query.awaitTermination()
  }

  def ProduceNation(): Unit =
  {
    val spark = SparkSession.builder()
      .appName("Produce Nation")
      .getOrCreate()

    val rows = spark.readStream
        .format("csv")
        .option("sep", "|")
        .schema(KafkaSchema.nationschema)
        .load(KafkaSchema.nationPath)

    val query = rows.selectExpr("to_json(struct(*)) as value")
      .writeStream
      .format("kafka")
      .option("topic", KafkaSchema.nationTopics)
      .option("kafka.bootstrap.servers", bootstrap)
      .option("checkpointLocation", KafkaSchema.checkpointLocation + "/nation")
      .start()

    query.awaitTermination()
  }

  def ProduceRegion(): Unit =
  {
    val spark = SparkSession.builder()
      .appName("Produce Region")
      .getOrCreate()

    val rows = spark.readStream
        .format("csv")
        .option("sep", "|")
        .schema(KafkaSchema.regionschema)
        .load(KafkaSchema.regionPath)

    val query = rows.selectExpr("to_json(struct(*)) as value")
      .writeStream
      .format("kafka")
      .option("topic", KafkaSchema.regionTopics)
      .option("kafka.bootstrap.servers", bootstrap)
      .option("checkpointLocation", KafkaSchema.checkpointLocation + "/region")
      .start()

    query.awaitTermination();
  }

}

class WritingThread(streamProducer: KafkaStreamProducer, tableName: String) extends Thread {

  override def run(): Unit = {
    if (tableName.toLowerCase() == "user") {
      streamProducer.ProduceUser()
    }
    else if (tableName.toLowerCase() == "nation") {
      streamProducer.ProduceNation()
    }
    else {
      streamProducer.ProduceRegion()
    }
  }
}

object WriteIntoKafka {

  def main(args: Array[String]): Unit = {
    if (args.length < 1) {
      System.err.println("Usage: WriteIntoKafka <bootstrap-servers>")
      System.exit(1)
    }

    val producer = new KafkaStreamProducer(args(0))

    val threadUser = new WritingThread(producer, "User")
    val threadNation = new WritingThread(producer, "Nation")
    val threadRegion = new WritingThread(producer, "Region")

    threadUser.start()
    threadNation.start()
    threadRegion.start()

    threadUser.join()
    threadNation.join()
    threadRegion.join()

  }

}
// scalastyle:off println