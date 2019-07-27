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
import org.apache.spark.sql.avro.to_avro
import org.apache.spark.sql.functions._

class ProduceKafka(bootstrap: String) {

  def convertToAvro(): Unit = {
    val spark = SparkSession.builder()
      .appName("Produce Avro")
      .getOrCreate()

    val rows = spark.readStream
        .format("csv")
        .option("sep", "|")
        .schema(KafkaMeta.userschema)
        .load(KafkaMeta.userPath)

    val query = rows.select(to_avro(struct("*")) as "value")
      .writeStream
      .format("kafka")
      .option("topic", KafkaMeta.userTopics)
      .option("kafka.bootstrap.servers", bootstrap)
      .option("checkpointLocation", KafkaMeta.checkpointLocation + "/user")
      .start()

    query.awaitTermination()
  }
}

object ProduceKafka {
  def main(args: Array[String]): Unit = {

    if (args.length < 1) {
      System.err.println("Usage: QueryKafka <bootstrap-servers>")
      System.exit(1)
    }

    val query = new ProduceKafka(args(0))
    query.convertToAvro()
  }
}
