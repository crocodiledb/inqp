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

package totem.middleground.tpch.unittest

import totem.middleground.tpch.DataUtils

import org.apache.spark.sql.SparkSession

// scalastyle:off println

class AggJoinMixTest (bootstrap: String, query: String) {
  DataUtils.bootstrap = bootstrap

  def execQuery(query: String): Unit = {
    val spark = SparkSession.builder()
      .appName("Executing Query " + query)
      .getOrCreate()

    query.toLowerCase match {
      case "q17" =>
        execQ17(spark)
      case _ =>
        printf("AggJoin test does not supported %s yet\n", query)
    }
  }

  def execQ17(spark: SparkSession): Unit = {
    import spark.implicits._

    val avgQuantity = new AvgQuantity
    val sumExtendedprice = new SumExtendedprice

    // val l = DataUtils.loadStreamTable(spark, "lineitem", "l")
    // val p = DataUtils.loadStreamTable(spark, "part", "p")
    //   .filter($"p_brand" === "Brand#23" and $"p_container" === "MED BOX")

    val agg_l = DataUtils.loadStreamTable(spark, "lineitem", "l")
      .groupBy($"l_partkey")
      .agg((avgQuantity($"l_quantity") * 0.2).as("avg_quantity"))
      .select($"l_partkey".as("agg_l_partkey"), $"avg_quantity")

    // val result = l.join(p, $"l_partkey" === $"p_partkey")
    //   .join(agg_l, $"p_partkey" === $"agg_l_partkey" and
    //     $"l_quantity" < $"avg_quantity")
    //   .agg((sumExtendedprice($"l_extendedprice") / 7.0).as("avg_yearly"))

    DataUtils.writeToSink(agg_l)
  }
}

object AggJoinMixTest {
    def main(args: Array[String]): Unit = {

    if (args.length < 2) {
      System.err.println("Usage: AggJoinMixTest <bootstrap-servers> <query>")
      System.exit(1)
    }

    val aggJoinMixTest = new AggJoinMixTest(args(0), args(1))
    aggJoinMixTest.execQuery(args(1))
  }
}

// scalastyle:off println
