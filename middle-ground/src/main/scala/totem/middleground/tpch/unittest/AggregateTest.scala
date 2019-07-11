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
import org.apache.spark.sql.functions._

// scalastyle:off println

class AggregateTest (bootstrap: String, query: String) {
    DataUtils.bootstrap = bootstrap

  def execQuery(query: String): Unit = {
    val spark = SparkSession.builder()
      .appName("Executing Query " + query)
      .getOrCreate()

    query.toLowerCase match {
      case "q_singleagg" =>
        execSingleAgg(spark)
      case "q_complexagg" =>
        execComplexAgg(spark)
      case _ =>
        printf("Aggtest does not supported %s yet\n", query)
    }
  }

  // A single aggregation with max/sum
  def execSingleAgg(spark: SparkSession): Unit = {
    import spark.implicits._

    val sum_disc_price = new Q1_sum_disc_price

    val l = DataUtils.loadStreamTable(spark, "lineitem", "l")

    val result = l.filter($"l_shipdate" <= "1998-09-01")
      .select($"l_ts", $"l_returnflag", $"l_linestatus",
        $"l_quantity", $"l_extendedprice", $"l_discount", $"l_tax")
      .groupBy($"l_returnflag", $"l_linestatus")
      .agg(
        max($"l_quantity").as("max_qty"),
        min($"l_quantity").as("min_qty"),
        sum_disc_price($"l_extendedprice", $"l_discount").as("sum_disc_price")
      ).orderBy($"l_returnflag", $"l_linestatus")
      .dropDuplicates()

    result.explain()
    // DataUtils.writeToSink(result)
  }

  // Two cascading aggregation with predicates
  // Note that the second receives deletes and has not group by
  def execComplexAgg(spark: SparkSession): Unit = {
    import spark.implicits._

    val sum_disc_price = new Q1_sum_disc_price

    val l = DataUtils.loadStreamTable(spark, "lineitem", "l")

    val result = l.filter($"l_shipdate" <= "1998-09-01")
      .select($"l_ts", $"l_returnflag", $"l_linestatus",
        $"l_quantity", $"l_extendedprice", $"l_discount", $"l_tax")
      .groupBy($"l_returnflag", $"l_linestatus")
      .agg(
        max($"l_quantity").as("max_qty"),
        sum_disc_price($"l_extendedprice", $"l_discount").as("sum_disc_price")
      )
      .filter($"l_linestatus" === "F" and $"sum_disc_price" < 5e9)
      // .filter($"l_returnflag" =!= "R" and $"sum_disc_price" > 5e9)
      // .groupBy($"l_linestatus")
      .agg(
        max($"max_qty").as("final_max_qty"),
        max($"sum_disc_price").as("final_sum_disc_price")
      )

    DataUtils.writeToSink(result)
  }
}

object AggregateTest {
    def main(args: Array[String]): Unit = {

    if (args.length < 2) {
      System.err.println("Usage: AggregateTest <bootstrap-servers> <query>")
      System.exit(1)
    }

    val aggTest = new AggregateTest(args(0), args(1))
    aggTest.execQuery(args(1))
  }
}

// scalastyle:off println
