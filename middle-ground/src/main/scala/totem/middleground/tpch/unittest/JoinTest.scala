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

class JoinTest (bootstrap: String, query: String) {
  DataUtils.bootstrap = bootstrap

  def execQuery(query: String): Unit = {
    val spark = SparkSession.builder()
      .appName("Executing Query " + query)
      .getOrCreate()

    query.toLowerCase match {
      case "q_innerjoin" =>
        execInnerJoin(spark)
      case "q_leftouterjoin" | "q_rightouterjoin" |
           "q_fullouterjoin" =>
        execOuterJoin(spark)
      case "q_semijoin" | "q_antijoin" =>
        execSemiAntiJoin(spark)
      case _ =>
        printf("Join test does not supported %s yet\n", query)
    }
  }

  def execInnerJoin(spark: SparkSession): Unit = {
    import spark.implicits._

    val o = DataUtils.loadStreamTable(spark, "orders", "o")
          .filter($"o_orderkey" isin(75233, 258050, 261607, 272002,
        348353, 446496, 465634, 529796, 554244, 535873))
    val l = DataUtils.loadStreamTable(spark, "lineitem", "l")
      .filter(($"l_shipmode" === "MAIL")
        and ($"l_commitdate" < $"l_receiptdate")
        and ($"l_shipdate" < $"l_commitdate")
        and ($"l_receiptdate" === "1994-01-01"))

    val result =
      // o.join(l, $"o_orderkey" === $"l_orderkey" and $"o_comment" < $"l_comment")
      o.join(l, $"o_orderkey" === $"l_orderkey")
          .select($"o_orderkey", $"o_orderpriority", $"l_orderkey")

    DataUtils.writeToSink(result)
  }

  def execOuterJoin(spark: SparkSession): Unit = {
    import spark.implicits._

    val o = DataUtils.loadStreamTable(spark, "orders", "o")
      // .filter($"o_orderkey" isin(75233, 258050, 261607, 272002,
      //  348353, 446496, 465634, 529796, 554244, 535873))
      .filter($"o_orderkey" isin(261607, 272002, 348353, 446496,
      465634, 529796, 554244, 535873))

    val l = DataUtils.loadStreamTable(spark, "lineitem", "l")
      .filter(($"l_shipmode" === "MAIL")
        and ($"l_commitdate" < $"l_receiptdate")
        and ($"l_shipdate" < $"l_commitdate")
        and ($"l_receiptdate" === "1994-01-01"))

    val result =
      // o.join(l, $"o_orderkey" === $"l_orderkey", "left_outer")
      // o.join(l, $"o_orderkey" === $"l_orderkey", "right_outer")
       o.join(l, $"o_orderkey" === $"l_orderkey", "full_outer")
       .select("o_orderkey", "l_orderkey", "o_orderpriority")

    DataUtils.writeToSink(result)
  }

  def execSemiAntiJoin(spark: SparkSession): Unit = {
    import spark.implicits._

    val o = DataUtils.loadStreamTable(spark, "orders", "o")
      .filter($"o_orderkey" isin(10, 261607, 272002, 348353, 446496,
        465634, 529796, 554244, 535873))

    val l = DataUtils.loadStreamTable(spark, "lineitem", "l")
      .filter($"l_orderkey" isin( 446496,
        465634, 529796, 554244, 535873))

    val result =
      // o.join(l, $"o_orderkey" === $"l_orderkey", "left_semi")
      o.join(l, $"o_orderkey" === $"l_orderkey", "left_anti")
      .select("o_orderkey", "o_orderpriority")

    DataUtils.writeToSink(result)
  }
}

object JoinTest {
    def main(args: Array[String]): Unit = {

    if (args.length < 2) {
      System.err.println("Usage: JoinTest <bootstrap-servers> <query>")
      System.exit(1)
    }

    val joinTest = new JoinTest(args(0), args(1))
    joinTest.execQuery(args(1))
  }
}

// scalastyle:off println
