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
package totem.middleground.tpch

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.Trigger

class QueryTPCH (bootstrap: String, query: String)
{
  def loadStreamTable(spark: SparkSession, tableName: String, alias: String): DataFrame = {
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
      .outputMode("complete")
      .format("console")
      .trigger(Trigger.ProcessingTime("2 seconds"))
      .start()

    q.awaitTermination()
  }

  def execQuery(query: String): Unit = {
    val spark = SparkSession.builder()
      .appName("Executing Query " + query)
      .getOrCreate()

    query.toLowerCase match {
      case "q1" =>
        execQ1(spark)
      case "q2" =>
        execQ2(spark)
      case "q3" =>
        execQ3(spark)
      case "q4" =>
        execQ4(spark)
      case "q5" =>
        execQ5(spark)
      case "q6" =>
        execQ6(spark)
      case "q7" =>
        execQ7(spark)
      case "q8" =>
        execQ8(spark)
      case "q9" =>
        execQ9(spark)
      case "q10" =>
        execQ10(spark)
      case "q11" =>
        execQ11(spark)
      case "q12" =>
        execQ12(spark)
      case "q13" =>
        execQ13(spark)
      case "q14" =>
        execQ14(spark)
      case "q15" =>
        execQ15(spark)
      case "q16" =>
        execQ16(spark)
      case "q17" =>
        execQ17(spark)
      case "q18" =>
        execQ18(spark)
      case "q19" =>
        execQ19(spark)
      case "q20" =>
        execQ20(spark)
      case "q21" =>
        execQ21(spark)
      case "q22" =>
        execQ22(spark)
      case _ =>
        printf("Not yet supported %s", query)
    }
  }

  def execQ1(spark: SparkSession): Unit = {
    import spark.implicits._

    val sum_disc_price = new Q1_sum_disc_price

    val l = loadStreamTable(spark, "lineitem", "l")

    val result = l.filter($"l_shipdate" <= "1998-09-01")
      .select($"l_ts", $"l_returnflag", $"l_linestatus",
        $"l_quantity", $"l_extendedprice", $"l_discount", $"l_tax")
      .groupBy($"l_returnflag", $"l_linestatus")
      .agg(
        max($"l_quantity").as("max_qty"),
        sum_disc_price($"l_extendedprice", $"l_discount").as("sum_disc_price")
        // ,
        // sum($"l_extendedprice").as("sum_base_price"),
        // sum($"l_extendedprice" + 1).as("sum_disc_price"),
        // sum($"l_extendedprice" * ($"l_discount" - 1) * -1).as("sum_disc_price"),
        // sum($"l_extendedprice" * (($"l_discount" - 1) * -1) * ($"l_tax" + 1)).as("sum_charge"),
        // avg($"l_quantity").as("avg_qty"),
        // avg($"l_extendedprice").as("avg_price"),
        // avg($"l_discount").as("avg_disc"),
        // count($"*").as("count_order")
      )
      .filter($"l_returnflag" =!= "A")
      .agg(
        max($"max_qty").as("final_max_qty"),
        min($"sum_disc_price").as("final_sum_disc_price")
      )
      // .orderBy($"l_returnflag", $"l_linestatus")

    // result.explain(true)

    writeToSink(result)
  }

  def execQ2_subquery(spark: SparkSession): DataFrame = {
    import spark.implicits._

    val s = loadStreamTable(spark, "supplier", "s")
    val ps = loadStreamTable(spark, "partsupp", "ps")

    val n = loadStaticTable(spark, "nation", "n")
    val r = loadStaticTable(spark, "region", "r")
      .filter($"r_name" === "EUROPE")

    return r.join(n, $"r_regionkey" === $"n_regionkey")
      .join(s, $"n_nationkey" === $"s_nationkey")
      .join(ps, $"s_suppkey" === $"ps_suppkey")
      .groupBy($"ps_partkey")
      .agg(
        min($"ps_supplycost").as("min_supplycost"))
      .select($"ps_partkey".as("min_partkey"), $"min_supplycost")
  }

  def execQ2(spark: SparkSession): Unit = {
    import spark.implicits._

    val p = loadStreamTable(spark, "part", "p")
      .filter(($"p_size" === 15) and ($"p_type" like("%BRASS")))
    val s = loadStreamTable(spark, "supplier", "s")
    val ps = loadStreamTable(spark, "partsupp", "ps")
    val n = loadStaticTable(spark, "nation", "n")
    val r = loadStaticTable(spark, "region", "r")
      .filter($"r_name" === "EUROPE")

    val subquery1_a = r.join(n, $"r_regionkey" === $"n_regionkey")
      .join(s, $"n_nationkey" === $"s_nationkey")

    val subquery1_b = ps.join(p, $"ps_partkey" === $"p_partkey")
    val subquery1 = subquery1_a.join(subquery1_b, $"s_suppkey" === $"ps_suppkey")

    val subquery2 = execQ2_subquery(spark)

    val result = subquery1
      .join(subquery2, ($"p_partkey" ===  $"min_partkey")
        and ($"ps_supplycost" === $"min_supplycost"))
      .orderBy(desc("s_acctbal"), $"n_name", $"s_name", $"p_partkey")
      .select($"s_acctbal", $"s_name", $"n_name",
        $"p_partkey", $"p_mfgr", $"s_address", $"s_phone", $"s_comment")

    result.explain(true)
    // writeToSink(result)
  }

  def execQ3(spark: SparkSession): Unit = {
    import spark.implicits._

    val c = loadStreamTable(spark, "customer", "c")
      .filter($"c_mktsegment" === "BUILDING")
    val o = loadStreamTable(spark, "orders", "o")
      .filter($"o_orderdate" < "1995-03-15")
    val l = loadStreamTable(spark, "lineitem", "l")
      .filter($"l_shipdate" > "1995-03-15")

    val result = c.join(o, $"c_custkey" === $"o_custkey")
      .join(l, $"o_orderkey" === $"l_orderkey")
      .groupBy("l_orderkey", "o_orderdate", "o_shippriority")
      .agg(
       sum($"l_extendedprice" * ($"l_discount" - 1) * -1).alias("revenue"))
      .orderBy("revenue", "o_orderdate")
      // .select("l_orderkey", "revenue", "o_orderdate", "o_shippriority")

    // result.explain(false)

    writeToSink(result)
  }

  def execQ4(spark: SparkSession): Unit = {
    import spark.implicits._

    val o = loadStreamTable(spark, "orders", "o")
      .filter($"o_orderdate" >= "1993-07-01"
      and $"o_orderdate" < "1993-10-01")

    val l = loadStreamTable(spark, "lineitem", "l")
      .filter($"l_commitdate" < $"l_receiptdate")
      .select("l_orderkey")

    val result = o.join(l, $"o_orderkey" === $"l_orderkey", "left_semi")
      .groupBy("o_orderpriority")
      .agg(
        count($"*").alias("order_count"))
      .orderBy("o_orderpriority")

    result.explain(false)

    // writeToSink(result)
  }

  def execQ5(spark: SparkSession): Unit = {
    import spark.implicits._

    val c = loadStreamTable(spark, "customer", "c")
    val o = loadStreamTable(spark, "orders", "o")
      .filter($"o_orderdate" >= "1994-01-01" and $"o_orderdate" < "1995-01-01")
    val l = loadStreamTable(spark, "lineitem", "l")
    val s = loadStreamTable(spark, "supplier", "s")

    val n = loadStaticTable(spark, "nation", "n")
    val r = loadStaticTable(spark, "region", "r")
      .filter($"r_name" === "ASIA")

    val query_a = r.join(n, $"r_regionkey" === $"n_regionkey")
      .join(s, $"n_nationkey" === $"s_nationkey")

    val query_b = l.join(o, $"l_orderkey" === $"o_orderkey")
      .join(c, $"o_custkey" === $"c_custkey")

    val result = query_a.join(query_b, $"s_nationkey" === $"c_nationkey")
      .groupBy("n_name")
      .agg(
        sum($"l_extendedprice" * ($"l_discount" - 1) * -1).alias("revenue"))
      .orderBy("revenue")

    result.explain(true)

    // writeToSink(result)
  }

  def execQ6(spark: SparkSession): Unit = {
    import spark.implicits._

    val l = loadStreamTable(spark, "lineitem", "l")
      .filter(($"l_shipdate" between("1994-01-01", "1995-01-01"))
        and ($"l_discount" between(0.05, 0.07)) and ($"l_quantity" < 24))

    val result = l.agg(
      sum($"l_extendedprice" * $"l_discount").alias("revenue")
    )

    result.explain(true)
    // writeToSink(result)
  }

  def execQ7(spark: SparkSession): Unit = {
    import spark.implicits._

    val l = loadStreamTable(spark, "lineitem", "l")
      .filter($"l_shipdate" between("1995-01-01", "1996-12-31"))
    val s = loadStreamTable(spark, "supplier", "s")
    val o = loadStreamTable(spark, "orders", "o")
    val c = loadStreamTable(spark, "customer", "c")
    val n1 = loadStaticTable(spark, "nation", "n1")
      .select($"n_name".alias("supp_nation"), $"n_nationkey".as("n1_nationkey"))
    val n2 = loadStaticTable(spark, "nation", "n2")
      .select($"n_name".alias("cust_nation"), $"n_nationkey".as("n2_nationkey"))

    val result = l.join(s, $"l_suppkey" === $"s_suppkey")
      .join(o, $"l_orderkey" === $"o_orderkey")
      .join(c, $"o_custkey" === $"c_custkey")
      .join(n1, $"s_nationkey" === $"n1_nationkey")
      .join(n2, $"c_nationkey" === $"n2_nationkey")
      .filter(($"supp_nation" === "FRANCE" and $"cust_nation" === "GERMANY")
        or ($"supp_nation" === "GERMANY" and $"cust_nation" === "FRANCE"))
      .select($"supp_nation", $"cust_nation", year($"l_shipdate").as("l_year"),
        $"l_extendedprice", $"l_discount")
      .groupBy("supp_nation", "cust_nation", "l_year")
      .agg(
        sum($"l_extendedprice" * ($"l_discount" - 1) * -1).as("revenue")
      )
      .orderBy("supp_nation", "cust_nation", "l_year")

    result.explain(true)
    // writeToSink(result)
  }

  def execQ8(spark: SparkSession): Unit = {
    import spark.implicits._

    val udaf_q8 = new UDAF_Q8

    val p = loadStreamTable(spark, "part", "p")
      .filter($"p_type" === "ECONOMY ANODIZED STEEL")
    val s = loadStreamTable(spark, "supplier", "s")
    val l = loadStreamTable(spark, "lineitem", "l")
    val o = loadStreamTable(spark, "orders", "o")
      .filter($"o_orderdate" between("1995-01-01", "1996-12-31"))
    val c = loadStreamTable(spark, "customer", "c")
    val n1 = loadStaticTable(spark, "nation", "n1")
      .select($"n_regionkey".alias("n1_regionkey"), $"n_nationkey".as("n1_nationkey"))
    val n2 = loadStaticTable(spark, "nation", "n2")
      .select($"n_name".alias("n2_name"), $"n_nationkey".as("n2_nationkey"))
    val r = loadStaticTable(spark, "region", "r")
      .filter($"r_name" === "AMERICA")

    val result = l.join(p, $"l_partkey" === $"p_partkey")
      .join(s, $"l_suppkey" === $"s_suppkey")
      .join(o, $"l_orderkey" === $"o_orderkey")
      .join(c, $"o_custkey" === $"c_custkey")
      .join(n1, $"c_nationkey" === $"n1_nationkey")
      .join(r, $"n1_regionkey" === $"r_regionkey")
      .join(n2, $"s_nationkey" === $"n2_nationkey")
      .select(year($"o_orderdate").as("o_year"),
        ($"l_extendedprice" * ($"l_discount" - 1) * -1).as("volume"), $"n2_name")
      .groupBy($"o_year")
      .agg(udaf_q8($"n2_name", $"volume"))
      .orderBy($"o_year")

    result.explain(true)
    // writeToSink(result)
  }

  def execQ9(spark: SparkSession): Unit = {
    import spark.implicits._

    val p = loadStreamTable(spark, "part", "p")
      .filter($"p_name" like("%green%"))
    val s = loadStreamTable(spark, "supplier", "s")
    val l = loadStreamTable(spark, "lineitem", "l")
    val ps = loadStreamTable(spark, "partsupp", "ps")
    val o = loadStreamTable(spark, "orders", "o")
    val n = loadStaticTable(spark, "nation", "n")

    val result = l.join(p, $"l_partkey" === $"p_partkey")
      .join(ps, $"l_partkey" === $"ps_partkey" and $"l_suppkey" === $"ps_suppkey")
      .join(s, $"l_suppkey" === $"s_suppkey")
      .join(o, $"l_orderkey" === $"o_orderkey")
      .join(n, $"s_nationkey" === $"n_nationkey")
      .select($"n_name".as("nation"),
        year($"o_orderdate").as("o_year"),
        (($"l_extendedprice" * ($"l_discount" - 1) * -1) - $"ps_supplycost" * $"l_quantity")
          .as("amount"))
      .groupBy("nation", "o_year")
      .agg(
        sum($"amount").as("sum_profit"))
      .orderBy($"nation", desc("o_year"))

    result.explain(true)
    // writeToSink(result)
  }

  def execQ10(spark: SparkSession): Unit = {
    import spark.implicits._

    val c = loadStreamTable(spark, "customer", "c")
    val o = loadStreamTable(spark, "orders", "o")
      .filter($"o_orderdate" >= "1993-10-01" and $"o_orderdate" < "1994-01-01")
    val l = loadStreamTable(spark, "lineitem", "l")
      .filter($"l_returnflag" === "R")
    val n = loadStaticTable(spark, "nation", "n")

    val result = l.join(o, $"l_orderkey" === $"o_orderkey")
      .join(c, $"o_custkey" === $"c_custkey")
      .join(n, $"c_nationkey" === $"n_nationkey")
      .groupBy("c_custkey", "c_name", "c_acctbal", "c_phone", "n_name", "c_address", "c_comment")
      .agg(
        sum($"l_extendedprice" * ($"l_discount" - 1) * -1).as("revenue"))
      .orderBy(desc("revenue"))

    result.explain(true)
      // writeToSink(result)
  }

  def execQ11_subquery(spark: SparkSession): DataFrame = {
    import spark.implicits._

    val ps = loadStreamTable(spark, "partsupp", "ps")
    val s = loadStreamTable(spark, "supplier", "s")
    val n = loadStaticTable(spark, "nation", "n")
      .filter($"n_name" === "GERMANY")

    return s.join(n, $"s_nationkey" === $"n_nationkey")
      .join(ps, $"s_suppkey" === $"ps_suppkey")
      .agg(
        sum($"ps_supplycost" * $"ps_availqty" * 0.0001).as("small_value"))
  }

  def execQ11(spark: SparkSession): Unit = {
    import spark.implicits._

    val ps = loadStreamTable(spark, "partsupp", "ps")
    val s = loadStreamTable(spark, "supplier", "s")
    val n = loadStaticTable(spark, "nation", "n")
      .filter($"n_name" === "GERMANY")

    val subquery = execQ11_subquery(spark);

    val result = s.join(n, $"s_nationkey" === $"n_nationkey")
      .join(ps, $"s_suppkey" === $"ps_suppkey")
      .groupBy($"ps_partkey")
      .agg(
        sum($"ps_supplycost" * $"ps_availqty").as("value"))
      .join(subquery, $"value" > $"small_value", "cross")
      .select($"ps_partkey", $"value")
      .orderBy(desc("value"))

    result.explain(true)
    // writeToSink(result)
  }

  def execQ12(spark: SparkSession): Unit = {
    import spark.implicits._

    val udaf_q12_low = new UDAF_Q12_LOW
    val udaf_q12_high = new UDAF_Q12_HIGH

    val o = loadStreamTable(spark, "orders", "o")
    val l = loadStreamTable(spark, "lineitem", "l")
      .filter(($"l_shipmode" === "MAIL")
        and ($"l_commitdate" < $"l_receiptdate")
        and ($"l_shipdate" < $"l_commitdate")
        and ($"l_receiptdate" === "1994-01-01"))

    val result = o.join(l, $"o_orderkey" === $"l_orderkey" and $"o_comment" < $"l_comment")
      .groupBy($"l_shipmode")
      .agg(
          udaf_q12_high($"o_orderpriority").as("high_line_count"),
          udaf_q12_low($"o_orderpriority").as("low_line_count"))

      // .orderBy($"l_shipmode")

    // result.explain(true)

    writeToSink(result)
  }

  def execQ13(spark: SparkSession): Unit = {
    import spark.implicits._
    val c = loadStreamTable(spark, "customer", "c")
    val o = loadStreamTable(spark, "orders", "o")
      .filter(!($"o_comment" like("%special%requests%")))

    val result = c.join(o, $"c_custkey" === $"o_custkey", "left_outer")
      .groupBy($"c_custkey")
      .agg(
        count($"o_orderkey").as("c_count"))
      .groupBy($"c_count")
      .agg(
        count("*").as("custdist"))
      .orderBy(desc("custdist"), desc("c_count"))

    result.explain(true)
    // writeToSink(result)
  }

  def execQ14(spark: SparkSession): Unit = {
    import spark.implicits._

    val udaf_q14 = new UDAF_Q14
    val l = loadStreamTable(spark, "lineitem", "l")
      .filter($"l_shipdate" between("1995-09-01", "1995-10-01"))
    val p = loadStreamTable(spark, "part", "p")

    val result = l.join(p, $"l_partkey" === $"p_partkey")
      .agg(
        udaf_q14($"p_type", $"l_extendedprice", $"l_discount")/
        sum($"l_extendedprice" * ($"l_discount" - 1) * -1))

    result.explain(true)
    // writeToSink(result)
  }

  def execQ15_subquery(spark: SparkSession): DataFrame = {
    import  spark.implicits._

    val l = loadStreamTable(spark, "lineitem", "l")
      .filter($"l_shipdate" between("1996-01-01", "1996-04-01"))

    return l.groupBy($"l_suppkey")
      .agg(
       sum($"l_extendedprice" * ($"l_discount" - 1) * -1)).as("total_revenue")
      .select($"l_suppkey".as("supplier_no"), max($"total_revenue").as("max_revenue"))
  }

  def execQ15(spark: SparkSession): Unit = {
    import spark.implicits._

    val s = loadStreamTable(spark, "supplier", "s")
    val revenue = execQ15_subquery(spark)

    val result = s.join(revenue, $"s_suppkey" === $"supplier_no")
      .select("s_suppkey", "s_name", "s_address", "s_phone", "max_revenue")
      .orderBy("s_suppkey")

    result.explain(true)
    // writeToSink(result)
  }

  def execQ16(spark: SparkSession): Unit = {
    import spark.implicits._

    val ps = loadStreamTable(spark, "partsupp", "ps")
    val p = loadStreamTable(spark, "part", "part")
      .filter(($"p_brand" =!= "Brand#45") and
        (!($"p_type" like("MEDIUM POLISHED")))
        and ($"p_size" isin(49, 14, 23, 45, 19, 3, 36, 9)))

    val s = loadStreamTable(spark, "supplier", "s")
      .filter($"s_comment" like("%Customer%Complaints%"))
      .select($"s_suppkey")

    val result = ps.join(p, $"ps_partkey" === $"p_partkey")
      .join(s, $"ps_suppkey" === $"s_suppkey", "left_anti")
      .groupBy($"p_brand", $"p_type", $"p_size")
      .agg(
        countDistinct($"ps_suppkey").as("supplier_cnt"))
      .orderBy(desc("supplier_cnt"), $"p_brand", $"p_type", $"p_size")

    result.explain(true)
    // writeToSink(result)
  }

  def execQ17(spark: SparkSession): Unit = {
    import spark.implicits._

    val l = loadStreamTable(spark, "lineitem", "l")
    val p = loadStreamTable(spark, "part", "p")
      .filter($"p_brand" === "Brand#23" and $"p_container" === "MED BOX")

    val agg_l = loadStreamTable(spark, "lineitem", "l")
      .groupBy($"l_partkey")
      .agg(
        (avg($"l_quantity") * 0.2).as("avg_quantity"))
      .select($"l_partkey".as("agg_l_partkey"), $"avg_quantity")

    val result = l.join(p, $"l_partkey" === $"p_partkey")
      .join(agg_l, $"p_partkey" === $"agg_l_partkey" and
        $"l_quantity" < $"avg_quantity", "left_semi")
      .agg(
        (sum($"l_extendedprice") / 7.0).as("avg_yearly"))

    result.explain(true)
    // writeToSink(result)
  }

  def execQ18(spark: SparkSession): Unit = {
    import spark.implicits._

    val c = loadStreamTable(spark, "customer", "c")
    val o = loadStreamTable(spark, "orders", "o")
    val l = loadStreamTable(spark, "lineitem", "l")
    val agg_l = loadStreamTable(spark, "lineitem", "l")
      .groupBy("l_orderkey")
      .agg(sum("l_quantity").as("sum_quantity"))
      .filter($"sum_quantity" > 300)
      .select($"l_orderkey".as("agg_orderkey"))

    val result = l.join(agg_l, $"l_orderkey" === $"agg_orderkey", "left_semi")
      .join(o, $"l_orderkey" === $"o_orderkey")
      .join(c, $"o_custkey" === $"c_custkey")
      .groupBy("c_name", "c_custkey", "o_orderkey", "o_orderdate", "o_totalprice")
      .sum("l_quantity")
      .orderBy(desc("o_totalprice"), $"o_orderdate")

    result.explain(true)
    // writeToSink(result)
  }

  def execQ19(spark: SparkSession): Unit = {
    import spark.implicits._

    val l = loadStreamTable(spark, "lineitem", "l")
      .filter(($"l_shipmode" isin("AIR", "AIR REG"))
        and ($"l_shipinstruct" === "DELIVER IN PERSON"))
    val p = loadStreamTable(spark, "part", "p")

    val result = l.join(p, $"l_partkey" === $"p_partkey"
      and ((($"p_brand" === "Brand#12") and
      ($"p_container" isin("SM CASE", "SM BOX", "SM PACK", "SM PKG")) and
      ($"l_quantity" >= 1 and $"l_quantity" <= 11) and
      ($"p_size" between(1, 5))
       )
       or (($"p_brand" === "Brand#23") and
      ($"p_container" isin("MED BAG", "MED BOX", "MED PKG", "MED PACK")) and
      ($"l_quantity" >= 10 and $"l_quantity" <= 20) and
      ($"p_size" between(1, 5))
       )
       or (($"p_brand" === "Brand#34") and
      ($"p_container" isin("LG CASE", "LG BOX", "LG PACK", "LG PKG")) and
      ($"l_quantity" >= 20 and $"l_quantity" <= 30) and
      ($"p_size" between(1, 15))))
      )
      .agg(sum($"l_extendedprice" * ($"l_discount" - 1) * -1)).as("revenue")

    result.explain(true)
    // writeToSink(result)
  }

  def execQ20(spark: SparkSession): Unit = {
    import spark.implicits._

    val agg_l = loadStreamTable(spark, "lineitem", "l")
      .filter($"l_shipdate" between("1994-01-01", "1994-12-31"))
      .groupBy($"l_partkey", $"l_suppkey")
      .agg((sum($"l_quantity") * 0.5).as("agg_l_sum"))
      .select($"l_partkey".as("agg_l_partkey"),
      $"l_suppkey".as("agg_l_suppkey"),
      $"agg_l_sum")

    val p = loadStreamTable(spark, "part", "p")
      .filter($"p_name" like("forest%"))

    val ps = loadStreamTable(spark, "partsupp", "ps")

    val subquery = ps.join(p, $"ps_partkey" === $"p_partkey", "left_semi")
      .join(agg_l, $"ps_partkey" === $"agg_l_partkey"
        and $"ps_suppkey" === $"agg_l_suppkey" and $"ps_availqty" > $"agg_l_sum", "left_semi")
      .select("ps_suppkey")

    val s = loadStreamTable(spark, "supplier", "s")
    val n = loadStaticTable(spark, "nation", "n")
      .filter($"n_name" === "CANADA")

    val result = s.join(n, $"s_nationkey" === $"n_nationkey")
      .join(subquery, $"s_suppkey" === $"ps_suppkey", "left_semi")

    result.explain(true)
    // writeToSink(result)
  }

  def execQ21(spark: SparkSession): Unit = {
    import spark.implicits._

    val s = loadStreamTable(spark, "supplier", "s")
    val l1 = loadStreamTable(spark, "lineitem", "l1")
      .filter($"l_receiptdate" > $"l_commitdate")
    val o = loadStreamTable(spark, "orders", "o")
      .filter($"o_orderstatus" === "F")
    val n = loadStaticTable(spark, "nation", "n")
      .filter($"n_name" === "SAUDI ARABIA")

    val init_result = l1.join(o, $"l_orderkey" === $"o_orderkey")
      .join(s, $"l_suppkey" === $"s_suppkey")
      .join(n, $"s_nationkey" === $"n_nationkey")

    val l2 = loadStreamTable(spark, "lineitem", "l2")
      .select($"l_orderkey".as("l2_orderkey"),
      $"l_suppkey".as("l2_suppkey"))
    val l3 = loadStreamTable(spark, "lineitem", "l3")
      .filter($"l_receiptdate" > $"l_commitdate")
      .select($"l_orderkey".as("l3_orderkey"),
      $"l_suppkey".as("l3_suppkey"))

    val result = init_result.join(l2, ($"l_orderkey" === $"l2_orderkey")
      and ($"l_suppkey" =!= $"l2_suppkey"), "left_semi")
      .join(l3, ($"l_orderkey" === $"l3_orderkey")
        and ($"l_suppkey" =!= $"l3_suppkey"), "left_anti")
      .groupBy("s_name")
      .agg(count($"*").as("numwait"))
      .orderBy(desc("numwait"), $"s_name")

    result.explain(true)
    // writeToSink(result)
  }

  def execQ22(spark: SparkSession): Unit = {
    import spark.implicits._

    val c = loadStreamTable(spark, "customer", "c")
      .filter(substring($"c_phone", 1, 2)
        isin("13", "31", "23", "29", "30", "18", "17"))

    val subquery1 = loadStreamTable(spark, "customer", "c1")
      .filter((substring($"c_phone", 1, 2)
        isin("13", "31", "23", "29", "30", "18", "17")) and
        ($"c_acctbal" > 0.00))
      .agg(avg($"c_acctbal").as("avg_acctbal"))

    val o = loadStreamTable(spark, "orders", "o")

    val result = c.join(subquery1, $"c_acctbal" > $"avg_acctbal", "cross")
      .join(o, $"c_custkey" === $"o_custkey", "left_anti")
      .select(substring($"c_phone", 1, 2).as("cntrycode"), $"c_acctbal")
      .groupBy($"cntrycode")
      .agg(count($"*").as("numcust"), sum($"c_acctbal").as("totalacctbal") )
      .orderBy($"cntrycode")

    result.explain(true)
    // writeToSink(result)
  }
}

object QueryTPCH {
  def main(args: Array[String]): Unit = {

    if (args.length < 2) {
      System.err.println("Usage: QueryTPCH <bootstrap-servers> <query>")
      System.exit(1)
    }

    val tpch = new QueryTPCH(args(0), args(1))
    tpch.execQuery(args(1))
  }
}

// scalastyle:off println
