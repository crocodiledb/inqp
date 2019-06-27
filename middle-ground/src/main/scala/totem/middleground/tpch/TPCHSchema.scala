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

import org.apache.spark.sql.types.StructType

object TPCHSchema {
  val supplierTopics = "Supplier"
  val partTopics = "Part"
  val partsuppTopics = "PartSupp"
  val customerTopics = "Customer"
  val ordersTopics = "Orders"
  val lineitemTopics = "Lineitem"

  val checkpointLocation = "hdfs://localhost:9000/tpch_checkpoint"

  val supplierSchema = new StructType().add("s_suppkey", "long")
    .add("s_name", "string")
    .add("s_address", "string")
    .add("s_nationkey", "long")
    .add("s_phone", "string")
    .add("s_acctbal", "double")
    .add("s_comment", "string")

  val partsuppSchema = new StructType().add("ps_partkey", "long")
    .add("ps_suppkey", "long")
    .add("ps_availqty", "int")
    .add("ps_supplycost", "double")
    .add("ps_comment", "string")

  val partSchema = new StructType().add("p_partkey", "long")
    .add("p_name", "string")
    .add("p_mfgr", "string")
    .add("p_brand", "string")
    .add("p_type", "string")
    .add("p_size", "int")
    .add("p_container", "string")
    .add("p_retailprice", "double")
    .add("p_comment", "string")

  val customerSchema = new StructType().add("c_custkey", "long")
    .add("c_name", "string")
    .add("c_address", "string")
    .add("c_nationkey", "long")
    .add("c_phone", "string")
    .add("c_acctbal", "double")
    .add("c_mktsegment", "string")
    .add("c_comment", "string")

  val ordersSchema = new StructType().add("o_orderkey", "long")
    .add("o_custkey", "long")
    .add("o_orderstatus", "string")
    .add("o_totalprice", "double")
    .add("o_orderdate", "date")
    .add("o_orderpriority", "string")
    .add("o_clerk", "string")
    .add("o_shippriority", "int")
    .add("o_comment", "string")

  val lineitemSchema = new StructType().add("l_orderkey", "long")
    .add("l_partkey", "long")
    .add("l_suppkey", "long")
    .add("l_linenumber", "int")
    .add("l_quantity", "double")
    .add("l_extendedprice", "double")
    .add("l_discount", "double")
    .add("l_tax", "double")
    .add("l_returnflag", "string")
    .add("l_linestatus", "string")
    .add("l_shipdate", "date")
    .add("l_commitdate", "date")
    .add("l_receiptdate", "date")
    .add("l_shipinstruct", "string")
    .add("l_shipmode", "string")
    .add("l_comment", "string")

  val nationSchema = new StructType().add("n_nationkey", "long")
    .add("n_name", "string")
    .add("n_regionkey", "long")
    .add("n_comment", "string")

  val regionSchema = new StructType().add("r_regionkey", "long")
    .add("r_name", "string")
    .add("r_comment", "string")

  val slothdb_testroot = "/home/totemtang/slothdb/slothdb_testsuite"
  val datadir = slothdb_testroot + "/datadir/tpchdata"
  val supplierPath = datadir + "/supplier"
  val partPath = datadir + "/part"
  val partsuppPath = datadir + "/partsupp"
  val customerPath = datadir + "/customer"
  val ordersPath = datadir + "/orders"
  val lineitemPath = datadir + "/lineitem"

  val nationPath = "hdfs://localhost:9000/tpch_data/nation.tbl"
  val regionPath = "hdfs://localhost:9000/tpch_data/region.tbl"

  val numMiniBatch = 3
  val scaleFactor = 1

  val lineitemSize = 600000 * scaleFactor
  val ordersSize = 150000 * scaleFactor
  val customerSize = 15000 * scaleFactor
  val partSize = 20000 * scaleFactor
  val partsuppSize = 80000 * scaleFactor
  val supplierSize = 1000 * scaleFactor

  val lineitemOffset = (lineitemSize + numMiniBatch - 1) / numMiniBatch
  val supplierOffset = (supplierSize + numMiniBatch - 1) / numMiniBatch
  val partOffset = (partSize + numMiniBatch - 1) / numMiniBatch
  val partsuppOffset = (partsuppSize + numMiniBatch - 1) / numMiniBatch
  val customerOffset = (customerSize + numMiniBatch - 1) / numMiniBatch
  val ordersOffset = (ordersSize + numMiniBatch - 1) / numMiniBatch

  def GetMetaData(tableName: String) : Option[Tuple4[StructType, String, String, Long]] =
  {
    tableName.toLowerCase match {
      case "part" =>
        Some((partSchema, partPath, partTopics, partOffset))
      case "partsupp" =>
        Some((partsuppSchema, partsuppPath, partsuppTopics, partsuppOffset))
      case "supplier" =>
        Some((supplierSchema, supplierPath, supplierTopics, supplierOffset))
      case "customer" =>
        Some((customerSchema, customerPath, customerTopics, customerOffset))
      case "orders" =>
        Some((ordersSchema, ordersPath, ordersTopics, ordersOffset))
      case "lineitem" =>
        Some((lineitemSchema, lineitemPath, lineitemTopics, lineitemOffset))
      case "nation" =>
        Some((nationSchema, nationPath, null, 0L))
      case "region" =>
        Some((regionSchema, regionPath, null, 0L))
      case _ =>
        printf("Unrecoganized Table %s\n", tableName)
        throw new Exception("Unrecoganized Table")
    }
  }

}
