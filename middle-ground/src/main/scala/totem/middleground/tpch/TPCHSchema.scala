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

import org.apache.spark.sql.avro.SchemaConverters
import org.apache.spark.sql.types.StructType

object TPCHSchema {
  val supplierTopics = "Supplier"
  val partTopics = "Part"
  val partsuppTopics = "PartSupp"
  val customerTopics = "Customer"
  val ordersTopics = "Orders"
  val lineitemTopics = "Lineitem"
  val nationTopics = "Nation"
  val regionTopics = "Region"

  val largeSupplierTopics = "Supplier-Large"
  val largePartTopics = "Part-Large"
  val largePartSuppTopics = "PartSupp-Large"
  val largeCustomerTopics = "Customer-Large"
  val largeOrdersTopics = "Orders-Large"

  var checkpointLocation = "hdfs://localhost:9000/tpch_checkpoint"
  var staticTableLocation = "hdfs://localhost:9000/tpch_static"

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

  val avroLineitemSchema = SchemaConverters.toAvroType(lineitemSchema)
  val avroOrdersSchema = SchemaConverters.toAvroType(ordersSchema)
  val avroCustomerSchema = SchemaConverters.toAvroType(customerSchema)
  val avroSupplierSchema = SchemaConverters.toAvroType(supplierSchema)
  val avroPartSuppSchema = SchemaConverters.toAvroType(partsuppSchema)
  val avroPartSchema = SchemaConverters.toAvroType(partSchema)
  val avroNationSchema = SchemaConverters.toAvroType(nationSchema)
  val avroRegionSchema = SchemaConverters.toAvroType(regionSchema)

  var datadir = "/home/totemtang/slothdb/slothdb_testsuite/datadir/tpchdata"
  private def supplierPath =
    if (!largeDataset) datadir + "/supplier"
    else datadir + "/supplier_large"
  private def partPath =
    if (!largeDataset) datadir + "/part"
    else datadir + "/part_large"
  private def partsuppPath =
    if (!largeDataset) datadir + "/partsupp"
    else datadir + "/partsupp_large"
  private def customerPath =
    if (!largeDataset) datadir + "/customer"
    else datadir + "/customer_large"
  private def ordersPath =
    if (!largeDataset) datadir + "/orders"
    else datadir + "/orders_large"
  private def lineitemPath = datadir + "/lineitem"
  private def nationPath = datadir  + "/nation"
  private def regionPath = datadir + "/region"

  private def nationStaticPath = staticTableLocation + "/nation.tbl"
  private def regionStaticPath = staticTableLocation + "/region.tbl"
  private val Empty_Static = ""

  // val nationPath = "hdfs://localhost:9000/tpch_data/nation.tbl"
  // val regionPath = "hdfs://localhost:9000/tpch_data/region.tbl"

  var numMiniBatch = 4
  var scaleFactor = 1.0
  var partitions = 1
  var largeDataset = false

  def lineitemSize: Int =
    if (scaleFactor == 10) 59986052
    else if (scaleFactor == 0.1) 600572
    else (6000000 * scaleFactor).toInt
  def ordersSize: Int = (1500000 * scaleFactor).toInt
  def customerSize: Int = (150000 * scaleFactor).toInt
  def partSize: Int = (200000 * scaleFactor).toInt
  def partsuppSize: Int = (800000 * scaleFactor).toInt
  def supplierSize: Int = (10000 * scaleFactor).toInt
  def nationSize: Int = 25
  def regionSize: Int = 5

  def getOffset(size: Int): Int = {
    val perPartition = (size + numMiniBatch*partitions - 1)/(numMiniBatch * partitions)
    perPartition*partitions
  }

  def lineitemOffset: Int = getOffset(lineitemSize)
  def supplierOffset: Int = getOffset(supplierSize)
  def partOffset: Int = getOffset(partSize)
  def partsuppOffset: Int = getOffset(partsuppSize)
  def customerOffset: Int = getOffset(customerSize)
  def ordersOffset: Int = getOffset(ordersSize)
  def nationOffset: Int = 25
  def regionOffset: Int = 5

  def GetMetaData(tableName: String):
  Option[(StructType, String, String, String, String, Long)] =
  {
    tableName.toLowerCase match {
      case "part" =>
        val realTopics =
          if (largeDataset) largePartTopics
          else partTopics
        Some((partSchema, avroPartSchema.toString, partPath, Empty_Static, realTopics, partOffset))
      case "partsupp" =>
        val realTopics =
          if (largeDataset) largePartSuppTopics
          else partsuppTopics
        Some((partsuppSchema, avroPartSuppSchema.toString,
          partsuppPath, Empty_Static, realTopics, partsuppOffset))
      case "supplier" =>
        val realTopics =
          if (largeDataset) largeSupplierTopics
          else supplierTopics
        Some((supplierSchema, avroSupplierSchema.toString,
          supplierPath, Empty_Static, realTopics, supplierOffset))
      case "customer" =>
        val realTopics =
          if (largeDataset) largeCustomerTopics
          else customerTopics
        Some((customerSchema, avroCustomerSchema.toString,
          customerPath, Empty_Static, realTopics, customerOffset))
      case "orders" =>
        val realTopics =
          if (largeDataset) largeOrdersTopics
          else ordersTopics
        Some((ordersSchema, avroOrdersSchema.toString,
          ordersPath, Empty_Static, realTopics, ordersOffset))
      case "lineitem" =>
        Some((lineitemSchema, avroLineitemSchema.toString,
          lineitemPath, Empty_Static, lineitemTopics, lineitemOffset))
      case "nation" =>
        Some((nationSchema, avroNationSchema.toString, nationPath, nationStaticPath,
          nationTopics, nationOffset))
      case "region" =>
        Some((regionSchema, avroRegionSchema.toString, regionPath, regionStaticPath,
          regionTopics, regionOffset))
      case _ =>
        printf("Unrecoganized Table %s\n", tableName)
        throw new Exception("Unrecoganized Table")
    }
  }

  def setQueryMetaData(numBatch: Int, SF: Double, hdfsRoot: String,
                       inputPartition: Int, largeDataset: Boolean): Unit = {
    numMiniBatch = numBatch
    scaleFactor = SF
    checkpointLocation = hdfsRoot + "/tpch_checkpoint"
    staticTableLocation = hdfsRoot + "/tpch_static"
    partitions = inputPartition
    this.largeDataset = largeDataset
    this.hdfsRoot = hdfsRoot
  }

  var hdfsRoot: String = _
}
