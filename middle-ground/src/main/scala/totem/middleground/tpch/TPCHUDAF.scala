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

import org.apache.spark.sql.{Column, Row}
import org.apache.spark.sql.expressions.MutableAggregationBuffer
import org.apache.spark.sql.expressions.UserDefinedAggregateFunction
import org.apache.spark.sql.types._

// sum over double
class DoubleSum extends  UserDefinedAggregateFunction {
  override def inputSchema: org.apache.spark.sql.types.StructType =
    StructType(StructField("double", DoubleType) :: Nil)

  override def bufferSchema: StructType = StructType(
    StructField("sum", DoubleType) :: Nil)

  override def dataType: DataType = DoubleType

  override def deterministic: Boolean = true

  override def initialize(buffer: MutableAggregationBuffer): Unit = {
    buffer(0) = 0.0
  }

  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    buffer(0) = buffer.getDouble(0) + input.getDouble(0)
  }

  override def delete(buffer: MutableAggregationBuffer, input: Row): Unit = {
    buffer(0) = buffer.getDouble(0) - input.getDouble(0)
  }

  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
    buffer1(0) = buffer1.getDouble(0) + buffer2.getDouble(0)
  }

  override def evaluate(buffer: Row): Any = {
    buffer.getDouble(0)
  }
}

// Avg over double
class DoubleAvg extends  UserDefinedAggregateFunction {
  override def inputSchema: org.apache.spark.sql.types.StructType =
    StructType(StructField("input", DoubleType) :: Nil)

  override def bufferSchema: StructType = StructType(
    StructField("count", DoubleType) ::
    StructField("sum", DoubleType) :: Nil
  )

  override def dataType: DataType = DoubleType

  override def deterministic: Boolean = true

  override def initialize(buffer: MutableAggregationBuffer): Unit = {
    buffer(0) = 0.0
    buffer(1) = 0.0
  }

  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    buffer(0) = buffer.getDouble(0) + 1.0
    buffer(1) = buffer.getDouble(1) + input.getDouble(0)
  }

  override def delete(buffer: MutableAggregationBuffer, input: Row): Unit = {
    buffer(0) = buffer.getDouble(0) - 1.0
    buffer(1) = buffer.getDouble(1) - input.getDouble(0)
  }

  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
    buffer1(0) = buffer1.getDouble(0) + buffer2.getDouble(0)
    buffer1(1) = buffer1.getDouble(1) + buffer2.getDouble(1)
  }

  override def evaluate(buffer: Row): Any = {
    buffer.getDouble(1)/buffer.getDouble(0)
  }
}

// Count
class Count extends  UserDefinedAggregateFunction {
  override def inputSchema: org.apache.spark.sql.types.StructType =
    StructType(StructField("input", IntegerType) :: Nil)

  override def bufferSchema: StructType = StructType(
    StructField("count", IntegerType) :: Nil)

  override def dataType: DataType = IntegerType

  override def deterministic: Boolean = true

  override def initialize(buffer: MutableAggregationBuffer): Unit = {
    buffer(0) = 0
  }

  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    buffer(0) = buffer.getInt(0) + 1
  }

  override def delete(buffer: MutableAggregationBuffer, input: Row): Unit = {
    buffer(0) = buffer.getInt(0) - 1
  }

  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
    buffer1(0) = buffer1.getInt(0) + buffer2.getInt(0)
  }

  override def evaluate(buffer: Row): Any = {
    buffer.getInt(0)
  }
}

// Count not null
class Count_not_null extends  UserDefinedAggregateFunction {
  override def inputSchema: org.apache.spark.sql.types.StructType =
    StructType(StructField("input", LongType) :: Nil)

  override def bufferSchema: StructType = StructType(
    StructField("count", LongType) :: Nil)

  override def dataType: DataType = LongType

  override def deterministic: Boolean = true

  override def initialize(buffer: MutableAggregationBuffer): Unit = {
    buffer(0) = 0L
  }

  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    if (!input.isNullAt(0)) buffer(0) = buffer.getLong(0) + 1
  }

  override def delete(buffer: MutableAggregationBuffer, input: Row): Unit = {
    if (!input.isNullAt(0)) buffer(0) = buffer.getLong(0) - 1
  }

  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
    buffer1(0) = buffer1.getLong(0) + buffer2.getLong(0)
  }

  override def evaluate(buffer: Row): Any = {
    buffer.getLong(0)
  }
}

// sum(l_extendedprice * (1 - l_discount))
class Sum_disc_price extends  UserDefinedAggregateFunction {
     // This is the input fields for your aggregate function.
  override def inputSchema: org.apache.spark.sql.types.StructType =
    StructType(StructField("l_extendedprice", DoubleType) ::
      StructField("l_discount", DoubleType) :: Nil)

  // This is the internal fields you keep for computing your aggregate.
  override def bufferSchema: StructType = StructType(
    StructField("sum_disc_price", DoubleType) :: Nil
  )

  // This is the output type of your aggregatation function.
  override def dataType: DataType = DoubleType

  override def deterministic: Boolean = true

  // This is the initial value for your buffer schema.
  override def initialize(buffer: MutableAggregationBuffer): Unit = {
    buffer(0) = 0.0
  }

  // This is how to update your buffer schema given an input.
  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
      buffer(0) = buffer.getAs[Double](0) + input.getAs[Double](0) * (1 - input.getAs[Double](1))
  }

  override def delete(buffer: MutableAggregationBuffer, input: Row): Unit = {
    buffer(0) = buffer.getAs[Double](0) - input.getAs[Double](0) * (1 - input.getAs[Double](1))
  }

  // This is how to merge two objects with the bufferSchema type.
  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
    buffer1(0) = buffer1.getAs[Double](0) + buffer2.getAs[Double](0)
  }

  // This is where you output the final value, given the final value of your bufferSchema.
  override def evaluate(buffer: Row): Any = {
    buffer.getDouble(0)
  }
}

// sum(l_extendedprice * (1 - l_discount)) * (tax + 1)
class Sum_disc_price_with_tax extends  UserDefinedAggregateFunction {
     // This is the input fields for your aggregate function.
  override def inputSchema: org.apache.spark.sql.types.StructType =
    StructType(StructField("l_extendedprice", DoubleType) ::
      StructField("l_discount", DoubleType) ::
      StructField("tax", DoubleType) :: Nil)

  // This is the internal fields you keep for computing your aggregate.
  override def bufferSchema: StructType = StructType(
    StructField("sum_disc_price_with_tax", DoubleType) :: Nil
  )

  // This is the output type of your aggregatation function.
  override def dataType: DataType = DoubleType

  override def deterministic: Boolean = true

  // This is the initial value for your buffer schema.
  override def initialize(buffer: MutableAggregationBuffer): Unit = {
    buffer(0) = 0.0
  }

  // This is how to update your buffer schema given an input.
  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
      buffer(0) = buffer.getDouble(0) +
        input.getDouble(0) * (1 - input.getDouble(1)) *
        (1 + input.getDouble(2))
  }

  override def delete(buffer: MutableAggregationBuffer, input: Row): Unit = {
    buffer(0) = buffer.getAs[Double](0) -
        input.getDouble(0) * (1 - input.getDouble(1)) *
        (1 + input.getDouble(2))
  }

  // This is how to merge two objects with the bufferSchema type.
  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
    buffer1(0) = buffer1.getDouble(0) + buffer2.getDouble(0)
  }

  // This is where you output the final value, given the final value of your bufferSchema.
  override def evaluate(buffer: Row): Any = {
    buffer.getDouble(0)
  }
}

class UDAF_Q8 extends UserDefinedAggregateFunction {
    // This is the input fields for your aggregate function.
  override def inputSchema: org.apache.spark.sql.types.StructType =
    StructType(StructField("n2_name", StringType) ::
      StructField("volume", DoubleType) :: Nil)

  // This is the internal fields you keep for computing your aggregate.
  override def bufferSchema: StructType = StructType(
    StructField("partial_volume", DoubleType) ::
    StructField("total_volume", DoubleType) :: Nil
  )

  // This is the output type of your aggregatation function.
  override def dataType: DataType = DoubleType

  override def deterministic: Boolean = true

  // This is the initial value for your buffer schema.
  override def initialize(buffer: MutableAggregationBuffer): Unit = {
    buffer(0) = 0.0
    buffer(1) = 0.0
  }

  // This is how to update your buffer schema given an input.
  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    val name = input.getAs[String](0)
    val volume = input.getAs[Double](1)

    if (name == "BRAZIL") {
      buffer(0) = buffer.getAs[Double](0) + volume
    }
    buffer(1) = buffer.getAs[Double](1) + volume
  }

  // This is how to merge two objects with the bufferSchema type.
  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
    buffer1(0) = buffer1.getAs[Double](0) + buffer2.getAs[Double](0)
    buffer1(1) = buffer1.getAs[Double](1) * buffer2.getAs[Double](1)
  }

  // This is where you output the final value, given the final value of your bufferSchema.
  override def evaluate(buffer: Row): Any = {
    buffer.getDouble(0) / buffer.getDouble(1)
  }
}

class UDAF_Q12_HIGH extends UserDefinedAggregateFunction {
    // This is the input fields for your aggregate function.
  override def inputSchema: org.apache.spark.sql.types.StructType =
    StructType(StructField("o_orderpriority", StringType) :: Nil)

  // This is the internal fields you keep for computing your aggregate.
  override def bufferSchema: StructType = StructType(
    StructField("count", LongType) :: Nil
  )

  // This is the output type of your aggregatation function.
  override def dataType: DataType = LongType

  override def deterministic: Boolean = true

  // This is the initial value for your buffer schema.
  override def initialize(buffer: MutableAggregationBuffer): Unit = {
    buffer(0) = 0L
  }

  // This is how to update your buffer schema given an input.
  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    val priority = input.getAs[String](0)

    if (priority == "1-URGENT" || priority == "2-HIGH") {
      buffer(0) = buffer.getAs[Long](0) + 1
    }
  }

  // This is how to merge two objects with the bufferSchema type.
  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
    buffer1(0) = buffer1.getAs[Long](0) + buffer2.getAs[Long](0)
  }

  // This is where you output the final value, given the final value of your bufferSchema.
  override def evaluate(buffer: Row): Any = {
    buffer.getLong(0)
  }
}

class UDAF_Q12_LOW extends UserDefinedAggregateFunction {
    // This is the input fields for your aggregate function.
  override def inputSchema: org.apache.spark.sql.types.StructType =
    StructType(StructField("o_orderpriority", StringType) :: Nil)

  // This is the internal fields you keep for computing your aggregate.
  override def bufferSchema: StructType = StructType(
    StructField("count", LongType) :: Nil
  )

  // This is the output type of your aggregatation function.
  override def dataType: DataType = LongType

  override def deterministic: Boolean = true

  // This is the initial value for your buffer schema.
  override def initialize(buffer: MutableAggregationBuffer): Unit = {
    buffer(0) = 0L
  }

  // This is how to update your buffer schema given an input.
  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    val priority = input.getAs[String](0)

    if (priority != "1-URGENT" && priority != "2-HIGH") {
      buffer(0) = buffer.getAs[Long](0) + 1
    }
  }

  // This is how to merge two objects with the bufferSchema type.
  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
    buffer1(0) = buffer1.getAs[Long](0) + buffer2.getAs[Long](0)
  }

  // This is where you output the final value, given the final value of your bufferSchema.
  override def evaluate(buffer: Row): Any = {
    buffer.getLong(0)
  }
}

class UDAF_Q14 extends UserDefinedAggregateFunction {
    // This is the input fields for your aggregate function.
  override def inputSchema: org.apache.spark.sql.types.StructType =
    StructType(StructField("p_type", StringType) ::
      StructField("l_extendedprice", DoubleType) ::
      StructField("l_discount", DoubleType) :: Nil)

  // This is the internal fields you keep for computing your aggregate.
  override def bufferSchema: StructType = StructType(
    StructField("revenue", DoubleType) :: Nil
  )

  // This is the output type of your aggregatation function.
  override def dataType: DataType = DoubleType

  override def deterministic: Boolean = true

  // This is the initial value for your buffer schema.
  override def initialize(buffer: MutableAggregationBuffer): Unit = {
    buffer(0) = 0.0
  }

  // This is how to update your buffer schema given an input.
  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    val p_type = input.getAs[String](0)

    if (p_type.startsWith("PROMO")) {
      buffer(0) = buffer.getAs[Double](0) + input.getAs[Double](1) * (1 - input.getAs[Double](2))
    }
  }

  // This is how to merge two objects with the bufferSchema type.
  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
    buffer1(0) = buffer1.getAs[Double](0) + buffer2.getAs[Double](0)
  }

  // This is where you output the final value, given the final value of your bufferSchema.
  override def evaluate(buffer: Row): Any = {
    buffer.getDouble(0)
  }
}
