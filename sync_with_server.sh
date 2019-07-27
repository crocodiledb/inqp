#!/bin/bash

TPCH_JAR=/home/totemtang/slothdb/spark/middle-ground/target/scala-2.11/totem-middle-ground_2.11-2.4.0.jar
SQL_JAR=/home/totemtang/slothdb/spark/sql/core/target/scala-2.11/spark-sql_2.11-2.4.0.jar
EXAMPLE_JAR=/home/totemtang/slothdb/spark/examples/target/scala-2.11/jars/spark-examples_2.11-2.4.0.jar
CAT_JAR=/home/totemtang/slothdb/spark/sql/catalyst/target/scala-2.11/spark-catalyst_2.11-2.4.0.jar
AVRO_JAR=/home/totemtang/slothdb/spark/external/avro/target/scala-2.11/spark-avro_2.11-2.4.0.jar


#REMOTE=lincoln.cs.uchicago.edu
REMOTE=southport.cs.uchicago.edu
REMOTE_SPARK=$REMOTE:/tank/hdfs/totem/slothdb/spark

#scp $TPCH_JAR $SQL_JAR $EXAMPLE_JAR $REMOTE_SPARK/jars/
#scp $TPCH_JAR $SQL_JAR $REMOTE_SPARK/jars/
scp $TPCH_JAR $REMOTE_SPARK/jars/
#scp $TPCH_JAR $REMOTE_SPARK/jars/
#scp $SQL_JAR $REMOTE_SPARK/jars/
#scp $CAT_JAR $REMOTE_SPARK/jars/


