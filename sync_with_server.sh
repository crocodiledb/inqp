#!/bin/bash

SPARK_HOME=/home/totemtang/slothdb/inqp

TPCH_JAR=$SPARK_HOME/middle-ground/target/scala-2.11/totem-middle-ground_2.11-2.4.0.jar
SQL_JAR=$SPARK_HOME/sql/core/target/scala-2.11/spark-sql_2.11-2.4.0.jar
CAT_JAR=$SPARK_HOME/sql/catalyst/target/scala-2.11/spark-catalyst_2.11-2.4.0.jar
AVRO_JAR=$SPARK_HOME/external/avro/target/scala-2.11/spark-avro_2.11-2.4.0.jar
KAFAK_JAR=$SPARK_HOME/external/kafka-0-10-sql/target/scala-2.11/spark-sql-kafka-0-10_2.11-2.4.0.jar


#REMOTE=lincoln.cs.uchicago.edu
REMOTE=southport.cs.uchicago.edu
REMOTE_SPARK=$REMOTE:/tank/hdfs/totem/slothdb/spark

#REMOTE=grace.cs.uchicago.edu
#REMOTE_SPARK=$REMOTE:/mnt/hdd-2T-1/totem/spark/

scp $TPCH_JAR $SQL_JAR $REMOTE_SPARK/jars
scp $CAT_JAR $AVRO_JAR $KAFKA_JAR $REMOTE_SPARK/jars


