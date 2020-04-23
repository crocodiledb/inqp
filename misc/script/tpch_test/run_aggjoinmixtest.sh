#!/bin/sh

SPARK_HOME=/home/totemtang/slothdb/spark

$SPARK_HOME/bin/spark-submit --num-executors 1 --executor-cores 1 --conf "spark.driver.extraJavaOptions=-agentlib:jdwp=transport=dt_socket,server=y,suspend=y,address=4000" --class totem.middleground.tpch.unittest.AggJoinMixTest --master local[1] $SPARK_HOME/middle-ground/target/scala-2.11/totem-middle-ground_2.11-2.4.0.jar localhost:9092 q17


