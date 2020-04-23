#!/bin/sh

SPARK_HOME=/home/totemtang/slothdb/spark

$SPARK_HOME/bin/spark-submit --num-executors 1 --executor-cores 1 --class totem.middleground.tpch.unittest.CPUTest --master local[1] $SPARK_HOME/middle-ground/target/scala-2.11/totem-middle-ground_2.11-2.4.0.jar


