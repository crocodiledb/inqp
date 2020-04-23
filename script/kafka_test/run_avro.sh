#!/bin/sh

SPARK_HOME=/home/totemtang/slothdb/spark

$SPARK_HOME/bin/spark-submit \
    --class totem.middleground.avro.ConsumeKafka \
    --master local[2] $SPARK_HOME/middle-ground/target/scala-2.11/totem-middle-ground_2.11-2.4.0.jar \
    localhost:9092


