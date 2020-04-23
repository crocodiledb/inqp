#!/bin/bash

SPARK_HOME=/home/totemtang/slothdb/spark

if [ $# -ne 2 ]
then
    echo "Need two parameters: <query name> <number of batches>"
    exit
fi

shuffle_num=2
stat_dir=/home/totemtang/slothdb/slothdb_testsuite/statdir
sf=0.1
master=local[1]
#master=spark://localhost:7077
hdfs_root=hdfs://localhost:9000

$SPARK_HOME/bin/spark-submit \
    --num-executors 1 \
    --executor-cores 1 \
    --class totem.middleground.tpch.QueryTPCH \
    --master $master \
    $SPARK_HOME/middle-ground/target/scala-2.11/totem-middle-ground_2.11-2.4.0.jar \
    localhost:9092 \
    $1 \
    $2 \
    $shuffle_num \
    $stat_dir \
    $sf \
    $hdfs_root

# --conf "spark.driver.extraJavaOptions=-agentlib:jdwp=transport=dt_socket,server=y,suspend=y,address=4000" \

#$SPARK_HOME/bin/spark-submit --num-executors 2 --executor-cores 2 --class totem.middleground.tpch.QueryTPCH --master local[1] $SPARK_HOME/middle-ground/target/scala-2.11/totem-middle-ground_2.11-2.4.0.jar localhost:9092 $1 $2 1


