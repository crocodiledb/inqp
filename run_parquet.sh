#!/bin/bash


bin/spark-submit \
    --num-executors 1 \
    --executor-cores 1 \
    --class org.apache.spark.examples.sql.SQLDataSourceExample \
    --master local[1] \
    --conf "spark.driver.extraJavaOptions=-agentlib:jdwp=transport=dt_socket,server=y,suspend=y,address=4000" \
    examples/target/scala-2.11/jars/spark-examples_2.11-2.4.0.jar

rm -r *.parquet
rm -r *.orc

