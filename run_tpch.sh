#!/bin/sh

#./bin/spark-submit --num-executors 1 --executor-cores 1 --conf "spark.executor.extraJavaOptions=-agentlib:jdwp=transport=dt_socket,server=y,address=4000,suspend=y" --conf spark.sql.codegen.wholeStage=false --class totem.middleground.tpch.QueryTPCH --master spark://localhost:7077 middle-ground/target/scala-2.11/totem-middle-ground_2.11-2.4.0.jar localhost:9092 q1

./bin/spark-submit --num-executors 1 --executor-cores 1 --conf "spark.driver.extraJavaOptions=-agentlib:jdwp=transport=dt_socket,server=y,suspend=y,address=4000" --class totem.middleground.tpch.QueryTPCH --master local[1] middle-ground/target/scala-2.11/totem-middle-ground_2.11-2.4.0.jar localhost:9092 q17

# --conf spark.sql.codegen.wholeStage=false


