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
input_partition=2
largedataset=false

## 0: querypath-aware, 1: subplan-aware, 2: IncObv,
## 3: IncStat, collect cardinality groudtruth
## 4: Run $batch_num, collect selectivities
## 5: Test SlothOverhead
execution_mode=0

## 0: turn off iOLAP, 1: turn on iOLAP
iolap=0

## Percentage of choosing the right incrementability
inc_percentage=1.0
#inc_percentage=1.0

## Bias of statistical information 
cost_bias=1.0

## Performance goal
## smaller than 1.0 -> latency constraint
## larger  than 1.0 -> resource constraint
constraint=0.001

## Max step
max_step=100

## Sample time
sample_time=4.0

## Spike rate
spike_rate=5.0

for constraint in 0.05
do
    $SPARK_HOME/bin/spark-submit \
        --num-executors 1 \
        --executor-cores 1 \
        --class totem.middleground.tpch.QueryTPCH \
        --master $master \
        --conf "spark.driver.extraJavaOptions=-agentlib:jdwp=transport=dt_socket,server=y,suspend=y,address=4000" \
        $SPARK_HOME/middle-ground/target/scala-2.11/totem-middle-ground_2.11-2.4.0.jar \
        localhost:9092 \
        $1 \
        $2 \
        $shuffle_num \
        $stat_dir \
        $sf \
        $hdfs_root \
        $execution_mode \
        $input_partition \
        $constraint \
        $largedataset \
        $iolap \
        $inc_percentage \
        $cost_bias \
        $max_step \
        $sample_time \
        $spike_rate
done

#$SPARK_HOME/bin/spark-submit --num-executors 2 --executor-cores 2 --class totem.middleground.tpch.QueryTPCH --master local[1] $SPARK_HOME/middle-ground/target/scala-2.11/totem-middle-ground_2.11-2.4.0.jar localhost:9092 $1 $2 1


#    --conf "spark.driver.extraJavaOptions=-agentlib:jdwp=transport=dt_socket,server=y,suspend=y,address=4000" \
