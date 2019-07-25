#!/bin/sh

datadir=/home/totemtang/slothdb/slothdb_testsuite/datadir/tpchdata
checkpoint=hdfs://localhost:9000/tpch_checkpoint

./bin/spark-submit --class totem.middleground.tpch.LoadTPCH --master local[2] middle-ground/target/scala-2.11/totem-middle-ground_2.11-2.4.0.jar localhost:9092 $datadir $checkpoint


