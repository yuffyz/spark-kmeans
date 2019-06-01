#!/bin/bash
source ../../../env.sh
/usr/local/hadoop/bin/hdfs dfs -rm -r /lab2/Q2/input/
/usr/local/hadoop/bin/hdfs dfs -mkdir -p /lab2/Q2/input/
/usr/local/hadoop/bin/hdfs dfs -copyFromLocal ../../../../data/parkingSample.csv /lab2/Q2/input/
/usr/local/spark/bin/spark-submit --master=spark://$SPARK_MASTER:7077 ./parking-kmeans.py hdfs://$SPARK_MASTER:9000/lab2/Q2/input/