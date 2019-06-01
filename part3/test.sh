#!/bin/bash
source ../../../env.sh
/usr/local/hadoop/bin/hdfs dfs -rm -r /lab2/Q3/input/
/usr/local/hadoop/bin/hdfs dfs -mkdir -p /lab2/Q3/input/
/usr/local/hadoop/bin/hdfs dfs -copyFromLocal ../../../../data/shot_logs.csv /lab2/Q3/input/
/usr/local/spark/bin/spark-submit --conf spark.default.parallelism=2 --master=spark://$SPARK_MASTER:7077 ./nba-kmeans-3.py hdfs://$SPARK_MASTER:9000/lab2/Q3/input/