#!/bin/bash

if [[ $# -ge 2 ]]; then

  echo -e "\nRebuilding package..."
  sbt clean package

  echo -e "\nRemoving any previously existing output dirs..."
  ${HADOOP_HOME}/bin/hdfs dfs -rm -r -f $2

  echo -e "\nSubmitting job to Spark..."
  ${SPARK_HOME}/bin/spark-submit  \
    --class Main  \
    --master spark://dover:34567  \
    --executor-cores 10  \
    --num-executors 10 \
    --executor-memory 10g  \
    --supervise target/scala-2.11/flystat_2.11-1.0.jar $1 $2

else
  echo -e "Usage:\n\n$\t./run.sh <hdfs_input_path> <hdfs_output_path>\n"
fi
