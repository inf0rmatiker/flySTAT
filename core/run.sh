#!/bin/bash

function printUsageMessage() {
  usageMessage="Usage:\n\n$\t./run.sh [OPTION] [ARGS]\n\n\tOPTION:\t\tDESCRIPTION:\n\n\t--averages, -a\tRuns average predictions.\n
                \t\t\tArguments: <HDFS_Input_Path> <HDFS_Output_Path>\n\n
                \t--linear, -l\tRuns linear regression.\n"

  echo -e $usageMessage
}

function setup() {
  echo -e "\nRebuilding package..."
  sbt clean package

  echo -e "\nRemoving any previously existing output directory..."
  ${HADOOP_HOME}/bin/hdfs dfs -rm -r -f $1
}

if [[ $# -ge 3 ]]; then
  
  HDFS_INPUT_DIR=$2
  HDFS_OUTPUT_DIR=$3

  if [[ $1 == "--averages" || $1 == "-a" ]]; then

    setup $HDFS_OUTPUT_DIR

    echo -e "\nSubmitting averages job to Spark..."
    ${SPARK_HOME}/bin/spark-submit  \
      --class Main  \
      --master spark://dover:34567  \
      --supervise target/scala-2.11/flystat_2.11-1.0.jar $HDFS_INPUT_DIR $HDFS_OUTPUT_DIR 
   
  elif [[ $# -eq 5 && ($1 == "--linear" || $1 == "-l") ]]; then
    
    DATES_FILE=$4
    AIRPORT_ID=$5 # I.E. DEN airport ID: 11292 TODO: Use lookup tables to search for name

    setup $HDFS_OUTPUT_DIR

    for date in $(cat < $DATES_FILE); do
      $SPARK_HOME/bin/spark-submit --master spark://dover:34567 --class analytics.LinearRegression --supervise target/scala-2.11/flystat_2.11-1.0.jar $date $HDFS_OUTPUT_DIR $AIRPORT_ID
    done   

  else
    printUsageMessage
  fi
else 
  printUsageMessage
fi
