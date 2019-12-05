#!/bin/bash

for date in $(cat < "days.txt"); do
    $SPARK_HOME/bin/spark-submit --master spark://honolulu:42056 --class analytics.LinearRegression --supervise target/scala-2.11/flystat_2.11-1.0.jar $date /testAll 11292
done
