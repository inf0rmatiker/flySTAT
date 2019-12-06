#!/bin/bash
HDFSNODE="hdfs://cheyenne:30241"
RESULTS="linearResults.txt"

#Get the name of all directories in output folder
hadoop fs -ls $HDFSNODE/"testAll" > "dirNames.txt"
python parseFile.py "dirNames.txt" > "dates.txt"

#Create a text file to add all results to
#touch $RESULTS
for date in $(cat < "dates.txt"); do
    hadoop fs -cat $HDFSNODE/"testAll/$date/withZerosStats/part*" >> $RESULTS
    hadoop fs -cat $HDFSNODE/"testAll/$date/withoutZerosStats/part*" >> $RESULTS
done
    
