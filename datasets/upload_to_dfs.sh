#!/bin/bash

# Unzips, uploads to HDFS, and removes monthly tables in batches by year.
# Otherwise, we exceed Disk Quota and are unable to unzip.

hdfs="${HADOOP_HOME}/bin/hdfs"

if [[ $# -ge 1 ]]; then
  for year in {2011..2019}
  do
    unzip "On_Time_Performance_${year}*"
    ${hdfs} dfs -put *.csv $1 
    rm *.csv
  done
else
  echo -e "\nUsage: ./upload_to_dfs.sh <hdfs_directory_path>\n"
fi

