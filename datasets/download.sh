#!/bin/bash

# Downloads the entire "On-Time : Reporting Carrier On-Time Performance (1987-present)"

for year in {2009..2019}
do
  max_month=-1
  
  # Only download tables for past months
  [[ year -eq 2019 ]] && max_month=$(($(date +%m) - 1)) || max_month=12
  
  for month in $(seq 1 $max_month)
  do
      echo -e "\nDownloading table for ${year}_${month}...\n"
      curl -X GET -o On_Time_Performance_${year}_${month}.zip "https://transtats.bts.gov/PREZIP/On_Time_Reporting_Carrier_On_Time_Performance_1987_present_${year}_${month}.zip"
  done
done
