#!/bin/bash

cat hive-table-size-statics.csv | while read line
do
  table_name=$(echo $line | awk -F ',' '{print $1}')

  if [ "$table_name" == "" ]; then
    echo $line
    continue
  fi

  result_in_30d=`curl "http://sg-dn10.bigdata.bigo.inner:9001/getTableSize?tableFullName=${table_name}&days=30&isRecent=true"`
  echo $result_in_30d
  result_in_90d=`curl "http://sg-dn10.bigdata.bigo.inner:9001/getTableSize?tableFullName=${table_name}&days=90&isRecent=true"`
  echo $result_in_90d
  result_beyond_90d=`curl "http://sg-dn10.bigdata.bigo.inner:9001/getTableSize?tableFullName=${table_name}&days=90&isRecent=false"`
  echo $result_beyond_90d

  result_mb_in_30d=`echo ${result_in_30d} | grep -Po "\d+"`
  result_mb_in_90d=`echo ${result_in_90d} | grep -Po "\d+"`
  result_mb_beyond_90d=`echo ${result_beyond_90d} | grep -Po "\d+"`

  if [ "$result_mb" == "" ]; then
    echo $line >> hive-table-size-statics-dest.csv
  else
    echo $line

    echo "$line,$result_mb_in_30d,$result_mb_in_90d,$result_mb_beyond_90d" >> hive-table-size-statics-dest.csv
  fi

done