#!/bin/bash

cat dir_tobe_remove.csv | while read line
do

  output=`hadoop fs -ls hdfs://hk-bigocluster${line}`

  if [[ !"$line" =~ "$output" ]]; then
     echo "dir not found!"
     hadoop fs -rm -r hdfs://bigocluster${line}
  else
     echo "dir exists!"
  fi

done