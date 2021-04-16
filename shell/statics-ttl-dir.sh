#!/bin/bash

cat result-with-flume-size.csv | while read line
do
	dir=$(echo $line | awk -F ',' '{print $3}')

  if [ "$dir" == "" ]; then
    echo $line
    continue
  fi

	info=$(grep -w $dir hdfs_clear_config.csv)
  info=`echo $info | tr -d "\n\t\r"`
  line=`echo $line | tr -d "\n\t\r"`

  if [ "$info" == "" ]; then
    echo $line >> result-with-ttl-dir.csv
  else
    echo $line
    echo $info
    partition=$(echo $info | awk -F ',' '{print $2}')
  	ttl=$(echo $info | awk -F ',' '{print $3}')
  	partition=`echo $partition | tr -d "\n\t\r"`
    ttl=`echo $ttl | tr -d "\n\t\r"`

    resultstart=$(echo $line | awk -F ',' '{print $1","$2","$3","$4","$5","$6","$7","$8","$9","$10}')

    echo "$resultstart,$partition,$ttl,done" >> result-with-ttl-dir.csv
  fi

done