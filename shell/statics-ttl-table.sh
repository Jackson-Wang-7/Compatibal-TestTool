#!/bin/bash

cat result-with-ttl-dir.csv | while read line
do
	db=$(echo $line | awk -F ',' '{print $1}')
	table=$(echo $line | awk -F ',' '{print $2}')

  if [ "$table" == "" ]; then
    echo $line
    continue
  fi

	info=$(grep -w $table hive_clear_config.csv | grep -w $db)
  info=`echo $info | tr -d "\n\t\r"`
  line=`echo $line | tr -d "\n\t\r"`

  if [ "$info" == "" ]; then
    echo $line >> result-with-ttl-table.csv
  else
    echo $line
    echo $info
    partition=$(echo $info | awk -F ',' '{print $3}')
  	ttl=$(echo $info | awk -F ',' '{print $4}')
  	partition=`echo $partition | tr -d "\n\t\r"`
    ttl=`echo $ttl | tr -d "\n\t\r"`

    resultstart=$(echo $line | awk -F ',' '{print $1","$2","$3","$4","$5","$6","$7","$8","$9","$10}')

    echo "$resultstart,$partition,$ttl,done" >> result-with-ttl-table.csv
  fi

done