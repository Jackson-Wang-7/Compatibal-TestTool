#!/bin/bash

cat bigocluster-like-live.csv | while read line
do
  db=$(echo $line | awk -F ',' '{print $1}')
	table=$(echo $line | awk -F ',' '{print $2}')

  if [ "$table" == "" ]; then
    echo $line
    continue
  fi

  info=$(grep -w $table hive-par-day.csv | grep -w $db | grep -v "imo" | grep -v "indigo" | grep -v "cubetv" | grep -v "indigo_mediate_tb")
  info=`echo $info | tr -d "\n\t\r"`
  line=`echo $line | tr -d "\n\t\r"`

  if [ "$info" == "" ]; then
    echo $line >> bigocluster-hive-is-partitioned1.csv
  else
    echo $line
    echo $info

    echo "$line,1" >> bigocluster-hive-is-partitioned1  .csv
  fi

done