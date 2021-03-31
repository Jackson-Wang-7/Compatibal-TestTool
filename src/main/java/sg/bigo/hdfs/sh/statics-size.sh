#!/bin/bash

cat hive-tables.csv | while read line
do
	dir=$(echo $line | awk -F ',' '{print $3}')

  if [ "$dir" == "" ]; then
    echo $line
    continue
  fi

	info=$(grep -w $dir bigocluster-all.csv)
	sizeB=$(echo $info | awk -F ',' '{print $2}')
	sizeTB=$(echo $info | awk -F ',' '{print $3}')
	owner=$(echo $info | awk -F ',' '{print $4}')
	group=$(echo $info | awk -F ',' '{print $5}')

  line=`echo $line | tr -d "\n\t\r"`
  info=`echo $info | tr -d "\n\t\r"`
  group=`echo $group | tr -d "\n\t\r"`
  owner=`echo $owner | tr -d "\n\t\r"`

  echo "$line,$sizeB,$sizeTB,$owner,$group" >> result-size-owner.csv

done