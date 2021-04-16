#!/bin/bash

cat result-size-owner.csv | while read line
do
	dir=$(echo $line | awk -F ',' '{print $3}')
	ownerold=$(echo $line | awk -F ',' '{print $7}')

  if [ "$dir" == "" ]; then
    echo $line
    continue
  fi

	info=$(grep -w $dir recommend-db-owner)
	owner=$(echo $info | awk '{print $1}')
	group=$(echo $info | awk '{print $2}')

  line=`echo $line | tr -d "\n\t\r"`
  info=`echo $info | tr -d "\n\t\r"`
  owner=`echo $owner | tr -d "\n\t\r"`
  group=`echo $group | tr -d "\n\t\r"`

  if [ "$ownerold" != "" ]; then
    echo $line >> result.csv
  else
    result=$(echo $line | awk -F ',' '{print $1","$2","$3","$4","$5","$6}')
    echo "$result,$owner,$group" >> result.csv
  fi

done