#!/bin/bash

cat hive-info.csv | while read line
do
	dir=$(echo $line | awk -F ',' '{print $3}')

  if [ "$dir" == "" ]; then
    echo $line
    continue
  fi

	info=$(grep -w $dir flume-data.csv)
  info=`echo $info | tr -d "\n\t\r"`
  line=`echo $line | tr -d "\n\t\r"`

  if [ "$info" == "" ]; then
    echo $line >> result-with-flume-size.csv
  else
    echo $line
    echo $info
    sizeB=$(echo $info | awk -F ',' '{print $2}')
  	sizeTB=$(echo $info | awk -F ',' '{print $3}')
  	sizeB=`echo $sizeB | tr -d "\n\t\r"`
    sizeTB=`echo $sizeTB | tr -d "\n\t\r"`

    resultstart=$(echo $line | awk -F ',' '{print $1","$2","$3}')
    resultend=$(echo $line | awk -F ',' '{print $6","$7","$8","$9","$10","$11","$12","$13}')

    echo "$resultstart,$sizeB,$sizeTB,$resultend" >> result-with-flume-size.csv
  fi

done