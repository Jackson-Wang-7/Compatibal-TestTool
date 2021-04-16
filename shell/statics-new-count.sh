#!/bin/bash

cat result-new-size3.csv | while read line
do
  dir=$(echo $line | awk -F ',' '{print $3}')

  if [ "$dir" == "" ]; then
    echo $line
    continue
  fi

	info=$(grep -w $dir data-apps-us_sync-data.csv)
  info=`echo $info | tr -d "\n\t\r"`
  line=`echo $line | tr -d "\n\t\r"`

  if [ "$info" == "" ]; then
    echo $line >> result-new-size4.csv
  else
    echo $line
    echo $info
    count=$(echo $info | awk -F ',' '{print $2}')
  	count=`echo $count | tr -d "\n\t\r"`

    resultstart=$(echo $line | awk -F ',' '{print $1","$2","$3","$4","$5","$6","$7","$8","$9}')
    resultend=$(echo $line | awk -F ',' '{print $11","$12","$13}')

    echo "$resultstart,$count,$resultend" >> result-new-size4.csv
  fi

done