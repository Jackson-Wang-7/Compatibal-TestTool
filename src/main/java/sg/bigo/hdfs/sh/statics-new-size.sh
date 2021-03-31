#!/bin/bash

cat result-new-size1.csv | while read line
do
  dir=$(echo $line | awk -F ',' '{print $3}')

  if [ "$dir" == "" ]; then
    echo $line
    continue
  fi

	info=$(grep -w $dir all.csv)
  info=`echo $info | tr -d "\n\t\r"`
  line=`echo $line | tr -d "\n\t\r"`

  if [ "$info" == "" ]; then
    echo $line >> result-new-size2.csv
  else
    echo $line
    echo $info
    sizeB=$(echo $info | awk -F ',' '{print $2}')
  	sizeTB=$(echo $info | awk -F ',' '{print $3}')
  	sizeB=`echo $sizeB | tr -d "\n\t\r"`
    sizeTB=`echo $sizeTB | tr -d "\n\t\r"`

    resultstart=$(echo $line | awk -F ',' '{print $1","$2","$3}')
    resultend=$(echo $line | awk -F ',' '{print $6","$7","$8","$9","$10","$11","$12","$13}')

    echo "$resultstart,$sizeB,$sizeTB,$resultend" >> result-new-size2.csv
  fi

done