#!/bin/bash

cat 111111111111111.csv | while read line
do
  if [[     "$line" =~ "like"
         || "$line" =~ "likee"
         || "$line" =~ "vlog"
         || "$line" =~ "welog"
         || "$line" =~ "live"
         || "$line" =~ "bigo"
         || "$line" =~ "bigolive"
         || "$line" =~ "livelite"
         || "$line" =~ "chenhuafeng"
         || "$line" =~ "chenchengjia"
         ]] ; then

    echo $line >> bigocluster-like-live.csv

  fi
done
