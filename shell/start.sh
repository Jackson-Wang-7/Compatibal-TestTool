#!/bin/bash
source /etc/profile
nohup java -jar HdfsTest-1.0-SNAPSHOT.jar 0 1000000 100 hdfs://dn2125.jja.bigo:8888 /user/litao/sg-test/data_generator13yjTAc83/ > hdfs.log  2>&1 &
