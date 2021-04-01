#!/bin/bash
int=1
while(( $int<=5 ))
do
    ./hdfs dfs -mkdir hdfs://dn1949.jja.bigo:8888/user/wangyuyang/test-mount
    ./hdfs dfs -rm -r -f -skipTrash hdfs://dn1949.jja.bigo:8888/user/wangyuyang/test-mount
    ./hdfs dfsrouteradmin -add /user/wangyuyang/test-mount test-hadoop2 /user/wangyuyang/test-mount
    ./hdfs dfs -mkdir hdfs://dn1949.jja.bigo:8888/user/wangyuyang/test-mount
    ./hdfs dfs -rm -r -f -skipTrash hdfs://dn1949.jja.bigo:8888/user/wangyuyang/test-mount
    ./hdfs dfsrouteradmin -update /user/wangyuyang/test-mount test-hadoop3 /user/wangyuyang/test-mount
    ./hdfs dfs -mkdir hdfs://dn1949.jja.bigo:8888/user/wangyuyang/test-mount
    ./hdfs dfs -rm -r -f -skipTrash hdfs://dn1949.jja.bigo:8888/user/wangyuyang/test-mount
    ./hdfs dfsrouteradmin -rm /user/wangyuyang/test-mount
    let "int++"
done