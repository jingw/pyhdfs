#!/bin/bash -eux

version=${1:-$VERSION}
tgz=download/hadoop-$version.tar.gz
if [[ ! -e  "$tgz" || "$(md5sum "$tgz")" != ${MD5:-}* ]]
then
    mkdir -p download
    curl -o "$tgz" "http://mirrors.ocf.berkeley.edu/apache/hadoop/common/hadoop-$version/hadoop-$version.tar.gz"
    [[ "$(md5sum "$tgz")" = ${MD5:-}* ]]
fi

mkdir -p hadoop
tar -xzf "$tgz" -C hadoop --strip-components 1

cp test-hdfs-conf/* hadoop/etc/hadoop

hadoop/bin/hdfs namenode -format
hadoop/bin/hdfs namenode > namenode.log 2>&1 &
hadoop/bin/hdfs datanode > datanode.log 2>&1 &

until hadoop/bin/hdfs dfs -touchz /healthcheck
do
    tail namenode.log datanode.log
    sleep 1
done

hadoop/bin/hdfs dfs -mkdir /tmp
hadoop/bin/hdfs dfs -chmod -R 1777 /tmp
