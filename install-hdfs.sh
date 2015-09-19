#!/bin/bash -eux

sudo wget "http://archive.cloudera.com/$CDH/ubuntu/precise/amd64/cdh/cloudera.list" \
    -O /etc/apt/sources.list.d/cloudera.list
# work around broken list
sudo sed -i 's mirror.infra.cloudera.com/archive archive.cloudera.com g' \
    /etc/apt/sources.list.d/cloudera.list
sudo apt-get update
sudo apt-get install -y --force-yes hadoop-hdfs-datanode hadoop-hdfs-namenode

# Set up config
sudo cp -r /etc/hadoop/conf.empty /etc/hadoop/conf.travis
sudo cp travis-hdfs-conf/* /etc/hadoop/conf.travis
sudo update-alternatives --install /etc/hadoop/conf hadoop-conf /etc/hadoop/conf.travis 50
sudo update-alternatives --set hadoop-conf /etc/hadoop/conf.travis

# Verify that the above worked
sudo update-alternatives --display hadoop-conf
# Dump everything with the file name prefixed for debugging
grep . /etc/hadoop/conf/*

sudo -u hdfs hdfs namenode -format -nonInteractive
sudo service hadoop-hdfs-datanode start || (grep . /var/log/hadoop-hdfs/* && exit 2)
sudo service hadoop-hdfs-namenode start || (grep . /var/log/hadoop-hdfs/* && exit 2)

sudo -u hdfs hdfs dfsadmin -safemode leave

sudo -u hdfs hdfs dfs -mkdir /tmp
sudo -u hdfs hdfs dfs -chmod -R 1777 /tmp
