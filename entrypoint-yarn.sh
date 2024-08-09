#!/bin/bash

SPARK_WORKLOAD=$1

echo "SPARK_WORKLOAD: $SPARK_WORKLOAD"

/etc/init.d/ssh start

if [ "$SPARK_WORKLOAD" == "master" ];
then
  hdfs namenode -format

  # start the master node processes
  hdfs --daemon start namenode
  hdfs --daemon start secondarynamenode
  yarn --daemon start resourcemanager

  # create required directories
  while ! hdfs dfs -mkdir -p /spark-logs;
  do
    echo "Failed creating /spark-logs hdfs dir"
  done
  echo "Created /spark-logs hdfs dir"
  hdfs dfs -mkdir -p /opt/spark/data
  echo "Created /opt/spark/data hdfs dir"

  # copy the data to the data HDFS directory
  hdfs dfs -copyFromLocal /opt/spark/data/* /opt/spark/data
  hdfs dfs -ls /opt/spark/data

elif [ "$SPARK_WORKLOAD" == "worker" ];
then
  hdfs namenode -format

  # start the worker node processes
  hdfs --daemon start datanode
  yarn --daemon start nodemanager
elif [ "$SPARK_WORKLOAD" == "history" ];
then

  while ! hdfs dfs -test -d /spark-logs;
  do
    echo "spark-logs doesn't exist yet... retrying"
    sleep 1;
  done
  echo "Exit loop"

  # start the spark history server
  start-history-server.sh

elif [ "$SPARK_WORKLOAD" == "hive" ];
then
  hdfs namenode -format

  # start the hive node processes
  hdfs --daemon start datanode
  yarn --daemon start nodemanager

  # create hive directories
  hdfs dfs -mkdir -p /tmp
  echo "Created /tmp hdfs dir"

  hdfs dfs -mkdir -p /user/hive
  echo "Created /user/hive hdfs dir"

  hdfs dfs -mkdir -p /user/hive/warehouse
  echo "Created /user/hive/warehouse hdfs dir"

  hdfs dfs -chmod g+w /user/hive
  echo "Granted write to user group /user/hive hdfs dir"

  hdfs dfs -chmod g+w /user/hive/warehouse
  echo "Granted write to user group /user/hive hdfs dir"

  echo "Setting DB Type"
  $HIVE_HOME/bin/schematool -dbType derby -initSchema
  echo "Set DB Type"

  # start hiveserver2
  echo "Starting hiveserver2"
  $HIVE_HOME/bin/hiveserver2
  echo "hiveserver2 Started with success"
fi

tail -f /dev/null
