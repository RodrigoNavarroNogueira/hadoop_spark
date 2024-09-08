#!/bin/bash

SPARK_WORKLOAD=$1

DATE=$(date "+%Y-%m-%d %H:%M:%S")

echo "$DATE INFO entrypoint-yarn.sh: Initializing the container $SPARK_WORKLOAD."

/etc/init.d/ssh start > /dev/null 2>&1

if [ "$SPARK_WORKLOAD" == "master" ];
then
    # Caminho do arquivo de flag
  FLAG_FILE="/opt/hadoop/data/flag/hdfs_format.flag"

  # Verifica se o HDFS já foi formatado
  if [ ! -f "$FLAG_FILE" ]; then  # SE O ARQUIVO N EXISTE..
      echo "$DATE INFO entrypoint-yarn.sh: Formatting HDFS."
      hdfs namenode -format > /dev/null 2>&1
      
      # Cria o arquivo de flag após a formatação
      touch "$FLAG_FILE"
      echo "$DATE INFO entrypoint-yarn.sh: HDFS formatted successfully."
  else
      echo "$DATE INFO entrypoint-yarn.sh: HDFS has already been formatted."
  fi

  # start the master node processes
  hdfs --daemon start namenode >> /dev/null 2>&1
  hdfs --daemon start secondarynamenode
  yarn --daemon start resourcemanager
  
  # create required directories
  while ! hdfs dfs -mkdir -p /spark-logs > /dev/null 2>&1;
  do
    echo "$DATE INFO entrypoint-yarn.sh: Failed creating /spark-logs hdfs dir"
  done
  echo "$DATE INFO entrypoint-yarn.sh: Created /spark-logs hdfs dir"
  hdfs dfs -mkdir -p /opt/spark/data
  echo "$DATE INFO entrypoint-yarn.sh: Created /opt/spark/data hdfs dir"

  SAFEMODE_STATUS=$(hdfs dfsadmin -safemode get)

  if [[ "$SAFEMODE_STATUS" == *"Safe mode is ON"* ]]; then
      echo "$DATE INFO entrypoint-yarn.sh: HDFS is in safe mode. Exiting safe mode."
      SAFEMODE_STATUS=$(hdfs dfsadmin -safemode leave)
      echo "$DATE INFO entrypoint-yarn.sh: $SAFEMODE_STATUS"
  else
      echo "$DATE INFO entrypoint-yarn.sh: HDFS is not in safe mode."
  fi

  # copy the data to the data HDFS directory
  hdfs dfs -copyFromLocal /opt/spark/data/* /opt/spark/data

  echo "$DATE INFO entrypoint-yarn.sh: The container $SPARK_WORKLOAD has been initialized"

  echo "$DATE INFO entrypoint-yarn.sh: The cluster initialized successfully!"

elif [ "$SPARK_WORKLOAD" == "worker" ];
then
  hdfs namenode -format > /dev/null 2>&1

  # start the worker node processes
  hdfs --daemon start datanode
  yarn --daemon start nodemanager

  echo "$DATE INFO entrypoint-yarn.sh: The container $SPARK_WORKLOAD has been initialized"

elif [ "$SPARK_WORKLOAD" == "history" ];
then

  while ! hdfs dfs -test -d /spark-logs > /dev/null 2>&1;
  do
    echo "$DATE INFO entrypoint-yarn.sh: spark-logs doesn't exist yet... retrying"
    sleep 0.5;
  done
  echo "$DATE INFO entrypoint-yarn.sh: /spark-logs directory created"

  echo "$DATE INFO entrypoint-yarn.sh: The container $SPARK_WORKLOAD has been initialized"

  # start the spark history server
  start-history-server.sh > /dev/null 2>&1
fi

tail -f /dev/null