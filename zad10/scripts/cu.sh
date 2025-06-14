#!/usr/bin/env bash

hadoop_version="3.3.5"
spark_version="3.4.0"
slaves="3"
jupyter_workspace_path="../notebooks"
hdfs_path="/tmp/hdfs"
hadoop_log_path="/tmp/hadoop_logs"
spark_log_path="/tmp/spark_logs"

if [ -z $spark_log_path ]
then
  echo "Some parameter value is empty. Usage: compose-up.sh <hadoop_version> <spark_version> <(The # of)slaves [integer]> <jupyter_workspace_path> <hdfs_path> <hadoop_log_path> <spark_log_path>"
  exit 1
fi

if [[ ! $slaves =~ ^-?[0-9]+$ ]]
then
  echo "The # of slaves is not integer."
  exit 1
elif [[ $slaves -le 1 ]]
then
  slaves=1
elif [[ $slaves -gt 5 ]]
then
  slaves=5
fi

echo "Set docker-compose.yml file."

for slave in $(seq 1 $slaves)
do
  workers+=slave$slave
  if [[ ! $slave -eq $slaves ]]
  then
    workers+='
'
  fi
done

cat << EOF > workers
master
$workers
EOF

for slave in $(seq 1 $slaves)
do
  ip_addr+='      - "'slave$slave':10.0.2.'$(($slave + 4))'"
'
done

for slave in $(seq 1 $slaves)
do
  slave_service+='  'slave$slave':
    image: hjben/hadoop:'$hadoop_version'-jdk1.8.0
    hostname: 'slave$slave'
    container_name: 'slave$slave'
    cgroup: host
    privileged: true
    ports:
      - '$slave'4040:4040
      - '$slave'8042:8042
      - '$slave'8080:8080
      - '$slave'8088:8088
    volumes:
      - /sys/fs/cgroup:/sys/fs/cgroup:rw
    networks:
      hadoop-cluster:
        ipv4_address: 10.0.2.'$(($slave+4))'
    extra_hosts:
      - "jupyter-lab:10.0.2.10"
      - "spark:10.0.2.4"
      - "master:10.0.2.3"
'$ip_addr
  if [[ ! $slave -eq $slaves ]]
  then
    slave_service+='
'
  fi
done

cat << EOF > docker-compose.yml
services:
  jupyter-lab:
    image: hjben/jupyter-lab:spark-$spark_version
    hostname: jupyter-lab
    container_name: jupyter-lab
    cgroup: host
    privileged: true
    ports:
      - 8888:8888
      - 4040-4044:4040-4044
    volumes:
      - /sys/fs/cgroup:/sys/fs/cgroup:rw
      - $jupyter_workspace_path:/root/workspace
    networks:
      hadoop-cluster:
        ipv4_address: 10.0.2.10
    extra_hosts:
      - "jupyter-lab:10.0.2.10"
      - "spark:10.0.2.4"
      - "master:10.0.2.3"
$ip_addr
  spark:
    image: hjben/spark:$spark_version-livy
    hostname: spark
    container_name: spark
    cgroup: host
    privileged: true
    ports:
      - 8080-8081:8080-8081
      - 8998:8998
    volumes:
      - /sys/fs/cgroup:/sys/fs/cgroup:rw
      - $jupyter_workspace_path:/root/workspace
      - $spark_log_path/master:/usr/local/spark/logs 
    networks:
      hadoop-cluster:
        ipv4_address: 10.0.2.4
    extra_hosts:
      - "jupyter-lab:10.0.2.10"
      - "spark:10.0.2.4"
      - "master:10.0.2.3"
$ip_addr
  master:
    image: hjben/hadoop:$hadoop_version-jdk1.8.0
    hostname: master
    container_name: master
    cgroup: host
    privileged: true
    ports:
      - 8088:8088
      - 9870:9870
      - 8042:8042
    volumes:
      - /sys/fs/cgroup:/sys/fs/cgroup:rw
      - $hdfs_path:/data/hadoop
      - $hadoop_log_path:/usr/local/hadoop/logs
    networks:
      hadoop-cluster:
        ipv4_address: 10.0.2.3
    extra_hosts:
      - "jupyter-lab:10.0.2.10"
      - "spark:10.0.2.4"
      - "master:10.0.2.3"
$ip_addr
$slave_service
  loader:
    image: python:3.13
    hostname: loader
    container_name: loader
    entrypoint: bash -c "curl -LsSf https://astral.sh/uv/install.sh | sh && tail -f /dev/null"
    tty: true
    networks:
      hadoop-cluster:
        ipv4_address: 10.0.2.100
    volumes:
      - ../../data:/root/data
      - ../../zad6/loader:/root/loader
networks:
 hadoop-cluster:
  ipam:
   driver: default
   config:
   - subnet: 10.0.2.0/24
EOF
echo "Done."

echo "Docker-compose container run."
echo "Remove old containers."
docker compose down --remove-orphans
sleep 1

echo "Create new containers."
docker compose up -d
sleep 1

docker cp ./workers master:/usr/local/hadoop/etc/hadoop/workers
docker exec -it master bash -c "rm -rf /run/nologin"

for slave in $(seq 1 $slaves)
do
  docker cp ./workers slave$slave:/usr/local/hadoop/etc/hadoop/workers
  docker exec -it slave$slave bash -c "rm -rf /run/nologin"
done
echo "Done."

rm -f workers

echo "Start Livy service."
docker exec -it spark bash -c "livy-server start"

echo "Start Hadoop service."
docker exec -it master bash -c "/sh/start-all.sh"
