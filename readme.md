# SPARK STREAMING ITERATION WITH KAFKA

## Tech stack
- Kafka, Kafka-ui
- Spark streaming
- Docker-compose

## Process
### Setup Kafka.
```sh
docker-compose up -d kafka zookeeper kafka-ui
```
Verify Kafka cluster on [Kafka-ui](http://localhost:8080)

### Publish message.
```sh
docker-compose up -d producer
```
Verify message on [Kafka-ui](http://localhost:8080)

### Setup spark-cluster.
```sh
docker-compose up -d spark-master spark-worker
```
---
> **_NOTE:_**  If the docker image build is too slow (cause by slow downloading), you should download the following file
```sh
export SPARK_VERSION=3.0.2
export HADOOP_VERSION=3.2
export SPARK_HOME=/opt/spark
https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz
```
---

#### Verify spark cluster on [Spark-ui](http://localhost:9090)

#### Access master node to start the streaming job. Transform and Publish to Source (Kafka)

```sh
docker exec -it spark-master /bin/sh
```

#### Submit job
```sh
/opt/spark/bin/spark-submit --master spark://spark-master:7077 \ --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.0.2 \ /opt/ spark-apps/spark-consumer.py
```

#### Verify spark job on [Spark-ui](http://localhost:9090)
#### Verify message on [Kafka-ui](http://localhost:8080)
