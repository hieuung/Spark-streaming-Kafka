# STREAMING ITERGRATION WITH KAFKA

## Tech stack
- Kafka, Kafka-ui
- Apache Spark
- Apache Flink
- Apache Beam
- Docker-compose

## Implementation
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

### STREAMING WITH APACHE SPARK. Setup long-running spark-cluster.
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

#### Verify Spark cluster on [Spark-ui](http://localhost:9090)

#### Access master node to start the streaming job. Transform and Publish to Source (Kafka)

```sh
docker exec -it spark-master /bin/sh
```

#### Submit job
```sh
/opt/spark/bin/spark-submit --master spark://spark-master:7077 \ --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.0.2 \ /opt/ spark-apps/spark-consumer.py
```

#### Verify spark job on [Spark-ui](http://localhost:9090), verify message on [Kafka-ui](http://localhost:8080)

### STREAMING WITH APACHE FLINK. 

#### Setup long-running Flink-cluster.
##### Download essential `external_jars` 
```
    flink-sql-connector-kafka-3.1.0-1.18.jar
```
##### Build pyflink docker image
```sh
docker build --tag pyflink:latest ./pyflink
```
##### Start Flink cluster
```sh
docker-compose up -d taskmanager jobmanager
```
---

##### Verify Flink cluster on [Flink-ui](http://localhost:8081)

#### Access taskmanager node to start the streaming job.
```sh
docker exec -it streaming-kakfa_jobmanager_1 /bin/sh
```

#### Submit job
```sh
./bin/flink run -py ./app/test-stream.py \ 
--jarfile ./app/external_jars/flink-sql-connector-kafka-3.1.0-1.18.jar
```

##### Verify Flink job on [Flink-ui](http://localhost:8081), verify message on [Kafka-ui](http://localhost:8080)

### STREAMING WITH APACHE BEAM.

#### Setup long-running Flink-cluster (or Spark-cluster).

#### Verify Flink cluster on [Flink-ui](http://localhost:8081)

#### Submit Beam job to cluster
```sh
python3 beam-consumer.py --runner FlinkRunner \
                        --bootstrap_servers localhost:9092 \
                        --topics hieuung \
                        --flink_master localhost:8081 \
```

#### Verify Flink job on [Flink-ui](http://localhost:8081)
