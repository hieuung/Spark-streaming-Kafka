/opt/spark/bin/spark-submit --master spark://spark-master:7077 --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.0.2 /opt/spark-apps/spark-consumer.py > log.txt
