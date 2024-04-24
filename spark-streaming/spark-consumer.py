import time

from pyspark.sql import functions as F
from pyspark.sql import SparkSession
from pyspark.sql.types import *

def _load_spark_config(app_name):
    """
        Load Spark session
    """
    builder = SparkSession.builder.appName(app_name)

    spark = builder.getOrCreate()

    return spark

def main():
    app_name = 'hieu-stream'
    spark = _load_spark_config(app_name)

    log4jLogger = spark._jvm.org.apache.log4j
    logger = log4jLogger.LogManager.getLogger(__name__)
    logger.info("Pyspark script logger initialized")

    df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9092") \
    .option("subscribe", "hieuung") \
    .option("kafka.group.id", "spark_streaming") \
    .option("includeHeaders", "true") \
    .option("startingOffsets", "latest") \
    .load()

    df.printSchema()

    value_schema =  ArrayType(StructType([  
    StructField("id", StringType(), True),
    StructField("value", StringType(), True),
    StructField("type", StringType(), True),
    StructField("timestamp", StringType(), True)     
    ]))

    # # Converting the data into string
    # message_value_df = df.selectExpr("CAST(value AS STRING)")

    # Converting the json data into the schema created above and naming the column data 
    parsed_df = df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)") \
                .select(F.col("key"),
                        F.from_json(F.col("value"), value_schema).alias("data")) 
    

        
    parsed_df.printSchema()

    # Exploding the data column into elements
    parsed_df = parsed_df.select(F.col("key"),
                                 F.explode(F.col("data")).alias('elements'))

    parsed_df.printSchema()

    parsed_df = parsed_df.select(
                            parsed_df.key,
                            parsed_df.elements.id.alias("ID"),
                            parsed_df.elements.value.alias("Value"),
                            parsed_df.elements.type.alias("Type"),
                            parsed_df.elements.timestamp.alias("Timestamp"),
                            )

    parsed_df.printSchema()

    # Milisecond: Integer Type
    parsed_df = parsed_df.withColumn("Millisecond", parsed_df['Timestamp'].cast(LongType()))

    # Timestamp: Timestamp
    parsed_df = parsed_df.withColumn("Timestamp", F.from_unixtime(parsed_df['Millisecond'] / 1000))

    # Value: Integer Type
    parsed_df = parsed_df.withColumn('Value', parsed_df['Value'].cast(IntegerType()))

    parsed_df.printSchema()

    wirte_df = parsed_df.select(
                    parsed_df.key, \
                    F.to_json(F.struct("ID","Value","Type","Millisecond","Timestamp")).alias("value")
                    )

    out = wirte_df \
        .writeStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "kafka:9092") \
        .option("topic", "spark-write") \
        .outputMode("append")\
        .option("checkpointLocation", "/opt/spark2kafka-checkpoint/")\
        .start()

    out.awaitTermination()
    
if __name__ == '__main__':
    main()
