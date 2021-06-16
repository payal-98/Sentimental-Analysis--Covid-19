import os
import  findspark
findspark.init()


from pyspark.sql import SparkSession




os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-streaming-kafka-0-10_2.12:3.0.0-preview2,' \
                                    'org.apache.spark:spark-sql-kafka-0-10_2.12:3.0.0-preview2 pyspark-shell '


spark = SparkSession \
    .builder \
    .appName("SSKafka") \
    .getOrCreate()

df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "TOPIC_TEST") \
    .option("startingOffsets", "earliest") \
    .load()

df1=df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")

query = df1 \
    .writeStream \
    .outputMode("append") \
    .format("console") \
    .start()

query.awaitTermination()