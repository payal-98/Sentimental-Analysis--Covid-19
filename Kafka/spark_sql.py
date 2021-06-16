import findspark
findspark.init()
import pyspark as ps
import warnings
from pyspark.sql import SQLContext
import os
from pyspark.sql import SparkSession


os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-streaming-kafka-0-10_2.12:3.0.0-preview2,' \
                                    'org.apache.spark:spark-sql-kafka-0-10_2.12:3.0.0-preview2 pyspark-shell '


spark = SparkSession \
    .builder \
    .appName("Tweets_kafka") \
    .getOrCreate()

df = spark \
    .read \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "Tweets_file") \
    .option("startingOffsets", "earliest") \
    .load()

df1=df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")

df1.show(5)

length = df1.count()

lower_case=df1.lower()
print(lower_case)
