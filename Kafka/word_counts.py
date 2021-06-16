import os
import  findspark


findspark.init()


from pyspark.sql import SparkSession
from pyspark.sql.functions import explode
from pyspark.sql.functions import split




os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-streaming-kafka-0-10_2.12:3.0.0-preview2,' \
                                    'org.apache.spark:spark-sql-kafka-0-10_2.12:3.0.0-preview2 pyspark-shell '


spark = SparkSession \
    .builder \
    .appName("WordCount") \
    .getOrCreate()

df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "TOPIC_TEST") \
    .option("startingOffsets", "earliest") \
    .load()

#df1 = df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
#Split the lines into words
words = df.select(
    #explode turns each item in an array into a separate row
    explode(
        split(df.value, ' ')
    ).alias('word')
)
#Generate a running word count
wordCounts = words.groupBy('word').count()

query =wordCounts  \
    .writeStream \
    .outputMode("complete") \
    .format("console") \
    .start()

query.awaitTermination()