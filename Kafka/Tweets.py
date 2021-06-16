from json import dumps
import tweepy
from tweepy import OAuthHandler
from tweepy import Stream
from tweepy.streaming import StreamListener
import os
import  findspark
findspark.init()
from pyspark.sql import SparkSession
from pyspark.sql.functions import explode
from pyspark.sql.functions import split
from kafka import KafkaProducer

os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-streaming-kafka-0-10_2.12:3.0.0-preview2,' \
                                    'org.apache.spark:spark-sql-kafka-0-10_2.12:3.0.0-preview2 pyspark-shell '

consumer_key = ''
consumer_secret = ''
access_token = ''
access_secret = ''

# Initialize Global variable
tweet_count = 0
# Input number of tweets to be downloaded
n_tweets = 10
msg="    "


auth=tweepy.OAuthHandler(consumer_key,consumer_secret)
auth.set_access_token(access_token,access_secret)

api = tweepy.API(auth)
#producer = KafkaProducer(bootstrap_servers='localhost:9092')
producer = KafkaProducer(bootstrap_servers='localhost:9092', value_serializer=lambda x: dumps(x).encode('utf-8'))

class StreamListener(tweepy.StreamListener):
    def on_status(self, status):
        producer.send('Tweets_file',status.text)

    def on_error(self, status_code):
        if status_code == 420:
            return False

stream_listener = StreamListener()
stream = tweepy.Stream(auth=api.auth, listener=stream_listener)
stream.filter(track=["corona"])





