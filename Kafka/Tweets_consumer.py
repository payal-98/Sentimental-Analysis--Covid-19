from kafka import KafkaConsumer
consumer = KafkaConsumer('Tweets_file')
for message in consumer:
    print (message)
