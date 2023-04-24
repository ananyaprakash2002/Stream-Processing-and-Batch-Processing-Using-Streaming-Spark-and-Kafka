import pandas as pd
from kafka import KafkaProducer
import time
import numpy as np
import json

KAFKA_TOPIC_1 = "Positive"
KAFKA_TOPIC_2 = "Negative"
KAFKA_TOPIC_3 = "Neutral" 
KAFKA_BOOTSTRAP_SERVERS_CONS = "localhost:9092"

def send_three_topic_message(tweet_list):
    kafka_producer_obj = KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS_CONS,
        value_serializer=lambda x: json.dumps(x).encode("utf-8"))

    for message in tweet_list:
        tweet_kafka = message["tweet"]
        sentiment_kafka = str(message["sentiment"])

        if sentiment_kafka == 'positive':
            kafka_producer_obj.send(KAFKA_TOPIC_1, value=tweet_kafka)

        elif sentiment_kafka == 'negative':
            kafka_producer_obj.send(KAFKA_TOPIC_2, value=tweet_kafka)

        else:
            kafka_producer_obj.send(KAFKA_TOPIC_3, value=tweet_kafka)

        print("Tweet: ", tweet_kafka)
        print("Sentiment: ", sentiment_kafka)
        time.sleep(2)
    kafka_producer_obj.flush()
    kafka_producer_obj.close()

if __name__ == "__main__":
    print("Kafka Producer Application Started ... ")
    filepath = "/path/to/your/dataset.csv"
    tweet_df = pd.read_csv(filepath, usecols=['tweet', 'sentiment'])
    tweet_list = tweet_df.to_dict(orient="records")
    send_three_topic_message(tweet_list)
    print("Kafka Producer Application Completed. ")