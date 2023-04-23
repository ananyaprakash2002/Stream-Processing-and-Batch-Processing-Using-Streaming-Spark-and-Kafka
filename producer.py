import pandas as pd
from kafka import KafkaProducer
import time
import numpy as np
import json

KAFKA_TWEET_TOPIC_NAME = "tweets"
KAFKA_SENTIMENT_TOPIC_NAME = "sentiments"
KAFKA_TOPIC3_NAME = "topic3"  # set the correct topic name
KAFKA_BOOTSTRAP_SERVERS_CONS = "localhost:9092"

def send_three_topic_message(tweet_list):
    kafka_producer_obj = KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS_CONS,
        value_serializer=lambda x: json.dumps(x).encode("utf-8"),
    )

    for message in tweet_list:
        tweet_kafka = message["Tweet"]
        sentiment_kafka = str(message["Sentiment"])
        topic3_kafka = message["topic3"]

        print("Tweet: ", tweet_kafka)
        print("Sentiment: ", sentiment_kafka)
        print("Topic3: ", topic3_kafka)

        kafka_producer_obj.send(KAFKA_TWEET_TOPIC_NAME, value=tweet_kafka)
        kafka_producer_obj.send(KAFKA_SENTIMENT_TOPIC_NAME, value=sentiment_kafka)
        kafka_producer_obj.send(KAFKA_TOPIC3_NAME, value=topic3_kafka)
        time.sleep(2)

    kafka_producer_obj.flush()
    kafka_producer_obj.close()

if __name__ == "__main__":
    print("Kafka Producer Application Started ... ")
    filepath = "/home/pes1ug19cs222/Desktop/PES/DBT/DBT_Project/test.csv"
    tweet_df = pd.read_csv(filepath)
    tweet_df["order_id"] = np.arange(len(tweet_df))
    tweet_list = tweet_df.to_dict(orient="records")
    send_three_topic_message(tweet_list)
    print("Kafka Producer Application Completed. ")
