from pyspark.sql import SparkSession
from pyspark.sql.functions import split, window, count
import pyspark.sql.functions as func

spark = SparkSession.builder.appName("test").getOrCreate()

# Read data from the Kafka topic
df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "tweets") \
    .option("startingOffsets", "earliest") \
    .option("includeHeaders", "true") \
    .load()

# Extract the tweet and hashtags from the Kafka message value
df1 = df.selectExpr("CAST(value AS STRING)") \
        .withColumn('tweet', split(df['value'], '\s+', limit=3).getItem(1)) \
        .withColumn('hashtags', split(df['value'], '\s+', limit=3).getItem(2))

# Filter the dataframe to include only tweets with a particular hashtag
filtered_df = df1.filter(df1.hashtags == '#myhashtag')

# Group the tweets by a 15-minute window and count the number of tweets in each window
count_df = filtered_df \
            .groupBy(window(df1.timestamp, "15 minutes")) \
            .agg(count("tweet").alias("count"))

# Start the streaming query to write the results to the console
query = count_df \
    .writeStream \
    .outputMode("complete") \
    .format("console") \
    .option("truncate", "false") \
    .option("checkpointLocation", "/tmp/checkpoint") \
    .start()

query.awaitTermination()
