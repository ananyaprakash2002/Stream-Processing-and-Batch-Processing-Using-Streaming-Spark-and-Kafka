from pyspark.sql import SparkSession
from pyspark.sql.functions import window
import pyspark.sql.functions as func
from pyspark.sql.functions import split, current_timestamp


spark = SparkSession.builder \
    .appName("test1") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.0") \
    .getOrCreate()
    
# Read data from the Kafka topic
df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "positive") \
    .option("startingOffsets", "earliest") \
    .option("includeHeaders", "true") \
    .load()

print(" Consumer Stream Processing has Started")

# Extract the tweet and hashtags from the Kafka message value
df1 = df.selectExpr("CAST(value AS STRING)", "topic")
df2 = df1.withColumn('tweet', split(df1['value'], '\s+', limit=3).getItem(1)) \
        .withColumn('hashtags', split(df1['value'], '\s+', limit=3).getItem(2)) \
        .withColumn("timestamp", current_timestamp()) 

# Create a temporary view of the dataframe
df2.createOrReplaceTempView("tweets")

print("Temporary view has been created!")
# Filtering dataframe to include only tweets with hashtag and having topic as positive
filtered_df = spark.sql("SELECT * FROM tweets WHERE hashtags != '' AND topic = 'positive'")

# Grouping tweets by a 30-minute window and counting the number of tweets in each window
count_df = filtered_df \
            .groupBy(window("timestamp","30 minutes")) \
            .agg(func.count("hashtags").alias("count"))

# Starting the streaming query to write the results to the console
query = count_df \
    .writeStream \
    .outputMode("complete") \
    .format("console") \
    .option("truncate", "false") \
    .option("checkpointLocation", "/tmp/checkpoint") \
    .start()

query.awaitTermination()
