from pyspark.sql import SparkSession
from pyspark.sql.functions import split, window, count
import pyspark.sql.functions as func

spark = SparkSession.builder.appName("test").getOrCreate()

# Read data from the Kafka topic
df = spark \
    .read \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "tweets") \
    .option("startingOffsets", "earliest") \
    .option("endingOffsets", "latest") \
    .option("includeHeaders", "true") \
    .load()

# Extract the tweet and hashtags from the Kafka message value
df1 = df.selectExpr("CAST(value AS STRING)") \
        .withColumn('tweet', split(df['value'], '\s+', limit=3).getItem(1)) \
        .withColumn('hashtags', split(df['value'], '\s+', limit=3).getItem(2))

# Create a temporary view of the dataframe
df1.createOrReplaceTempView("tweets")

# Filter the dataframe to include only positive tweets
positive_df = spark.sql("SELECT * FROM tweets WHERE topic = 'positive'")

# Group the tweets by a 15-minute window and count the number of hashtags in each window
count_df = positive_df \
            .groupBy(window("timestamp", "15 minutes"), "hashtags") \
            .agg(func.count("hashtags").alias("count"))

# Write the results to a CSV file
count_df \
    .coalesce(1) \
    .write \
    .format("csv") \
    .mode("overwrite") \
    .option("header", "true") \
    .save("/path/to/output/folder")
