from pyspark.sql import SparkSession
from pyspark.sql.functions import split, window, count
import pyspark.sql.functions as func

spark = SparkSession.builder.appName("test3").getOrCreate()

# Read data from the database table
jdbcUrl = "jdbc:mysql://localhost:3306/dbt"
connectionProperties = {
  "user": "dbtuser",
  "password": "root"
}
df = spark.read.jdbc(url=jdbcUrl, table="tweets", properties=connectionProperties)

# Extract the tweet and hashtags from the value column
df1 = df.withColumn('tweet', split(df['value'], '\s+', limit=3).getItem(1)) \
        .withColumn('hashtags', split(df['value'], '\s+', limit=3).getItem(2))

# Create a temporary view of the dataframe
df1.createOrReplaceTempView("tweets_view")

# Filter the dataframe to include only positive tweets
positive_df = spark.sql("SELECT * FROM tweets_view WHERE topic = 'positive'")

# Group the tweets by a 30-minute window and count the number of hashtags in each window
count_df = positive_df \
            .groupBy(window("timestamp", "30 minutes"), "hashtags") \
            .agg(func.count("hashtags").alias("count"))

# Write the results to terminal
count_df.show()

