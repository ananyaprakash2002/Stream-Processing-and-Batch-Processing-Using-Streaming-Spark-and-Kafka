from pyspark.sql import SparkSession   
from pyspark.sql.functions import split, udf, col
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
import mysql.connector
import re

spark = SparkSession.builder.appName("test").getOrCreate()

hostname = "localhost"
user = "root"
password = "99tkoffice" # whatever you have set as your password in mysql
database = "dbt" # or whatever you named your database in mysql
table_name = "tweets"


def drop_table_if_exists(tablename):
    db = mysql.connector.connect(host=hostname, user=user, password=password, database=database)
    cursor = db.cursor()
    sql = f"DROP TABLE IF EXISTS {tablename}"
    cursor.execute(sql)
    db.close()


def lowercase(s):
    return s.lower()
    
def remove_digits(s):
    return ''.join(c for c in s if not c.isdigit())

    
def remove_punctuations(s):
    return re.sub(r'[^\w\s]', '', s)


@udf(returnType = StringType())
def extract_hashtags(s):
    hashtags = re.findall(r'#(\w+)', s)
    return '#' + ' #'.join(hashtags)

@udf(returnType = StringType())
def preprocess(s):
    news = lowercase(s)
    news = remove_digits(news)
    news = remove_punctuations(news)
    return news

# Define the schema of the data
schema = StructType([
    StructField("value", StringType(), True),
    StructField("topic", StringType(), True)
])

# Read data from the Kafka topics
df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "tweets, sentiments, topic3") \
    .option("startingOffsets", "earliest") \
    .option("includeHeaders", "true") \
    .load()

query = df.selectExpr("CAST(value AS STRING)", "topic")
df1 = query \
    .withColumn('topic1', split(query['value'], '\s+', limit=3).getItem(0)) \
    .withColumn('topic2', split(query['value'], '\s+', limit=3).getItem(1)) \
    .withColumn('topic3', split(query['value'], '\s+', limit=3).getItem(2))


db_target_properties = {"user":user, "password":password}

# Function to write the data to MySQL
def foreach_batch_function(df, epoch_id):
    df.write.mode("append").jdbc(url=f'jdbc:mysql://localhost:3306/{database}',  table=table_name,  properties=db_target_properties)

data = df1.select("sentiment", "tweet", "topic3")
data = data.withColumn("hashtags", extract_hashtags(col("tweet")))
data = data.withColumn("preprocess", preprocess(col("tweet")))

# Drop the table if it exists and start the streaming query
drop_table_if_exists(table_name)
out = data.writeStream.outputMode("append").foreachBatch(foreach_batch_function).start()
out.awaitTermination()
