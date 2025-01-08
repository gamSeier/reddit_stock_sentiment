import re
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, udf, from_unixtime, to_timestamp
from pyspark.sql.types import StructType, StringType, FloatType, IntegerType, LongType, ArrayType
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer
import pandas as pd
from pymongo import MongoClient
from pymongo.server_api import ServerApi
import certifi

uri = "mongodb+srv://@redditfinalproject.wlts7.mongodb.net/?retryWrites=true&w=majority&appName=RedditFinalProject"

client = MongoClient(uri, server_api=ServerApi('1'), tlsCAFile=certifi.where())

db = client["redditfinalproject"]
collection = db["finance"]

spark = SparkSession.builder \
    .appName("RedditSentimentAnalysis") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.0") \
    .getOrCreate()

analyzer = SentimentIntensityAnalyzer()


def get_sentiment(text):
    if text:
        return analyzer.polarity_scores(text)["compound"]
    return 0.0


sentiment_udf = udf(get_sentiment, FloatType())

members = pd.read_csv('constituents.csv')
broadcast_tickers = spark.sparkContext.broadcast(set(members.Symbol))


def extract_tickers(text):
    # text = f"{title} {body}"

    dollar_tickers = set(re.findall(r'\$[A-Z]{1,5}', text))
    dollar_tickers = {ticker.strip('$') for ticker in dollar_tickers}

    word_tickers = set(re.findall(r'\b[A-Z]{1,5}\b', text))
    word_tickers = word_tickers.intersection(broadcast_tickers.value)

    return list(dollar_tickers.union(word_tickers))


tickers_udf = udf(extract_tickers, ArrayType(StringType()))

schema = StructType() \
    .add("subreddit", StringType()) \
    .add("body", StringType()) \
    .add("comment_author", StringType()) \
    .add("created_utc", LongType()) \
    .add("post_title", StringType()) \
    .add("score", LongType())

spark.sparkContext.setLogLevel("ERROR")

df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "reddit") \
    .option("startingOffsets", "latest") \
    .load()

parsed_df = df.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), schema).alias("data")) \
    .select("data.*")

df_with_sentiment = (parsed_df
                     .withColumn("sentiment_score", sentiment_udf(col("body")))
                     .withColumn("date", to_timestamp(from_unixtime(col("created_utc")))))

final_df = df_with_sentiment.withColumn("tickers", tickers_udf(col('body')))

query = final_df.writeStream \
    .outputMode("append") \
    .format("console") \
    .trigger(processingTime="10 seconds") \
    .start()


def write_to_mongo(batch_df, _):
    records = batch_df.toPandas().to_dict(orient="records")
    if records:
        collection.insert_many(records)


query = final_df.writeStream \
    .foreachBatch(write_to_mongo) \
    .outputMode("update") \
    .start()

query.awaitTermination()
