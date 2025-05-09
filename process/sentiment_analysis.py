from pyspark.sql import SparkSession
from textblob import TextBlob
from pyspark.sql.functions import udf, col
from pyspark.sql.types import FloatType

def get_sentiment(text):
    return TextBlob(text).sentiment.polarity

spark = SparkSession.builder.appName("RedditNLP").getOrCreate()
df = spark.read.json("gs://reddit-sentiment-bucket/reddit/raw/*.json")
get_sentiment_udf = udf(get_sentiment, FloatType())

df = df.withColumn("sentiment", get_sentiment_udf(col("title")))
df.write.mode("overwrite").json("gs://reddit-sentiment-bucket/reddit/processed/sentiment")
