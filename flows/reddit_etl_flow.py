from prefect import flow, task
from extract.fetch_reddit_posts import fetch_reddit_posts
from transform.analyze_sentiment import analyze_sentiment
from load.load_to_bq import load_to_bigquery
import os

@task
def run_spark():
    os.system("spark-submit process/sentiment_analysis.py")

@flow(name="reddit_sentiment_pipeline")
def reddit_sentiment_pipeline():
    fetch_reddit_posts()
    run_spark()
    analyze_sentiment()
    load_to_bigquery()

if __name__ == "__main__":
    reddit_sentiment_pipeline()
