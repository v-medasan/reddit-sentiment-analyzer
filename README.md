# Reddit Sentiment Analyzer

An end-to-end pipeline that collects Reddit posts, performs sentiment analysis using Spark NLP, and loads the output to Google BigQuery — all orchestrated with Prefect.

## 📌 Features
- Pulls hot Reddit posts from multiple subreddits
- Sentiment scored with TextBlob (or VADER)
- Spark used for parallel NLP
- Hosted on Google Cloud (free tier friendly)
- Visualized with Google Data Studio

## 🧰 Stack
- Prefect (Orchestration)
- PySpark (Processing)
- TextBlob (Sentiment)
- Google Cloud Storage + BigQuery (Storage)

## 📂 Structure
See folder structure in the repo — all modules are independent and easy to plug into production pipelines.

## 🚀 Running the Flow

```bash
prefect deployment build flows/reddit_etl_flow.py:reddit_sentiment_pipeline -n "reddit-sentiment"
prefect deployment apply reddit_sentiment_pipeline-deployment.yaml
prefect agent start --work-queue default
