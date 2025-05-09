import os
import json
from datetime import datetime
from textblob import TextBlob
from config.gcp_auth import get_storage_client
from config.settings import PROJECT_ID, get_secret
from dotenv import load_dotenv

load_dotenv()


def analyze_sentiment():

    gcs_bucket = os.environ.get("GCS_BUCKET")
    client = get_storage_client()
    bucket = client.bucket(gcs_bucket)

    # List raw files
    blobs = list(bucket.list_blobs(prefix="reddit/raw/"))

    if not blobs:
        print("❌ No raw files found.")
        return

    for blob in blobs:
        print(f"🔍 Processing file: {blob.name}")
        raw_data = blob.download_as_text()
        posts = json.loads(raw_data)

        enriched_posts = []
        for post in posts:
            text = post.get("title", "") + " " + post.get("body", "")
            sentiment = TextBlob(text).sentiment
            post["sentiment"] = {
                "polarity": sentiment.polarity,
                "subjectivity": sentiment.subjectivity
            }
            enriched_posts.append(post)

        # Upload to processed/sentiment
        out_blob = bucket.blob(f"reddit/processed/sentiment/enriched_{datetime.utcnow().isoformat()}.json")
        out_blob.upload_from_string(json.dumps(enriched_posts), content_type="application/json")
        print(f"✅ Uploaded enriched sentiment file: {out_blob.name}")


if __name__ == "__main__":
    analyze_sentiment()
