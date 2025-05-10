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

    raw_prefix = "reddit/raw/"
    processed_prefix = "reddit/processed/sentiment/"

    # Fetch already-processed file names to skip duplicates
    processed_files = set(blob.name.split("/")[-1].replace("enriched_", "").replace(".json", "")
                          for blob in bucket.list_blobs(prefix=processed_prefix))

    raw_blobs = list(bucket.list_blobs(prefix=raw_prefix))
    if not raw_blobs:
        print("❌ No raw files found in GCS.")
        return

    for blob in raw_blobs:
        raw_filename = blob.name.split("/")[-1].replace("posts_", "").replace(".json", "")

        if raw_filename in processed_files:
            print(f"⏩ Already processed: {blob.name}")
            continue

        print(f"🔍 Processing: {blob.name}")
        try:
            raw_data = blob.download_as_text()
            posts = json.loads(raw_data)
        except Exception as e:
            print(f"❌ Failed to load JSON from {blob.name}: {e}")
            continue

        enriched_lines = []
        for post in posts:
            text = post.get("title", "") + " " + post.get("body", "")
            sentiment = TextBlob(text).sentiment
            post["sentiment"] = {
                "polarity": sentiment.polarity,
                "subjectivity": sentiment.subjectivity
            }
            enriched_lines.append(json.dumps(post))

        # Upload as NDJSON
        ndjson_string = "\n".join(enriched_lines)
        enriched_blob_name = f"{processed_prefix}enriched_{raw_filename}.json"
        enriched_blob = bucket.blob(enriched_blob_name)
        enriched_blob.upload_from_string(ndjson_string, content_type="application/json")

        print(f"✅ Uploaded enriched NDJSON: {enriched_blob_name}")

        # Cleanup raw file after success
        blob.delete()
        print(f"🗑️ Deleted raw: {blob.name}")


if __name__ == "__main__":
    analyze_sentiment()
