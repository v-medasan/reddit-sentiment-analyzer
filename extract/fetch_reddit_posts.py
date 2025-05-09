from config.settings import PROJECT_ID, get_secret
from config.gcp_auth import get_storage_client
import praw, json, os
from datetime import datetime
from dotenv import load_dotenv

load_dotenv()

def fetch_reddit_posts():

    gcs_bucket = os.environ.get("GCS_BUCKET")
    client_id = get_secret("REDDIT_CLIENT_ID", PROJECT_ID)
    client_secret = get_secret("REDDIT_CLIENT_SECRET", PROJECT_ID)
    user_agent = get_secret("REDDIT_USER_AGENT", PROJECT_ID)

    reddit = praw.Reddit(
        client_id=client_id,
        client_secret=client_secret,
        user_agent=user_agent
    )

    reddit.read_only = True
    print("✅ Reddit API authenticated.")

    # Fetch posts
    posts = []
    for post in reddit.subreddit("technology").hot(limit=25):
        posts.append({
            "id": post.id,
            "title": post.title,
            "body": post.selftext,
            "created": datetime.utcfromtimestamp(post.created_utc).isoformat()
        })

    print(f"✅ Fetched {len(posts)} posts")

    # Upload to GCS
    client = get_storage_client()
    bucket = client.bucket(gcs_bucket)
    blob = bucket.blob(f"reddit/raw/posts_{datetime.utcnow().isoformat()}.json")
    blob.upload_from_string(json.dumps(posts), content_type="application/json")

    print("✅ Uploaded to GCS bucket:", gcs_bucket)


if __name__ == "__main__":
    fetch_reddit_posts()
