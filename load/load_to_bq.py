import os
from google.cloud import bigquery, storage
from google.cloud.exceptions import NotFound
from config.gcp_auth import get_bigquery_client, get_secret_manager_client


def access_secret(secret_id: str, project_id: str) -> str:
    client = get_secret_manager_client()
    name = f"projects/{project_id}/secrets/{secret_id}/versions/latest"
    response = client.access_secret_version(request={"name": name})
    return response.payload.data.decode("UTF-8")


def is_ndjson_format(blob) -> bool:
    """
    Quickly check whether the GCS blob content starts with '{' (valid NDJSON) or '[' (invalid array).
    """
    try:
        sample = blob.download_as_text(start=0, end=1)
        return sample.startswith("{")
    except Exception as e:
        print(f"❌ Failed to read blob {blob.name}: {e}")
        return False


def load_to_bigquery():
    project_id = os.getenv("GCP_PROJECT_ID")
    dataset_id = os.getenv("BQ_DATASET")
    table_name = os.getenv("BQ_TABLE")
    bucket_name = os.getenv("GCS_BUCKET")

    if not all([project_id, dataset_id, table_name]):
        raise ValueError("❌ Missing one or more required env vars: GCP_PROJECT_ID, BQ_DATASET, BQ_TABLE")

    table_id = f"{project_id}.{dataset_id}.{table_name}"
    client = get_bigquery_client()

    # ✅ Ensure dataset
    try:
        client.get_dataset(dataset_id)
        print(f"✅ Dataset {dataset_id} exists.")
    except NotFound:
        dataset = bigquery.Dataset(f"{project_id}.{dataset_id}")
        dataset.location = "US"
        client.create_dataset(dataset)
        print(f"✅ Dataset {dataset_id} created.")

    # ✅ Ensure table (will be auto-created on load if not found)
    try:
        client.get_table(table_id)
        print(f"✅ Table {table_id} exists.")
    except NotFound:
        print(f"⚠️ Table {table_id} not found. It will be created on load.")

    # ✅ Filter and validate files from GCS
    valid_uris = []
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)

    print("🔍 Scanning GCS for valid NDJSON files...")
    for blob in bucket.list_blobs(prefix="reddit/processed/sentiment/"):
        if not blob.name.endswith(".json") or "enriched_" not in blob.name:
            continue
        if is_ndjson_format(blob):
            valid_uris.append(f"gs://{bucket_name}/{blob.name}")
        else:
            print(f"❌ Skipping malformed file: {blob.name}")

    if not valid_uris:
        print("⚠️ No valid NDJSON files found to load.")
        return

    print(f"📥 Loading {len(valid_uris)} file(s) into {table_id}...")

    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
        autodetect=True,
        write_disposition="WRITE_APPEND",
    )

    load_job = client.load_table_from_uri(valid_uris, table_id, job_config=job_config)
    load_job.result()  # Wait for job to complete

    table = client.get_table(table_id)
    print(f"✅ Load complete. {table.num_rows} total rows in {table_id}.")


if __name__ == "__main__":
    load_to_bigquery()
