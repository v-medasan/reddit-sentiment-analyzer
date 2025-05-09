import os
from google.cloud import bigquery
from google.cloud.exceptions import NotFound
from config.gcp_auth import get_bigquery_client, get_secret_manager_client


def access_secret(secret_id: str, project_id: str) -> str:
    """
    Access a secret value from Google Secret Manager.
    """
    client = get_secret_manager_client()
    name = f"projects/{project_id}/secrets/{secret_id}/versions/latest"
    response = client.access_secret_version(request={"name": name})
    return response.payload.data.decode("UTF-8")


def load_to_bigquery():
    # Load required config from env or fail early
    project_id = os.getenv("GCP_PROJECT_ID")
    dataset_id = os.getenv("BQ_DATASET")
    table_name = os.getenv("BQ_TABLE")
    bucket_name = os.getenv("GCS_BUCKET")

    if not all([project_id, dataset_id, table_name]):
        raise ValueError("❌ Missing one or more required env vars: GCP_PROJECT_ID, BQ_DATASET, BQ_TABLE")

    table_id = f"{project_id}.{dataset_id}.{table_name}"
    gcs_uri = f"gs://{bucket_name}/reddit/processed/sentiment/*.json"

    client = get_bigquery_client()

    # ✅ Create dataset if it doesn't exist
    try:
        client.get_dataset(dataset_id)
        print(f"✅ Dataset {dataset_id} exists.")
    except NotFound:
        print(f"⚠️ Dataset {dataset_id} not found. Creating it...")
        dataset = bigquery.Dataset(f"{project_id}.{dataset_id}")
        dataset.location = "US"
        client.create_dataset(dataset)
        print(f"✅ Dataset {dataset_id} created.")

    # ✅ Create table if it doesn't exist (autodetect)
    try:
        client.get_table(table_id)
        print(f"✅ Table {table_id} exists.")
    except NotFound:
        print(f"⚠️ Table {table_id} not found. It will be created on load.")

    print(f"📥 Loading data from {gcs_uri} into {table_id}...")

    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
        autodetect=True,
        write_disposition="WRITE_APPEND",
    )

    load_job = client.load_table_from_uri(gcs_uri, table_id, job_config=job_config)
    load_job.result()

    table = client.get_table(table_id)
    print(f"✅ Load completed. Table now contains {table.num_rows} rows.")


if __name__ == "__main__":
    load_to_bigquery()
