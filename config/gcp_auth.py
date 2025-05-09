import os
from google.cloud import storage, bigquery, secretmanager
from google.oauth2 import service_account


def get_gcp_credentials():
    creds_path = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
    if not creds_path or not os.path.isfile(creds_path):
        raise FileNotFoundError(f"❌ GOOGLE_APPLICATION_CREDENTIALS not set or file not found at: {creds_path}")

    print(f"✅ Using GCP credentials from: {creds_path}")
    return service_account.Credentials.from_service_account_file(creds_path)


def get_storage_client():
    credentials = get_gcp_credentials()
    return storage.Client(credentials=credentials)


def get_bigquery_client():
    credentials = get_gcp_credentials()
    return bigquery.Client(credentials=credentials)


def get_secret_manager_client():
    credentials = get_gcp_credentials()
    return secretmanager.SecretManagerServiceClient(credentials=credentials)
