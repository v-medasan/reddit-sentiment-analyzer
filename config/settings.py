import os
from dotenv import load_dotenv
from config.gcp_auth import get_secret_manager_client

# Load from .env if present
load_dotenv()


def get_env_var(var_name: str, required: bool = True) -> str:
    value = os.getenv(var_name)
    if required and not value:
        raise EnvironmentError(f"❌ Required environment variable '{var_name}' is not set.")
    return value


def get_secret(secret_id: str, project_id: str) -> str:
    """
    Fetch a secret from Secret Manager.
    """
    client = get_secret_manager_client()
    name = f"projects/{project_id}/secrets/{secret_id}/versions/latest"
    response = client.access_secret_version(request={"name": name})
    return response.payload.data.decode("UTF-8")


# Load core config
PROJECT_ID = get_env_var("GCP_PROJECT_ID")
BQ_DATASET = get_env_var("BQ_DATASET", required=False)  # optional depending on usage
BQ_TABLE = get_env_var("BQ_TABLE", required=False)
