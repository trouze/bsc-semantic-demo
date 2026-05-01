import os


def get_dbt_cloud_token() -> str:
    token = os.getenv("DBT_CLOUD_TOKEN", "")
    if not token:
        raise RuntimeError(
            "DBT_CLOUD_TOKEN env var not set. "
            "In SPCS this is injected via the secret binding in the service spec."
        )
    return token
