"""
GCS uploader for Bronze layer data.

Uploads extracted data to GCS with a date-partitioned path layout:
    gs://<bucket>/raw/<api_name>/year=YYYY/month=MM/day=DD/data.json

Uses google-cloud-storage directly (not dlt filesystem destination)
because we want full control over the path structure.
"""

import json
from datetime import datetime
from google.cloud import storage

import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))

from config.settings import GCS_BUCKET_BRONZE, GCP_CREDENTIALS_PATH

def get_gcs_client() -> storage.Client:
    """
    Creates a GCS client using the service account key.
    Falls back to default credentials if the key file doesn't exist
    (e.g., running on GCP with attached service account).
    """
    if os.path.exists(GCP_CREDENTIALS_PATH):
        return storage.Client.from_service_account_json(GCP_CREDENTIALS_PATH)
    return storage.Client()


def build_gcs_path(api_name: str, date: datetime = None) -> str:
    """
    Builds the date-partitioned GCS object path.

    Args:
        api_name: 'fda' or 'fsis'
        date: The date for partitioning. Defaults to today.

    Returns:
        Path like: raw/fda/year=2026/month=04/day=14/data.json
    """
    if date is None:
        date = datetime.utcnow()

    return (
        f"raw/{api_name}/"
        f"year={date.strftime('%Y')}/"
        f"month={date.strftime('%m')}/"
        f"day={date.strftime('%d')}/"
        f"data.json"
    )


def upload_to_gcs(
    data: list[dict],
    api_name: str,
    date: datetime = None,
    bucket_name: str = None,
) -> str:
    """
    Uploads a list of records as a JSON file to GCS.

    Args:
        data:        List of dictionaries (the extracted records).
        api_name:    'fda' or 'fsis' — used in the path.
        date:        Date for partitioning. Defaults to today (UTC).
        bucket_name: Override the default bucket name.

    Returns:
        The full gs:// URI of the uploaded file.
    """
    bucket_name = bucket_name or GCS_BUCKET_BRONZE
    gcs_path = build_gcs_path(api_name, date)

    client = get_gcs_client()
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(gcs_path)

    # Convert to newline-delimited JSON (one record per line)
    # This is easier for downstream Spark/BigQuery to read than a JSON array
    ndjson_content = "\n".join(json.dumps(record) for record in data)

    blob.upload_from_string(
        ndjson_content,
        content_type="application/json",
    )

    uri = f"gs://{bucket_name}/{gcs_path}"
    print(f"✅ Uploaded {len(data)} records to {uri}")
    return uri


# =============================================================================
# Local testing
# =============================================================================
if __name__ == "__main__":
    """
    Test with a small fake payload to verify GCS access:

        docker run -it -v $(pwd):/app food-recall-dev \
            python src/uploaders/gcs_uploader.py
    """
    # Test with fake data first to verify connectivity
    test_data = [
        {"recall_number": "TEST-001", "classification": "Class I"},
        {"recall_number": "TEST-002", "classification": "Class II"},
    ]

    print(f"Bucket: {GCS_BUCKET_BRONZE}")
    print(f"Credentials: {GCP_CREDENTIALS_PATH}")
    print(f"Path: {build_gcs_path('test')}")
    print("-" * 50)

    uri = upload_to_gcs(test_data, api_name="test")
    print(f"\nUploaded to: {uri}")
