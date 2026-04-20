"""
DAG: Ingest USDA FSIS Recall data → GCS Bronze

Schedule: Daily (8 AM UTC) — FSIS updates in real-time
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator

import sys
sys.path.insert(0, "/opt/airflow")


default_args = {
    "owner": "food-recall",
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
}


def upload_fsis_to_gcs(**context):
    from src.extractors.fsis_extractor import fetch_fsis_data
    from src.uploaders.gcs_uploader import upload_to_gcs

    data = fetch_fsis_data()
    execution_date = context["ds"]
    date = datetime.strptime(execution_date, "%Y-%m-%d")
    uri = upload_to_gcs(data, api_name="fsis", date=date)
    context["ti"].xcom_push(key="gcs_uri", value=uri)
    context["ti"].xcom_push(key="record_count", value=len(data))


with DAG(
    dag_id="ingest_fsis_recalls",
    default_args=default_args,
    description="Extract FSIS recall data and upload to GCS Bronze",
    schedule_interval="0 8 * * *",  # Every day at 8 AM UTC
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["ingestion", "fsis", "bronze"],
) as dag:

    extract_and_upload_fsis = PythonOperator(
        task_id="extract_and_upload_fsis",
        python_callable=upload_fsis_to_gcs,
    )
