"""
DAG: Ingest FDA Food Enforcement data → GCS Bronze

Schedule: Weekly (Monday 6 AM UTC) — matches FDA's weekly update cadence
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

import sys
sys.path.insert(0, "/opt/airflow")


default_args = {
    "owner": "food-recall",
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
}


def extract_fda(**context):
    from src.extractors.fda_extractor import fetch_fda_data
    data = fetch_fda_data()
    # Store count in XCom for downstream tasks
    context["ti"].xcom_push(key="record_count", value=len(data))
    context["ti"].xcom_push(key="fda_data", value="extracted")
    return data


def upload_fda_to_gcs(**context):
    from src.uploaders.gcs_uploader import upload_to_gcs
    from src.extractors.fda_extractor import fetch_fda_data

    # Re-fetch (XCom not ideal for large data)
    data = fetch_fda_data()
    execution_date = context["ds"]  # YYYY-MM-DD string
    date = datetime.strptime(execution_date, "%Y-%m-%d")
    uri = upload_to_gcs(data, api_name="fda", date=date)
    context["ti"].xcom_push(key="gcs_uri", value=uri)
    context["ti"].xcom_push(key="record_count", value=len(data))


with DAG(
    dag_id="ingest_fda_food_enforcement",
    default_args=default_args,
    description="Extract FDA food enforcement data and upload to GCS Bronze",
    schedule_interval="0 6 * * 1",  # Every Monday at 6 AM UTC
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["ingestion", "fda", "bronze"],
) as dag:

    extract_and_upload_fda = PythonOperator(
        task_id="extract_and_upload_fda",
        python_callable=upload_fda_to_gcs,
    )

    trigger_transform = TriggerDagRunOperator(
        task_id="trigger_transform_pipeline",
        trigger_dag_id="transform_and_load",
        wait_for_completion=False,
    )

    extract_and_upload_fda >> trigger_transform
