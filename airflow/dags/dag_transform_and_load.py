"""
DAG: Transform (Spark Silver) → Load to BigQuery → dbt Gold

Schedule: None (triggered by FDA ingest DAG)
"""

import os
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator

import sys
sys.path.insert(0, "/opt/airflow")


default_args = {
    "owner": "food-recall",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}


def run_spark_silver(**context):
    """Run the Spark Silver transform."""
    import subprocess

    execution_date = context["ds"]

    result = subprocess.run(
        [
            "python", "/opt/airflow/spark/silver_transform.py",
            "--bronze-bucket", f"gs://{context['params']['bronze_bucket']}",
            "--silver-bucket", f"gs://{context['params']['silver_bucket']}",
            "--date", execution_date,
        ],
        capture_output=True,
        text=True,
    )

    print(result.stdout)
    if result.returncode != 0:
        print(result.stderr)
        raise Exception(f"Spark job failed with return code {result.returncode}")


def load_silver_to_bigquery(**context):
    """Load Silver parquet from GCS into BigQuery."""
    from google.cloud import bigquery

    execution_date = context["ds"]
    date = datetime.strptime(execution_date, "%Y-%m-%d")
    silver_bucket = context["params"]["silver_bucket"]

    silver_path = (
        f"gs://{silver_bucket}/silver/food_recalls/"
        f"year={date.strftime('%Y')}/"
        f"month={date.strftime('%m')}/"
        f"day={date.strftime('%d')}/*.parquet"
    )

    client = bigquery.Client()

    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.PARQUET,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
    )

    table_ref = "food_recall_silver.food_recalls"
    print(f"Loading {silver_path} → {table_ref}")

    load_job = client.load_table_from_uri(
        silver_path,
        table_ref,
        job_config=job_config,
    )
    load_job.result()

    table = client.get_table(table_ref)
    print(f"✅ Loaded {table.num_rows} rows into {table_ref}")


with DAG(
    dag_id="transform_and_load",
    default_args=default_args,
    description="Spark Silver transform → BigQuery load → dbt Gold models",
    schedule_interval=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["transform", "silver", "gold", "dbt"],
    params={
        "bronze_bucket": os.environ.get("GCS_BUCKET_BRONZE", "food-recall-bronze"),
        "silver_bucket": os.environ.get("GCS_BUCKET_SILVER", "food-recall-silver"),
    },
) as dag:

    spark_silver = PythonOperator(
        task_id="spark_silver_transform",
        python_callable=run_spark_silver,
    )

    load_to_bq = PythonOperator(
        task_id="load_silver_to_bigquery",
        python_callable=load_silver_to_bigquery,
    )

    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command="cd /opt/airflow/dbt_food_recall && dbt deps && dbt run",
        append_env=True,
        env={
            "PATH": "/home/airflow/.local/bin:/usr/local/bin:/usr/bin:/bin",
        },
    )

    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command="cd /opt/airflow/dbt_food_recall && dbt test",
        append_env=True,
        env={
            "PATH": "/home/airflow/.local/bin:/usr/local/bin:/usr/bin:/bin",
        },
    )

    spark_silver >> load_to_bq >> dbt_run >> dbt_test
