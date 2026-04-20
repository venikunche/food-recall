"""
USDA FSIS Recall API extractor using dlt.

Uses the FSIS Recall API endpoint:
https://www.fsis.usda.gov/fsis/api/recall/v/1

The FSIS website blocks requests from standard HTTP clients (like
python-requests) with a 403 Forbidden. We use curl_cffi with browser
impersonation to bypass this protection.

The API returns ALL records in a single JSON array — no pagination needed.

See: https://www.fsis.usda.gov/science-data/developer-resources/recall-api
"""

import dlt
from curl_cffi import requests as cffi_requests

import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))

from config.settings import (
    FSIS_BASE_URL,
    REQUEST_TIMEOUT_SECONDS,
    MAX_RETRIES,
)


def fetch_fsis_data() -> list[dict]:
    """
    Fetches all FSIS recall data using curl_cffi to bypass 403 protection.

    Returns:
        List of recall dictionaries.

    Raises:
        Exception if the request fails after retries.
    """
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            response = cffi_requests.get(
                FSIS_BASE_URL,
                impersonate="chrome",
                timeout=REQUEST_TIMEOUT_SECONDS,
            )
            response.raise_for_status()
            data = response.json()

            if not isinstance(data, list):
                raise ValueError(f"Expected a list, got {type(data)}")

            print(f"Fetched {len(data)} FSIS records")
            return data

        except Exception as e:
            print(f"Attempt {attempt}/{MAX_RETRIES} failed: {e}")
            if attempt == MAX_RETRIES:
                raise
            import time
            time.sleep(2 ** attempt)  # Exponential backoff


@dlt.resource(
    name="fsis_recalls",
    write_disposition="replace",
)
def fsis_recalls_resource():
    """
    dlt resource that yields all FSIS recall records.

    Uses curl_cffi to fetch the data, then yields the list
    for dlt to handle schema inference and loading.
    """
    data = fetch_fsis_data()
    yield data


@dlt.source(name="fsis_food_recalls")
def fsis_source():
    """
    dlt source grouping all FSIS-related resources.
    """
    return fsis_recalls_resource()


def extract_fsis(destination: str = "duckdb") -> str:
    """
    Main extraction function — will be called by the Airflow DAG later.

    Args:
        destination: Where dlt should write.
                     'duckdb' for local testing.
                     'filesystem' for GCS (configured later).

    Returns:
        Load info string with record counts, duration, etc.
    """
    pipeline = dlt.pipeline(
        pipeline_name="fsis_recalls",
        destination=destination,
        dataset_name="bronze_fsis",
    )

    load_info = pipeline.run(
        fsis_source(),
        loader_file_format="parquet",
    )

    return str(load_info)


# =============================================================================
# Local testing — run this file directly
# =============================================================================
if __name__ == "__main__":
    """
    Quick local test:
        docker run -it -v $(pwd):/app food-recall-dev \
            python src/extractors/fsis_extractor.py
    """
    print("Starting FSIS extraction (local test with DuckDB)...")
    print(f"API: {FSIS_BASE_URL}")
    print("-" * 50)

    load_info = extract_fsis(destination="duckdb")

    print(f"\n✅ Extraction complete!")
    print(load_info)

    # Quick peek at the data
    print("\n📊 Quick data check:")
    import duckdb
    conn = duckdb.connect("fsis_recalls.duckdb")

    count = conn.sql("SELECT count(*) FROM bronze_fsis.fsis_recalls").fetchone()[0]
    print(f"Total records: {count}")

    print("\nColumns:")
    print(conn.sql("DESCRIBE bronze_fsis.fsis_recalls").df())

    print("\nSample data:")
    print(conn.sql("SELECT * FROM bronze_fsis.fsis_recalls LIMIT 3").df())

    conn.close()
