import dlt
from dlt.sources.rest_api import rest_api_source

import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))

from config.settings import (
    FDA_BASE_URL,
    FDA_PAGE_LIMIT,
)

def fetch_fda_data() -> list[dict]:
    """
    Fetches all FDA food enforcement records and returns them as a list of dicts.
    Used by the GCS uploader for the Bronze layer.
    """
    import requests

    all_records = []
    url = f"{FDA_BASE_URL}?limit={FDA_PAGE_LIMIT}&sort=report_date:asc"

    while url:
        response = requests.get(url, timeout=30)
        response.raise_for_status()

        data = response.json()
        results = data.get("results", [])
        all_records.extend(results)
        print(f"Fetched {len(all_records)} FDA records so far...")

        # Follow the Link header for next page (Search-After pagination)
        link_header = response.headers.get("Link", "")
        if 'rel="next"' in link_header:
            # Extract URL from: <https://api.fda.gov/...>; rel="next"
            url = link_header.split(";")[0].strip("<> ")
        else:
            url = None

    print(f"Total FDA records fetched: {len(all_records)}")
    return all_records



def create_fda_source():
    return rest_api_source(
        {
            "client": {
                "base_url": FDA_BASE_URL,
                "paginator": {
                    "type": "header_link",
                },
            },
            "resource_defaults": {
                "write_disposition": "replace",
            },
            "resources": [
                {
                    "name": "fda_food_enforcement",
                    "primary_key": "recall_number",
                    "endpoint": {
                        "path": "",
                        "params": {
                            "limit": FDA_PAGE_LIMIT,
                            "sort": "report_date:asc",
                        },
                        "data_selector": "results",
                    },
                },
            ],
        }
    )


def extract_fda(destination: str = "duckdb") -> str:
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
        pipeline_name="fda_food_enforcement",
        destination=destination,
        dataset_name="bronze_fda",
    )

    source = create_fda_source()

    load_info = pipeline.run(
        source,
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
            python src/extractors/fda_extractor.py

    This will:
    1. Pull ALL FDA food enforcement records (~30k+)
    2. Store them in a local DuckDB file
    3. Print load info (record count, duration, etc.)

    No GCP credentials needed for local testing.
    """
    print("Starting FDA extraction (local test with DuckDB)...")
    print(f"API: {FDA_BASE_URL}")
    print(f"Page size: {FDA_PAGE_LIMIT}")
    print(f"Pagination: Search-After via Link header")
    print("-" * 50)

    load_info = extract_fda(destination="duckdb")

    print(f"\n✅ Extraction complete!")
    print(load_info)

    # Quick peek at the data
    print("\n📊 Quick data check:")
    import duckdb
    conn = duckdb.connect("fda_food_enforcement.duckdb")
    count = conn.sql("SELECT count(*) FROM bronze_fda.fda_food_enforcement").fetchone()[0]
    print(f"Total records: {count}")
    sample = conn.sql("""
        SELECT recall_number, classification, report_date, state 
        FROM bronze_fda.fda_food_enforcement 
        LIMIT 5
    """).df()
    print(sample)
    conn.close()
