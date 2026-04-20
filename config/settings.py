"""
Central configuration for the Food Recall pipeline.
"""

import os

# =============================================================================
# API ENDPOINTS
# =============================================================================

# openFDA Food Enforcement API
# Docs: https://open.fda.gov/apis/food/enforcement/
FDA_BASE_URL = "https://api.fda.gov/food/enforcement.json"
FDA_PAGE_LIMIT = 1000  # Max allowed by the API per request

# USDA FSIS Recall API
# Docs: https://www.fsis.usda.gov/science-data/developer-resources/recall-api
FSIS_BASE_URL = "https://www.fsis.usda.gov/fsis/api/recall/v/1?field_translation_language=en"

# =============================================================================
# GCP
# =============================================================================

GCP_CREDENTIALS_PATH = os.getenv(
    "GOOGLE_APPLICATION_CREDENTIALS",
    "/opt/airflow/config/gcp/service-account.json"
)

GCS_BUCKET_BRONZE = os.getenv("GCS_BUCKET_BRONZE", "food-recall-bronze")
GCS_BUCKET_SILVER = os.getenv("GCS_BUCKET_SILVER", "food-recall-silver")


# Bronze layer paths (inside the bucket)
GCS_BRONZE_FDA_PREFIX = "fda"
GCS_BRONZE_FSIS_PREFIX = "fsis"

# =============================================================================
# RETRY / ERROR HANDLING
# =============================================================================

# Used by tenacity @retry decorator in extractors
MAX_RETRIES = 5
RETRY_WAIT_MULTIPLIER = 1      # seconds
RETRY_WAIT_MAX = 60             # seconds

# HTTP request timeout
REQUEST_TIMEOUT_SECONDS = 30

# =============================================================================
# DATA QUALITY THRESHOLDS
# =============================================================================

# FDA has ~30k+ total records; if we get fewer than this, something is wrong
FDA_MIN_EXPECTED_RECORDS = 10000

# FSIS has far fewer recalls; lower threshold
FSIS_MIN_EXPECTED_RECORDS = 50

# Required fields that must be present in every FDA record
FDA_REQUIRED_FIELDS = [
    "recall_number",
    "classification",
    "report_date",
    "reason_for_recall",
]

# Required fields that must be present in every FSIS record
FSIS_REQUIRED_FIELDS = [
    "field_title",
    "field_recall_classification",
    "field_recall_date",
]

# =============================================================================
# NOTIFICATIONS
# =============================================================================

SLACK_WEBHOOK_URL = os.getenv("SLACK_WEBHOOK_URL", "")

# =============================================================================
# AIRFLOW DAG SETTINGS
# =============================================================================

DAG_OWNER = "food-recall-team"
DAG_EMAIL = os.getenv("ALERT_EMAIL", "")
DAG_RETRIES = 3
DAG_RETRY_DELAY_MINUTES = 5
