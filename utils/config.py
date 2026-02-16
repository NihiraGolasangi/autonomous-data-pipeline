import os

# GCP Configuration
GCP_PROJECT_ID = "autonomous-data-pipeline"
GCS_BUCKET_NAME = "autonomous-pipeline-data-ng-2026"
BIGQUERY_DATASET = "pipeline_output"
BIGQUERY_TABLE = "sales_data"

# Model Configuration
MODEL_NAME = "gemini-2.0-flash-001"
LOCATION = "us-east1"

# Quality Thresholds
QUALITY_THRESHOLD_ABORT = 60
QUALITY_THRESHOLD_CLEAN = 80

# File Paths
DATA_FOLDER = "data"
REPORTS_FOLDER = "reports"

# Get credentials path
CREDENTIALS_PATH = os.path.join(os.path.dirname(__file__), "..", "credentials", "service-account-key.json")