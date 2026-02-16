# Autonomous Data Pipeline

A multi-agent data pipeline that ingests CSV files from Google Cloud Storage, detects quality issues, cleans data, and loads to BigQuery.

## What It Does

Processes CSV files through 5 specialized agents:

1. **Ingestion Agent** - Reads files from GCS, detects schema
2. **Quality Agent** - Checks for nulls, duplicates, outliers; calculates quality score (0-100)
3. **Transform Agent** - Cleans data based on quality issues
4. **Loader Agent** - Loads clean data to BigQuery
5. **Pipeline Manager** - Orchestrates the workflow and makes decisions

**Decision Logic:**
- Quality score < 60 → Abort (data too poor)
- Quality score 60-80 → Clean data, then load
- Quality score > 80 → Load directly (skip cleaning)

## Quick Start

### Prerequisites
- Python 3.9+
- GCP account with billing enabled
- Service account with permissions: Vertex AI User, Storage Object Admin, BigQuery Admin

### Setup

1. **Clone and install dependencies:**
```bash
git clone https://github.com/NihiraGolasangi/autonomous-data-pipeline.git
cd autonomous-data-pipeline
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

2. **Set up GCP:**
```bash
# Set credentials
export GOOGLE_APPLICATION_CREDENTIALS="path/to/service-account-key.json"
export GCP_PROJECT_ID="your-project-id"
export GCS_BUCKET_NAME="your-bucket-name"
```

3. **Update config:**

Edit `utils/config.py`:
```python
GCP_PROJECT_ID = "your-project-id"
GCS_BUCKET_NAME = "your-bucket-name"
LOCATION = "us-east1"
```

4. **Upload test data to GCS:**
```bash
gsutil cp data/*.csv gs://your-bucket-name/
```

5. **Create BigQuery dataset:**
- Go to BigQuery Console
- Create dataset: `pipeline_output`
- Location: same as your GCS bucket

### Run
```bash
python main.py
```

## Project Structure
```
autonomous-data-pipeline/
├── agents/              # 5 agent implementations
├── tools/               # Reusable functions (quality checks, transforms, etc.)
├── utils/               # Configuration and logging
├── data/                # Test CSV files
├── reports/             # Generated pipeline reports
└── main.py              # Entry point
```

## Test Data

Three test files demonstrate different scenarios:

| File | Rows | Quality | Pipeline Action |
|------|------|---------|----------------|
| day1_clean.csv | 33 | 100/100 | Skip cleaning |
| day2_messy.csv | 15 | 60/100 | Clean then load (3 nulls, 1 dup, 2 outliers) |
| day3_schema_change.csv | 10 | 100/100 | Handle new "region" column |

## Output

Pipeline generates JSON reports in `reports/`:
- Individual file reports
- Summary report with overall statistics

### Sample Output
```
[STEP 1/5] Running Ingestion Agent...
Ingestion complete: 15 rows

[STEP 2/5] Running Quality Agent...
Quality check complete: Score = 60.0/100

[STEP 3/5] Making decision...
Quality score 60.0 < 80: CLEANING required

[STEP 4/5] Running Transform Agent...
Transform complete: 15 → 14 rows

[STEP 5/5] Running Loader Agent...
Load complete: 14 rows to BigQuery

PIPELINE COMPLETED SUCCESSFULLY
```

## Design Decisions

**Why deterministic tools instead of LLMs?**

Data quality checks (nulls, duplicates, outliers) are mathematically well-defined. Deterministic solutions are:
- **Faster**: Milliseconds vs seconds per file
- **Cheaper**: No API costs
- **More reliable**: Same input = same output every time
- **Easier to debug**: Clear formulas, no black boxes

See `DESIGN_DECISIONS.md` for detailed technical analysis.

## Configuration

Key settings in `utils/config.py`:
```python
QUALITY_THRESHOLD_ABORT = 60   # Below this: abort
QUALITY_THRESHOLD_CLEAN = 80   # Below this: clean first
```

## Requirements

See `requirements.txt`. Key dependencies:
- google-cloud-aiplatform
- google-cloud-storage
- google-cloud-bigquery
- pandas
- numpy

