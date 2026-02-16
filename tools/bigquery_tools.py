# tools/bigquery_tools.py
import pandas as pd
from google.cloud import bigquery
import sys
import os

# Add parent directory to path to import config
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from utils.config import GCP_PROJECT_ID, BIGQUERY_DATASET, BIGQUERY_TABLE
from utils.logger import setup_logger

logger = setup_logger(__name__)

def load_to_bigquery(df, table_name=None):
    """
    Load DataFrame to BigQuery table
    
    Args:
        df: pandas DataFrame
        table_name: Optional table name (defaults to config)
    
    Returns:
        dict with load results
    """
    try:
        # Use default table name if not provided
        if table_name is None:
            table_name = BIGQUERY_TABLE
        
        # Full table ID: project.dataset.table
        table_id = f"{GCP_PROJECT_ID}.{BIGQUERY_DATASET}.{table_name}"
        
        logger.info(f"Loading {len(df)} rows to BigQuery table: {table_id}")
        
        # Initialize BigQuery client
        client = bigquery.Client(project=GCP_PROJECT_ID)
        
        # Configure load job
        job_config = bigquery.LoadJobConfig(
            write_disposition="WRITE_TRUNCATE",  # Replace table if exists
            autodetect=True  # Auto-detect schema from DataFrame
        )
        
        # Load data
        job = client.load_table_from_dataframe(df, table_id, job_config=job_config)
        job.result()  # Wait for completion
        
        logger.info(f"Successfully loaded {len(df)} rows to {table_id}")
        
        return {
            "status": "success",
            "destination": table_id,
            "rows_loaded": len(df)
        }
        
    except Exception as e:
        logger.error(f"Error loading to BigQuery: {str(e)}")
        return {
            "status": "failed",
            "error": str(e)
        }

def validate_load(table_name=None, expected_rows=None):
    """
    Validate that data was loaded correctly
    
    Args:
        table_name: Optional table name (defaults to config)
        expected_rows: Expected row count
    
    Returns:
        dict with validation results
    """
    try:
        if table_name is None:
            table_name = BIGQUERY_TABLE
        
        table_id = f"{GCP_PROJECT_ID}.{BIGQUERY_DATASET}.{table_name}"
        
        logger.info(f"Validating load for table: {table_id}")
        
        client = bigquery.Client(project=GCP_PROJECT_ID)
        
        # Query row count
        query = f"SELECT COUNT(*) as row_count FROM `{table_id}`"
        result = client.query(query).result()
        actual_rows = list(result)[0].row_count
        
        # Check if matches expected
        match = True
        if expected_rows is not None:
            match = (actual_rows == expected_rows)
        
        logger.info(f"Table has {actual_rows} rows")
        
        return {
            "table": table_id,
            "actual_rows": actual_rows,
            "expected_rows": expected_rows,
            "match": match
        }
        
    except Exception as e:
        logger.error(f"Error validating load: {str(e)}")
        return {
            "status": "validation_failed",
            "error": str(e)
        }

def get_table_schema(table_name=None):
    """
    Get the schema of a BigQuery table
    
    Args:
        table_name: Optional table name (defaults to config)
    
    Returns:
        dict with schema information
    """
    try:
        if table_name is None:
            table_name = BIGQUERY_TABLE
        
        table_id = f"{GCP_PROJECT_ID}.{BIGQUERY_DATASET}.{table_name}"
        
        client = bigquery.Client(project=GCP_PROJECT_ID)
        table = client.get_table(table_id)
        
        schema = {}
        for field in table.schema:
            schema[field.name] = field.field_type
        
        logger.info(f"Retrieved schema for {table_id}: {len(schema)} columns")
        
        return {
            "table": table_id,
            "schema": schema,
            "num_columns": len(schema)
        }
        
    except Exception as e:
        logger.error(f"Error getting schema: {str(e)}")
        return {
            "error": str(e)
        }