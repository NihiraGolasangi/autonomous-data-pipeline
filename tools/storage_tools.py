import pandas as pd
from google.cloud import storage
from io import StringIO
import sys
import os

# Add parent directory to path to import config
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from utils.config import GCS_BUCKET_NAME
from utils.logger import setup_logger

logger = setup_logger(__name__)

def read_csv_from_gcs(file_name):
    """
    Read a CSV file from Google Cloud Storage
    
    Args:
        file_name: Name of the file in the bucket (e.g., 'day1_clean.csv')
    
    Returns:
        pandas DataFrame
    """
    try:
        logger.info(f"Reading file: gs://{GCS_BUCKET_NAME}/{file_name}")
        
        # Initialize GCS client
        client = storage.Client()
        bucket = client.bucket(GCS_BUCKET_NAME)
        blob = bucket.blob(file_name)
        
        # Download as string
        csv_string = blob.download_as_text()
        
        # Convert to DataFrame
        df = pd.read_csv(StringIO(csv_string))
        
        logger.info(f"Successfully read {len(df)} rows from {file_name}")
        return df
        
    except Exception as e:
        logger.error(f"Error reading file {file_name}: {str(e)}")
        raise

def detect_schema(df):
    """
    Detect schema from a DataFrame
    
    Args:
        df: pandas DataFrame
    
    Returns:
        dict with schema information
    """
    schema = {}
    for col in df.columns:
        dtype = str(df[col].dtype)
        schema[col] = dtype
    
    return schema