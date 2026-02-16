# agents/ingestion_agent.py
import sys
import os

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from tools.storage_tools import read_csv_from_gcs, detect_schema
from utils.logger import setup_logger

logger = setup_logger(__name__)

class IngestionAgent:
    """
    Ingestion Agent: Reads data files and detects schema
    """
    
    def __init__(self):
        self.name = "IngestionAgent"
        logger.info(f"{self.name} initialized")
    
    def detect_file_format(self, file_name):
        """
        Detect file format from extension
        
        Args:
            file_name: Name of the file
        
        Returns:
            str: File format (csv, json, parquet, etc.)
        """
        extension = file_name.lower().split('.')[-1]
        
        format_map = {
            'csv': 'csv',
            'json': 'json',
            'parquet': 'parquet',
            'jsonl': 'jsonl',
            'txt': 'text',
            'tsv': 'tsv'
        }
        
        return format_map.get(extension, 'unknown')
    
    def run(self, file_name):
        """
        Read file and detect schema
        
        Args:
            file_name: Name of file in GCS bucket (e.g., 'day1_clean.csv')
        
        Returns:
            dict with ingestion results
        """
        logger.info(f"{self.name} processing file: {file_name}")
        
        try:
            # Detect file format
            file_format = self.detect_file_format(file_name)
            logger.info(f"Detected file format: {file_format}")
            
            # Read CSV from GCS (currently only supports CSV)
            if file_format != 'csv':
                logger.warning(f"File format '{file_format}' detected, but only CSV is currently supported. Attempting to read as CSV...")
            
            df = read_csv_from_gcs(file_name)
            
            # Detect schema
            schema = detect_schema(df)
            
            # Prepare result
            result = {
                "status": "success",
                "file_name": file_name,
                "format": file_format,
                "rows": len(df),
                "columns": list(df.columns),
                "schema": schema,
                "dataframe": df 
            }
            
            logger.info(f"{self.name} completed: {len(df)} rows, {len(schema)} columns, format: {file_format}")
            return result
            
        except Exception as e:
            logger.error(f"{self.name} failed: {str(e)}")
            return {
                "status": "failed",
                "file_name": file_name,
                "format": self.detect_file_format(file_name),
                "error": str(e)
            }