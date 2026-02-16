# agents/ingestion_agent.py
import sys
import os

# Add parent directory to path
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
            # Read CSV from GCS
            df = read_csv_from_gcs(file_name)
            
            # Detect schema
            schema = detect_schema(df)
            
            # Prepare result
            result = {
                "status": "success",
                "file_name": file_name,
                "format": "csv",
                "rows": len(df),
                "columns": list(df.columns),
                "schema": schema,
                "dataframe": df  # Pass dataframe to next agent
            }
            
            logger.info(f"{self.name} completed: {len(df)} rows, {len(schema)} columns")
            return result
            
        except Exception as e:
            logger.error(f"{self.name} failed: {str(e)}")
            return {
                "status": "failed",
                "error": str(e)
            }