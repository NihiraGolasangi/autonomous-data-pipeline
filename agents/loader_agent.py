# agents/loader_agent.py
import sys
import os

# Add parent directory to path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from tools.bigquery_tools import load_to_bigquery, validate_load
from utils.logger import setup_logger

logger = setup_logger(__name__)

class LoaderAgent:
    """
    Loader Agent: Loads clean data to BigQuery
    """
    
    def __init__(self):
        self.name = "LoaderAgent"
        logger.info(f"{self.name} initialized")
    
    def run(self, df, table_name=None):
        """
        Load data to BigQuery and validate
        
        Args:
            df: pandas DataFrame (cleaned data)
            table_name: Optional table name (defaults to config)
        
        Returns:
            dict with load results
        """
        rows_to_load = len(df)
        logger.info(f"{self.name} loading {rows_to_load} rows to BigQuery")
        
        try:
            # Load to BigQuery
            load_result = load_to_bigquery(df, table_name)
            
            if load_result['status'] == 'success':
                # Validate the load
                validation = validate_load(table_name, expected_rows=rows_to_load)
                
                result = {
                    "status": "success",
                    "destination": load_result['destination'],
                    "rows_loaded": load_result['rows_loaded'],
                    "validation": validation
                }
                
                logger.info(f"{self.name} completed: {rows_to_load} rows loaded successfully")
                return result
            else:
                logger.error(f"{self.name} load failed: {load_result.get('error')}")
                return {
                    "status": "failed",
                    "error": load_result.get('error')
                }
            
        except Exception as e:
            logger.error(f"{self.name} failed: {str(e)}")
            return {
                "status": "failed",
                "error": str(e)
            }