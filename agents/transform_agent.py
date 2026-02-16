# agents/transform_agent.py
import sys
import os

# Add parent directory to path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from tools.transform_tools import apply_all_transformations
from utils.logger import setup_logger

logger = setup_logger(__name__)

class TransformAgent:
    """
    Transform Agent: Cleans data based on quality issues
    """
    
    def __init__(self):
        self.name = "TransformAgent"
        logger.info(f"{self.name} initialized")
    
    def run(self, df, issues):
        """
        Apply transformations to clean data
        
        Args:
            df: pandas DataFrame
            issues: list of issue dictionaries from Quality Agent
        
        Returns:
            dict with transformation results
        """
        rows_in = len(df)
        logger.info(f"{self.name} cleaning {rows_in} rows with {len(issues)} issue types")
        
        try:
            # Apply all transformations
            cleaned_df, fixes_applied = apply_all_transformations(df, issues)
            
            rows_out = len(cleaned_df)
            
            # Prepare result
            result = {
                "status": "success",
                "rows_in": rows_in,
                "rows_out": rows_out,
                "rows_removed": rows_in - rows_out,
                "fixes_applied": fixes_applied,
                "dataframe": cleaned_df  # Pass cleaned dataframe to Loader
            }
            
            logger.info(f"{self.name} completed: {rows_in} → {rows_out} rows, {len(fixes_applied)} fixes")
            return result
            
        except Exception as e:
            logger.error(f"{self.name} failed: {str(e)}")
            return {
                "status": "failed",
                "error": str(e)
            }