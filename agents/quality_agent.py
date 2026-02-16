# agents/quality_agent.py
import sys
import os

# Add parent directory to path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from tools.quality_tools import (
    check_null_values, 
    check_duplicates, 
    detect_outliers, 
    calculate_quality_score
)
from utils.logger import setup_logger

logger = setup_logger(__name__)

class QualityAgent:
    """
    Quality Agent: Profiles data and calculates quality score
    """
    
    def __init__(self):
        self.name = "QualityAgent"
        logger.info(f"{self.name} initialized")
    
    def run(self, df):
        """
        Check data quality and calculate score
        
        Args:
            df: pandas DataFrame
        
        Returns:
            dict with quality results
        """
        logger.info(f"{self.name} analyzing {len(df)} rows")
        
        try:
            issues = []
            
            # 1. Check for null values
            null_issues = check_null_values(df)
            issues.extend(null_issues)
            
            # 2. Check for duplicates
            dup_issue = check_duplicates(df)
            if dup_issue:
                issues.append(dup_issue)
            
            # 3. Check for outliers in numeric columns
            numeric_columns = df.select_dtypes(include=['int64', 'float64']).columns
            for col in numeric_columns:
                outlier_issue = detect_outliers(df, col)
                if outlier_issue:
                    issues.append(outlier_issue)
            
            # 4. Calculate overall quality score
            quality_score = calculate_quality_score(df, issues)
            
            # Prepare result
            result = {
                "status": "success",
                "quality_score": quality_score,
                "total_rows": len(df),
                "issues": issues,
                "dataframe": df  # Pass dataframe to next agent
            }
            
            logger.info(f"{self.name} completed: Score = {quality_score}/100")
            return result
            
        except Exception as e:
            logger.error(f"{self.name} failed: {str(e)}")
            return {
                "status": "failed",
                "error": str(e)
            }