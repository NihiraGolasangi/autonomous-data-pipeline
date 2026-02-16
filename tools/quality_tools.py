# tools/quality_tools.py
import pandas as pd
import numpy as np
from utils.logger import setup_logger

logger = setup_logger(__name__)

def check_null_values(df):
    """
    Find null/missing values in DataFrame
    
    Args:
        df: pandas DataFrame
    
    Returns:
        list of issue dictionaries with indices
    """
    issues = []
    
    for col in df.columns:
        null_mask = df[col].isnull()
        null_indices = df[null_mask].index.tolist()
        
        if len(null_indices) > 0:
            issues.append({
                "type": "nulls",
                "column": col,
                "count": len(null_indices),
                "indices": null_indices
            })
            logger.info(f"Found {len(null_indices)} null values in column '{col}'")
    
    return issues

def check_duplicates(df):
    """
    Find duplicate rows
    
    Args:
        df: pandas DataFrame
    
    Returns:
        dict with duplicate info or None
    """
    duplicate_mask = df.duplicated(keep='first')
    duplicate_indices = df[duplicate_mask].index.tolist()
    
    if len(duplicate_indices) > 0:
        logger.info(f"Found {len(duplicate_indices)} duplicate rows")
        return {
            "type": "duplicates",
            "count": len(duplicate_indices),
            "indices": duplicate_indices
        }
    return None

def detect_outliers(df, column):
    """
    Detect outliers using IQR method
    
    Args:
        df: pandas DataFrame
        column: column name to check
    
    Returns:
        dict with outlier info or None
    """
    if column not in df.columns:
        return None
    
    # Only check numeric columns
    if df[column].dtype not in ['int64', 'float64']:
        return None
    
    Q1 = df[column].quantile(0.25)
    Q3 = df[column].quantile(0.75)
    IQR = Q3 - Q1
    
    lower_bound = Q1 - 1.5 * IQR
    upper_bound = Q3 + 1.5 * IQR
    
    outlier_mask = (df[column] < lower_bound) | (df[column] > upper_bound)
    outlier_indices = df[outlier_mask].index.tolist()
    
    if len(outlier_indices) > 0:
        outlier_values = df.loc[outlier_indices, column].tolist()
        logger.info(f"Found {len(outlier_indices)} outliers in column '{column}'")
        return {
            "type": "outliers",
            "column": column,
            "count": len(outlier_indices),
            "indices": outlier_indices,
            "values": outlier_values[:5]  # Show first 5 values
        }
    return None

def calculate_quality_score(df, issues):
    """
    Calculate overall quality score (0-100) based on problematic rows
    
    Args:
        df: pandas DataFrame
        issues: list of issue dictionaries
    
    Returns:
        float score between 0-100
    """
    total_rows = len(df)
    if total_rows == 0:
        return 0
    
    # Track which rows have problems (use set to avoid counting same row twice)
    problematic_rows = set()
    
    # Collect all problematic row indices
    for issue in issues:
        if issue and 'indices' in issue:
            problematic_rows.update(issue['indices'])
    
    # Calculate percentage of problematic rows
    problematic_count = len(problematic_rows)
    issue_percentage = (problematic_count / total_rows) * 100
    score = max(0, 100 - issue_percentage)
    
    logger.info(f"Problematic rows: {problematic_count}/{total_rows} ({issue_percentage:.1f}%)")
    logger.info(f"Quality score: {score:.2f}/100")
    
    return round(score, 2)