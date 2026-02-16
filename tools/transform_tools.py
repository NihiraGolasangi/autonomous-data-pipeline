# tools/transform_tools.py
import pandas as pd
import numpy as np
from datetime import datetime
from utils.logger import setup_logger

logger = setup_logger(__name__)

def fill_nulls(df, column, strategy="mean"):
    """
    Fill null values in a column
    
    Args:
        df: pandas DataFrame
        column: column name
        strategy: 'mean', 'median', 'mode', or 'drop'
    
    Returns:
        tuple: (modified_df, list of fix descriptions)
    """
    fixes = []
    null_count = df[column].isnull().sum()
    
    if null_count == 0:
        return df, fixes
    
    if strategy == "drop":
        df = df.dropna(subset=[column])
        fixes.append(f"Dropped {null_count} rows with null values in '{column}'")
    elif df[column].dtype in ['int64', 'float64']:
        if strategy == "mean":
            fill_value = df[column].mean()
            df.loc[:, column] = df[column].fillna(fill_value)  # ✅ FIXED
            fixes.append(f"Filled {null_count} nulls in '{column}' with mean ({fill_value:.2f})")
        elif strategy == "median":
            fill_value = df[column].median()
            df.loc[:, column] = df[column].fillna(fill_value)  # ✅ FIXED
            fixes.append(f"Filled {null_count} nulls in '{column}' with median ({fill_value:.2f})")
    else:
        # For non-numeric columns, use mode or placeholder
        df.loc[:, column] = df[column].fillna("UNKNOWN")  # ✅ FIXED
        fixes.append(f"Filled {null_count} nulls in '{column}' with 'UNKNOWN'")
    
    logger.info(fixes[-1] if fixes else "No nulls to fill")
    return df, fixes



def remove_duplicates(df):
    """
    Remove duplicate rows
    
    Args:
        df: pandas DataFrame
    
    Returns:
        tuple: (modified_df, list of fix descriptions)
    """
    fixes = []
    before = len(df)
    df = df.drop_duplicates(keep='first')
    after = len(df)
    removed = before - after
    
    if removed > 0:
        fixes.append(f"Removed {removed} duplicate rows")
        logger.info(fixes[-1])
    
    return df, fixes

def fix_date_formats(df, column):
    """
    Standardize date formats to YYYY-MM-DD
    
    Args:
        df: pandas DataFrame
        column: column name containing dates
    
    Returns:
        tuple: (modified_df, list of fix descriptions)
    """
    fixes = []
    
    try:
        # Try to parse dates (handles multiple formats)
        df[column] = pd.to_datetime(df[column], errors='coerce')
        
        # Count how many failed to parse
        failed = df[column].isnull().sum()
        
        # Convert to standard format
        df[column] = df[column].dt.strftime('%Y-%m-%d')
        
        fixes.append(f"Standardized date format in '{column}' to YYYY-MM-DD")
        if failed > 0:
            fixes.append(f"Warning: {failed} dates could not be parsed in '{column}'")
        
        logger.info(fixes[0])
    except Exception as e:
        logger.error(f"Error fixing dates in '{column}': {str(e)}")
    
    return df, fixes

def handle_outliers(df, column, method="cap"):
    """
    Handle outliers by capping or removing
    
    Args:
        df: pandas DataFrame
        column: column name
        method: 'cap' (default) or 'remove'
    
    Returns:
        tuple: (modified_df, list of fix descriptions)
    """
    fixes = []
    
    if column not in df.columns or df[column].dtype not in ['int64', 'float64']:
        return df, fixes
    
    # Calculate IQR bounds
    Q1 = df[column].quantile(0.25)
    Q3 = df[column].quantile(0.75)
    IQR = Q3 - Q1
    lower_bound = Q1 - 1.5 * IQR
    upper_bound = Q3 + 1.5 * IQR
    
    # Count outliers
    outlier_count = len(df[(df[column] < lower_bound) | (df[column] > upper_bound)])
    
    if outlier_count == 0:
        return df, fixes
    
    if method == "cap":
        # Cap values at bounds
        df[column] = df[column].clip(lower=lower_bound, upper=upper_bound)
        fixes.append(f"Capped {outlier_count} outliers in '{column}' to [{lower_bound:.2f}, {upper_bound:.2f}]")
    elif method == "remove":
        # Remove outlier rows
        before = len(df)
        df = df[(df[column] >= lower_bound) & (df[column] <= upper_bound)]
        after = len(df)
        removed = before - after
        fixes.append(f"Removed {removed} rows with outliers in '{column}'")
    
    logger.info(fixes[-1] if fixes else "No outliers to handle")
    return df, fixes

def apply_all_transformations(df, issues):
    """
    Apply all necessary transformations based on issues found
    
    Args:
        df: pandas DataFrame
        issues: list of issue dictionaries from Quality Agent
    
    Returns:
        tuple: (cleaned_df, list of all fixes applied)
    """
    all_fixes = []
    rows_in = len(df)
    
    logger.info(f"Starting transformations on {rows_in} rows")
    
    # 1. Remove duplicates first
    for issue in issues:
        if issue.get('type') == 'duplicates':
            df, fixes = remove_duplicates(df)
            all_fixes.extend(fixes)
            break
    
    # 2. Handle outliers 
    for issue in issues:
        if issue.get('type') == 'outliers':
            column = issue['column']
            df, fixes = handle_outliers(df, column, method="cap")
            all_fixes.extend(fixes)
    
    # 3. NOW fill nulls (mean is calculated on clean data without outliers)
    for issue in issues:
        if issue.get('type') == 'nulls':
            column = issue['column']
            df, fixes = fill_nulls(df, column, strategy="mean")
            all_fixes.extend(fixes)
    
    # 4. Fix date formats last (doesn't affect other transformations)
    if 'order_date' in df.columns:
        df, fixes = fix_date_formats(df, 'order_date')
        all_fixes.extend(fixes)
    
    rows_out = len(df)
    logger.info(f"Transformations complete: {rows_in} rows → {rows_out} rows")
    
    return df, all_fixes