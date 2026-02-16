# test_tools.py
from tools.storage_tools import read_csv_from_gcs, detect_schema
from tools.quality_tools import check_null_values, check_duplicates, detect_outliers, calculate_quality_score
from tools.transform_tools import apply_all_transformations
from tools.bigquery_tools import load_to_bigquery, validate_load

# Test 1: Read CSV
print("Test 1: Reading CSV from GCS...")
df = read_csv_from_gcs("day2_messy.csv")
print(f"Loaded {len(df)} rows")
print(df.head())

# Test 2: Detect schema
print("\nTest 2: Detecting schema...")
schema = detect_schema(df)
print(f"Schema: {schema}")

# Test 3: Check quality
print("\nTest 3: Checking quality...")
issues = []
issues.extend(check_null_values(df))
dup = check_duplicates(df)
if dup:
    issues.append(dup)
outlier = detect_outliers(df, "amount")
if outlier:
    issues.append(outlier)

score = calculate_quality_score(df, issues)
print(f"Quality score: {score}")
print(f"Issues found: {len(issues)}")

# Test 4: Transform
print("\nTest 4: Transforming data...")
cleaned_df, fixes = apply_all_transformations(df, issues)
print(f"Rows: {len(df)} → {len(cleaned_df)}")
print(f"Fixes applied: {len(fixes)}")
for fix in fixes:
    print(f"  - {fix}")

# Test 5: Load to BigQuery
print("\nTest 5: Loading to BigQuery...")
result = load_to_bigquery(cleaned_df, table_name="test_sales_data")
print(f"Load result: {result}")

# Test 6: Validate
print("\nTest 6: Validating load...")
validation = validate_load(table_name="test_sales_data", expected_rows=len(cleaned_df))
print(f"Validation: {validation}")

# Test 7: Test on clean data
print("\n" + "="*60)
print("Test 7: Testing on CLEAN data (day1_clean.csv)...")
print("="*60)

df_clean = read_csv_from_gcs("day1_clean.csv")
print(f"Loaded {len(df_clean)} rows")

# Check quality on clean data
issues_clean = []
issues_clean.extend(check_null_values(df_clean))
dup_clean = check_duplicates(df_clean)
if dup_clean:
    issues_clean.append(dup_clean)
outlier_clean = detect_outliers(df_clean, "amount")
if outlier_clean:
    issues_clean.append(outlier_clean)

score_clean = calculate_quality_score(df_clean, issues_clean)
print(f" Quality score for clean data: {score_clean}")
print(f"Issues found: {len(issues_clean)}")

if score_clean == 100:
    print(" Perfect! Clean data scores 100!")
else:
    print(f"Expected 100, got {score_clean}")
    print(f"Issues: {issues_clean}")

print("All tools working!")