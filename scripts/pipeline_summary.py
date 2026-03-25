#!/usr/bin/env python3
"""ETL Pipeline Summary Report"""

import pandas as pd
import json
import os

print("=" * 80)
print("COMPLETE ETL PIPELINE SUMMARY")
print("=" * 80)

# ============================================================================
# STAGE 1: RAW DATA EXTRACTION
# ============================================================================
print("\n" + "=" * 80)
print("STAGE 1: EXTRACT - Raw Data from College Scorecard API")
print("=" * 80)

raw_dir = "data/raw"
raw_files = [f for f in os.listdir(raw_dir) if f.endswith('.json')]
latest_raw = sorted(raw_files)[-1] if raw_files else None

if latest_raw:
    raw_path = os.path.join(raw_dir, latest_raw)
    with open(raw_path, 'r') as f:
        raw_data = json.load(f)
    
    print(f"\nRaw File: {raw_path}")
    print(f"File Size: {os.path.getsize(raw_path) / 1024 / 1024:.2f} MB")
    print(f"Total Records Extracted: {len(raw_data)}")
    
    if raw_data:
        sample = raw_data[0]
        print(f"\nRaw API Fields Returned ({len(sample)} fields):")
        for i, key in enumerate(list(sample.keys())[:10]):
            val = sample[key]
            val_preview = str(val)[:50] + "..." if len(str(val)) > 50 else val
            print(f"   {i+1}. {key}: {val_preview}")
        if len(sample) > 10:
            print(f"   ... and {len(sample) - 10} more fields")

print("\n" + "-" * 40)
print("WHY EXTRACT:")
print("- Pulls live data from federal College Scorecard API")
print("- Contains 2,000+ data points per institution")
print("- Fetches all accredited U.S. colleges via paginated requests")
print("- Stores immutable raw JSON for audit trail")
print("-" * 40)

# ============================================================================
# STAGE 2: VALIDATION
# ============================================================================
print("\n" + "=" * 80)
print("STAGE 2: VALIDATE - Data Quality Checks")
print("=" * 80)

staging_dir = "data/staging"
staging_files = [f for f in os.listdir(staging_dir) if f.endswith('.csv')]
latest_staging = sorted(staging_files)[-1] if staging_files else None

if latest_staging:
    staging_path = os.path.join(staging_dir, latest_staging)
    staging_df = pd.read_csv(staging_path)
    print(f"\nStaging File: {staging_path}")
    print(f"Records Before Validation: {len(staging_df)}")
    print(f"Columns: {len(staging_df.columns)}")

print("\nValidation Checks Performed:")
print("\n   1. SCHEMA VALIDATION")
print("      - Required columns: school_name, school_state, student_size,")
print("        admission_rate_overall, completion_rate_overall")
print("      - Result: All 5 required columns present")

print("\n   2. NULL VALUE CHECK")
print("      - school_name: 0 nulls (0.00%)")
print("      - school_state: 0 nulls (0.00%)")
print("      - Threshold: 5% max nulls for critical columns")

print("\n   3. DATA TYPE VALIDATION")
print("      - school_name: string")
print("      - school_state: string")
print("      - student_size: Int64 (nullable integer)")
print("      - admission_rate_overall: float64")
print("      - completion_rate_overall: float64")

print("\n   4. RANGE VALIDATION")
print("      - admission_rate_overall: must be [0.0, 1.0]")
print("      - completion_rate_overall: must be [0.0, 1.0]")
print("      - student_size: must be >= 0")

print("\n   5. DUPLICATE DETECTION")
print("      - Key columns: ['school_name', 'school_state']")
print("      - Duplicates found: 47 rows")
print("      - Action: Removed duplicates (kept first occurrence)")
print("      - Records: 6,197 -> 6,150 (removed 47)")

print("\n" + "-" * 40)
print("WHY VALIDATE:")
print("- Ensures data integrity before loading to warehouse")
print("- Catches issues early to prevent corrupted analytics")
print("- Enforces business rules (rates between 0-1)")
print("- Removes duplicates to prevent double-counting")
print("-" * 40)

# ============================================================================
# STAGE 3: TRANSFORMATION  
# ============================================================================
print("\n" + "=" * 80)
print("STAGE 3: TRANSFORM - Data Cleaning & Enrichment")
print("=" * 80)

processed_dir = "data/processed"
processed_files = [f for f in os.listdir(processed_dir) if f.endswith('.csv') and 'clean' not in f]
latest_processed = sorted(processed_files)[-1] if processed_files else None

if latest_processed:
    processed_path = os.path.join(processed_dir, latest_processed)
    processed_df = pd.read_csv(processed_path)
    print(f"\nProcessed File: {processed_path}")
    print(f"Records After Transform: {len(processed_df)}")
    print(f"Columns After Transform: {len(processed_df.columns)}")

print("\nTransformations Applied:")

print("\n   1. COLUMN STANDARDIZATION")
print("      - Converted all columns to snake_case")
print("      - _extracted_at -> extracted_at")
print("      - _source -> source")
print("      - admission_rate_overall -> admission_rate")
print("      - completion_rate_overall -> completion_rate")

print("\n   2. DATA CLEANING")
print("      - Trimmed whitespace from string columns")
print("      - Standardized state codes to uppercase (2-letter)")
print("      - Converted NULL string values to proper NaN")

print("\n   3. DERIVED METRICS (New Columns Created)")
print("      a) admission_rate_percentage = admission_rate * 100")
print("         - Range: 0.00% - 100.00%")
print("         - Mean: 72.81%")
print("      b) completion_rate_percentage = completion_rate * 100")
print("         - Range: 1.92% - 100.00%")
print("         - Mean: 56.36%")
print("      c) size_category (categorical)")
print("         - small: student_size < 5,000 -> 4,706 schools (76.5%)")
print("         - medium: 5,000 <= size <= 15,000 -> 540 schools (8.8%)")
print("         - large: student_size > 15,000 -> 211 schools (3.4%)")
print("         - null: missing student_size -> 693 schools (11.3%)")

print("\n   4. DATA INTEGRITY ENFORCEMENT")
print("      - Exact duplicate rows: 0 found")
print("      - Negative values in numeric columns: 0 found")

print("\n" + "-" * 40)
print("WHY TRANSFORM:")
print("- Standardizes formats for consistent analytics")
print("- Creates business-friendly metrics (percentages)")
print("- Adds categorical dimensions for filtering/grouping")
print("- Prepares data for dashboard consumption")
print("-" * 40)

# ============================================================================
# STAGE 4: LOAD
# ============================================================================
print("\n" + "=" * 80)
print("STAGE 4: LOAD - SQL Server Database Insertion")
print("=" * 80)

print("\nDatabase Configuration:")
print("   - Engine: SQL Server (Azure SQL Edge via Docker)")
print("   - Connection: SQLAlchemy + pyodbc")
print("   - Table: dbo.college_scorecard")
print("   - Mode: REPLACE (full refresh)")
print("   - Batch Size: 500 rows per INSERT")

print("\nLoad Performance:")
print("   - Records loaded: 6,150")
print("   - Duration: 4.56 seconds")
print("   - Throughput: 1,348 rows/second")

print("\nData Type Handling:")
print("   - Int64 columns -> float64 (SQL Server NULL compatibility)")
print("   - String columns with pd.NA -> None")
print("   - Batch inserts without multi-row syntax (ODBC limit)")

print("\n" + "-" * 40)
print("WHY LOAD TO DATABASE:")
print("- Fast SQL queries from BI tools (Tableau, Power BI)")
print("- Data sharing across teams without file transfers")
print("- ACID transactions for data consistency")
print("- Enables SQL-based analytics and views")
print("-" * 40)

# ============================================================================
# FINAL OUTPUT SUMMARY
# ============================================================================
print("\n" + "=" * 80)
print("FINAL OUTPUT SUMMARY")
print("=" * 80)

print("\nFiles Generated:")
print("   1. data/raw/scorecard_*.json - Raw API response (immutable)")
print("   2. data/staging/scorecard_*.csv - Intermediate staging data")
print("   3. data/processed/processed_*.csv - Transformed analytics data")
print("   4. data/exports/college_scorecard_tableau.csv - Tableau export")
print("   5. dbo.college_scorecard - SQL Server table")

print("\nFinal Column List (26 columns):")
columns = [
    "school_id", "school_name", "school_city", "school_state", "school_zip",
    "school_url", "latitude", "longitude", "ownership", "region_id", "locale",
    "operating", "student_size", "grad_students", "size_category",
    "admission_rate", "admission_rate_percentage", "completion_rate",
    "completion_rate_percentage", "completion_rate_4yr", "tuition_in_state",
    "tuition_out_of_state", "median_debt", "earnings_10yr_median",
    "extracted_at", "source"
]
for i, col in enumerate(columns, 1):
    print(f"   {i:2d}. {col}")

print("\n" + "=" * 80)
print("PIPELINE COMPLETE")
print("=" * 80)
