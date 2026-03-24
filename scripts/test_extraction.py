#!/usr/bin/env python3
"""
College Scorecard Extraction Test
=================================

Verifies the extraction module works correctly by:
1. Testing API connection
2. Extracting a sample of data
3. Validating required fields exist
4. Displaying extraction statistics

Usage:
    python scripts/test_extraction.py
"""

import sys
from pathlib import Path

# Add project to path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from higher_ed_data_pipeline.etl.college_scorecard_extractor import CollegeScorecardExtractor


def main():
    # API Key (replace with environment variable in production)
    API_KEY = "oHByS1HOahWshBi23IkWXGpqeYyU77lYmP7XY0Qz"
    
    print("=" * 70)
    print("COLLEGE SCORECARD EXTRACTION MODULE TEST")
    print("=" * 70)
    
    # Initialize extractor
    print("\n[1] Initializing extractor...")
    try:
        extractor = CollegeScorecardExtractor(api_key=API_KEY)
        print("    ✓ Extractor initialized successfully")
    except Exception as e:
        print(f"    ✗ Failed to initialize: {e}")
        return 1
    
    # Validate connection
    print("\n[2] Validating API connection...")
    if extractor.client.validate_connection():
        print("    ✓ API connection valid")
    else:
        print("    ✗ API connection failed")
        return 1
    
    # Extract sample data
    print("\n[3] Extracting sample data (2 pages)...")
    try:
        df = extractor.extract_sample(n_pages=2, save_to_staging=True)
        print(f"    ✓ Extracted {len(df)} records")
    except Exception as e:
        print(f"    ✗ Extraction failed: {e}")
        import traceback
        traceback.print_exc()
        return 1
    
    # Validate required fields
    print("\n[4] Validating required fields...")
    required_fields = [
        ("school_name", "school.name"),
        ("school_state", "school.state"),
        ("student_size", "latest.student.size"),
        ("admission_rate_overall", "latest.admissions.admission_rate.overall"),
        ("completion_rate_overall", "latest.completion.rate_suppressed.overall"),
    ]
    
    all_valid = True
    for field_name, api_path in required_fields:
        if field_name in df.columns:
            non_null = df[field_name].notna().sum()
            pct = (non_null / len(df)) * 100
            print(f"    ✓ {field_name}: {non_null}/{len(df)} non-null ({pct:.1f}%)")
        else:
            print(f"    ✗ {field_name}: MISSING")
            all_valid = False
    
    if not all_valid:
        print("\n    ERROR: Some required fields are missing!")
        return 1
    
    # Display sample data
    print("\n[5] Sample records:")
    print("-" * 70)
    
    display_cols = [
        "school_name", "school_state", "student_size",
        "admission_rate_overall", "completion_rate_overall"
    ]
    
    sample = df[display_cols].head(10)
    for idx, row in sample.iterrows():
        size_str = f"{int(row['student_size']):>6}" if pd.notna(row['student_size']) else "   N/A"
        adm_str = f"{row['admission_rate_overall']*100:>5.1f}%" if pd.notna(row['admission_rate_overall']) else "  N/A "
        comp_str = f"{row['completion_rate_overall']*100:>5.1f}%" if pd.notna(row['completion_rate_overall']) else "  N/A "
        print(f"    {row['school_name'][:40]:<40} | {row['school_state']:>2} | "
              f"Size: {size_str} | Adm: {adm_str} | Comp: {comp_str}")
    
    # Get statistics
    print("\n[6] Extraction statistics:")
    print("-" * 70)
    stats = extractor.get_extraction_stats(df)
    print(f"    Total records: {stats['total_records']}")
    print(f"    Columns: {stats['columns']}")
    print(f"    States covered: {stats['states_covered']}")
    
    if 'total_students' in stats:
        print(f"    Total students: {stats['total_students']:,.0f}")
    if 'avg_admission_rate' in stats and stats['avg_admission_rate']:
        print(f"    Avg admission rate: {stats['avg_admission_rate']*100:.1f}%")
    if 'avg_completion_rate' in stats and stats['avg_completion_rate']:
        print(f"    Avg completion rate: {stats['avg_completion_rate']*100:.1f}%")
    
    # Show completeness
    print("\n[7] Field completeness:")
    print("-" * 70)
    for field_name, _ in required_fields:
        if field_name in stats['completeness']:
            pct = stats['completeness'][field_name]
            bar = "█" * int(pct / 5) + "░" * (20 - int(pct / 5))
            print(f"    {field_name:<30} {bar} {pct:.1f}%")
    
    print("\n" + "=" * 70)
    print("TEST COMPLETED SUCCESSFULLY")
    print("=" * 70)
    
    # Show saved files
    from higher_ed_data_pipeline.config.settings import get_settings
    settings = get_settings()
    staging_files = list(settings.data_staging_path.glob("college_scorecard_*.parquet"))
    if staging_files:
        print(f"\nData saved to: {staging_files[-1]}")
    
    return 0


if __name__ == "__main__":
    import pandas as pd
    sys.exit(main())
