#!/usr/bin/env python3
"""Test duplicate detection and validate_data function."""

import pandas as pd
import sys
sys.path.insert(0, ".")

from scripts.validate import validate_data, validate_scorecard_data


def test_duplicate_detection():
    """Test that duplicate records are detected and removed."""
    print("=" * 60)
    print("TEST: Duplicate Detection")
    print("=" * 60)
    
    # Create test data with duplicates
    test_data = {
        "school_name": ["School A", "School B", "School A", "School C"],
        "school_state": ["CA", "NY", "CA", "TX"],  # School A + CA is duplicated
        "student_size": [1000, 2000, 1000, 3000],
        "admission_rate_overall": [0.5, 0.6, 0.5, 0.7],
        "completion_rate_overall": [0.8, 0.7, 0.8, 0.9],
    }
    df = pd.DataFrame(test_data)
    print(f"\nInput rows: {len(df)}")
    print("Input data:")
    print(df[["school_name", "school_state"]].to_string())
    
    # Run validation
    clean_df = validate_data(df)
    
    print("\n" + "=" * 60)
    print("RESULT")
    print("=" * 60)
    print(f"Output rows: {len(clean_df)}")
    duplicate_removed = len(df) - len(clean_df) == 1
    print(f"Duplicate removed correctly: {duplicate_removed}")
    print("\nRemaining records:")
    print(clean_df[["school_name", "school_state"]].to_string())
    
    return duplicate_removed


def test_validate_data_integration():
    """Test validate_data as integration entry point."""
    print("\n" + "=" * 60)
    print("TEST: validate_data() Integration")
    print("=" * 60)
    
    # Clean data
    test_data = {
        "school_name": ["MIT", "Stanford", "Harvard"],
        "school_state": ["MA", "CA", "MA"],
        "student_size": [10000, 15000, 20000],
        "admission_rate_overall": [0.07, 0.04, 0.05],
        "completion_rate_overall": [0.95, 0.94, 0.97],
    }
    df = pd.DataFrame(test_data)
    
    # Run validation
    clean_df = validate_data(df)
    
    print("\n" + "=" * 60)
    print("RESULT")
    print("=" * 60)
    rows_preserved = len(clean_df) == len(df)
    print(f"All clean rows preserved: {rows_preserved}")
    
    return rows_preserved


if __name__ == "__main__":
    test1 = test_duplicate_detection()
    test2 = test_validate_data_integration()
    
    print("\n" + "=" * 60)
    print("TEST SUMMARY")
    print("=" * 60)
    print(f"  Duplicate Detection: {'✓ PASS' if test1 else '✗ FAIL'}")
    print(f"  Integration Entry: {'✓ PASS' if test2 else '✗ FAIL'}")
    
    if test1 and test2:
        print("\nALL TESTS PASSED")
    else:
        print("\nSOME TESTS FAILED")
        sys.exit(1)
