#!/usr/bin/env python3
"""Test suite for transform.py module."""

import pandas as pd
import sys
sys.path.insert(0, ".")

from scripts.transform import (
    transform_data,
    standardize_columns,
    clean_data,
    create_derived_metrics,
    enforce_data_integrity,
    to_snake_case,
    COLUMN_RENAME_MAP,
)


def test_snake_case_conversion():
    """Test snake_case conversion function."""
    print("=" * 60)
    print("TEST: Snake Case Conversion")
    print("=" * 60)
    
    test_cases = [
        ("schoolName", "school_name"),
        ("School Name", "school_name"),
        ("SCHOOL_NAME", "school_name"),
        ("admission_rate_overall", "admission_rate_overall"),
        ("CamelCaseColumn", "camel_case_column"),
    ]
    
    passed = True
    for input_val, expected in test_cases:
        result = to_snake_case(input_val)
        status = "✓" if result == expected else "✗"
        if result != expected:
            passed = False
        print(f"  {status} '{input_val}' -> '{result}' (expected: '{expected}')")
    
    return passed


def test_column_standardization():
    """Test column renaming."""
    print("\n" + "=" * 60)
    print("TEST: Column Standardization")
    print("=" * 60)
    
    df = pd.DataFrame({
        "admission_rate_overall": [0.5, 0.6],
        "completion_rate_overall": [0.7, 0.8],
        "school_name": ["A", "B"],
        "school_state": ["CA", "NY"],
        "student_size": [1000, 2000],
    })
    
    result = standardize_columns(df, COLUMN_RENAME_MAP)
    
    checks = [
        ("admission_rate" in result.columns, "admission_rate column exists"),
        ("completion_rate" in result.columns, "completion_rate column exists"),
        ("admission_rate_overall" not in result.columns, "old column removed"),
    ]
    
    passed = True
    for check, desc in checks:
        status = "✓" if check else "✗"
        if not check:
            passed = False
        print(f"  {status} {desc}")
    
    return passed


def test_derived_metrics():
    """Test derived metric creation."""
    print("\n" + "=" * 60)
    print("TEST: Derived Metrics")
    print("=" * 60)
    
    df = pd.DataFrame({
        "admission_rate": [0.5, 0.75, 0.25],
        "completion_rate": [0.8, 0.6, 0.9],
        "student_size": [3000, 10000, 20000],
        "school_name": ["A", "B", "C"],
        "school_state": ["CA", "NY", "TX"],
    })
    
    result = create_derived_metrics(df)
    
    checks = [
        ("admission_rate_percentage" in result.columns, "admission_rate_percentage created"),
        ("completion_rate_percentage" in result.columns, "completion_rate_percentage created"),
        ("size_category" in result.columns, "size_category created"),
        (result["admission_rate_percentage"].iloc[0] == 50.0, "admission_rate % calculated correctly"),
        (result["completion_rate_percentage"].iloc[1] == 60.0, "completion_rate % calculated correctly"),
        (result["size_category"].iloc[0] == "small", "small category correct (3000)"),
        (result["size_category"].iloc[1] == "medium", "medium category correct (10000)"),
        (result["size_category"].iloc[2] == "large", "large category correct (20000)"),
    ]
    
    passed = True
    for check, desc in checks:
        status = "✓" if check else "✗"
        if not check:
            passed = False
        print(f"  {status} {desc}")
    
    return passed


def test_data_integrity():
    """Test data integrity enforcement."""
    print("\n" + "=" * 60)
    print("TEST: Data Integrity")
    print("=" * 60)
    
    # Data with duplicates and negative values
    df = pd.DataFrame({
        "school_name": ["A", "A", "B", "C"],  # Row 0 and 1 are duplicates
        "school_state": ["CA", "CA", "NY", "TX"],
        "student_size": [1000, 1000, -500, 2000],  # Row 2 has negative
        "admission_rate": [0.5, 0.5, 0.6, 0.7],
        "completion_rate": [0.8, 0.8, 0.7, 0.9],
    })
    
    result, stats = enforce_data_integrity(df)
    
    checks = [
        (stats["duplicates_removed"] == 1, f"1 duplicate removed (got {stats['duplicates_removed']})"),
        (stats["negative_value_rows_removed"] == 1, f"1 negative row removed (got {stats['negative_value_rows_removed']})"),
        (len(result) == 2, f"Final rows is 2 (got {len(result)})"),
    ]
    
    passed = True
    for check, desc in checks:
        status = "✓" if check else "✗"
        if not check:
            passed = False
        print(f"  {status} {desc}")
    
    return passed


def test_full_transform():
    """Test full transformation pipeline."""
    print("\n" + "=" * 60)
    print("TEST: Full Transform Pipeline")
    print("=" * 60)
    
    df = pd.DataFrame({
        "school_name": ["MIT", "Stanford", "Harvard"],
        "school_state": ["ma", "ca", "ma"],  # lowercase - should be uppercased
        "student_size": [11000, 17000, 21000],
        "admission_rate_overall": [0.07, 0.04, 0.05],
        "completion_rate_overall": [0.95, 0.94, 0.97],
    })
    
    result = transform_data(df, save_output=False)
    
    checks = [
        (len(result) == 3, "All rows preserved"),
        ("admission_rate" in result.columns, "admission_rate column renamed"),
        ("admission_rate_percentage" in result.columns, "admission_rate_percentage derived"),
        ("size_category" in result.columns, "size_category derived"),
        (result["school_state"].iloc[0] == "MA", "State code uppercased"),
        (abs(result["admission_rate_percentage"].iloc[0] - 7.0) < 0.01, "Percentage calculated correctly"),
        (result["size_category"].iloc[0] == "medium", "Size category correct for 11000"),
        (result["size_category"].iloc[1] == "large", "Size category correct for 17000"),
    ]
    
    passed = True
    for check, desc in checks:
        status = "✓" if check else "✗"
        if not check:
            passed = False
        print(f"  {status} {desc}")
    
    return passed


if __name__ == "__main__":
    tests = [
        ("Snake Case Conversion", test_snake_case_conversion),
        ("Column Standardization", test_column_standardization),
        ("Derived Metrics", test_derived_metrics),
        ("Data Integrity", test_data_integrity),
        ("Full Transform Pipeline", test_full_transform),
    ]
    
    results = {}
    for name, test_func in tests:
        try:
            results[name] = test_func()
        except Exception as e:
            print(f"\n✗ Test '{name}' raised exception: {e}")
            results[name] = False
    
    print("\n" + "=" * 60)
    print("TEST SUMMARY")
    print("=" * 60)
    
    all_passed = True
    for name, passed in results.items():
        status = "✓ PASS" if passed else "✗ FAIL"
        if not passed:
            all_passed = False
        print(f"  {status}: {name}")
    
    print()
    if all_passed:
        print("ALL TESTS PASSED")
    else:
        print("SOME TESTS FAILED")
        sys.exit(1)
