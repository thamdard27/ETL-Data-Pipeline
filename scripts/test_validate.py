#!/usr/bin/env python3
"""Test script for validation module."""

import sys
sys.path.insert(0, '.')

import pandas as pd
from scripts.validate import validate_scorecard_data, SchemaValidationError

def test_schema_validation():
    """Test that schema validation catches missing columns."""
    print("TEST 1: Schema validation with missing column")
    print("=" * 60)
    
    bad_df = pd.DataFrame({
        'school_name': ['Test University'],
        'school_state': ['CA'],
        # Missing: student_size, admission_rate_overall, completion_rate_overall
    })
    
    try:
        validated_df, result = validate_scorecard_data(bad_df)
        print("✗ SchemaValidationError was NOT raised")
        return False
    except SchemaValidationError as e:
        print(f"✓ SchemaValidationError raised correctly")
        print(f"  Message: {e}")
        return True


def test_range_validation():
    """Test that range validation catches invalid values."""
    print("\nTEST 2: Range validation with invalid values")
    print("=" * 60)
    
    bad_range_df = pd.DataFrame({
        'school_name': ['Test 1', 'Test 2', 'Test 3'],
        'school_state': ['CA', 'NY', 'TX'],
        'student_size': [1000, -500, 2000],  # -500 is invalid
        'admission_rate_overall': [0.5, 1.5, 0.8],  # 1.5 is invalid (>1)
        'completion_rate_overall': [0.6, 0.7, -0.1],  # -0.1 is invalid (<0)
    })
    
    validated_df, result = validate_scorecard_data(bad_range_df, remove_invalid_ranges=True)
    
    print(f"\nInput rows: {result.total_rows}")
    print(f"Output rows: {result.validated_rows}")
    print(f"Removed rows: {result.removed_rows}")
    print(f"Warnings: {len(result.warnings)}")
    for w in result.warnings:
        print(f"  - {w.message}")
    
    # Should have removed 3 rows (all have at least one invalid value)
    return result.removed_rows > 0


def test_null_threshold():
    """Test that null threshold warning is raised."""
    print("\nTEST 3: Null threshold warning")
    print("=" * 60)
    
    # Create data with >5% nulls in school_name
    names = ['School ' + str(i) for i in range(94)] + [None] * 6  # 6% nulls
    states = ['CA'] * 100
    
    null_df = pd.DataFrame({
        'school_name': names,
        'school_state': states,
        'student_size': [1000] * 100,
        'admission_rate_overall': [0.5] * 100,
        'completion_rate_overall': [0.6] * 100,
    })
    
    validated_df, result = validate_scorecard_data(null_df)
    
    print(f"\nWarnings: {len(result.warnings)}")
    for w in result.warnings:
        print(f"  - {w.message}")
    
    # Should have a warning about nulls exceeding threshold
    null_warnings = [w for w in result.warnings if 'null' in w.message.lower()]
    return len(null_warnings) > 0


if __name__ == "__main__":
    print("=" * 60)
    print("VALIDATION MODULE TEST SUITE")
    print("=" * 60)
    print()
    
    results = []
    
    results.append(("Schema Validation", test_schema_validation()))
    results.append(("Range Validation", test_range_validation()))
    results.append(("Null Threshold", test_null_threshold()))
    
    print("\n" + "=" * 60)
    print("TEST RESULTS")
    print("=" * 60)
    
    all_passed = True
    for name, passed in results:
        status = "✓ PASS" if passed else "✗ FAIL"
        print(f"  {name}: {status}")
        if not passed:
            all_passed = False
    
    print()
    if all_passed:
        print("ALL TESTS PASSED")
        sys.exit(0)
    else:
        print("SOME TESTS FAILED")
        sys.exit(1)
