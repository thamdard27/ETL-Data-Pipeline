#!/usr/bin/env python3
"""
College Scorecard Data Validation Module

Production-quality validation layer for the extracted College Scorecard dataset.

Implements:
- Schema validation (required columns)
- Null checks with thresholds
- Data type validation with coercion
- Range validation for numeric fields
- Duplicate detection and removal

Usage (Simple - Recommended for Pipeline Integration):
    from scripts.validate import validate_data
    
    clean_df = validate_data(raw_df)

Usage (Detailed - For Custom Configuration):
    from scripts.validate import validate_scorecard_data, ValidationResult
    
    validated_df, result = validate_scorecard_data(df)
    if not result.is_valid:
        print(result.errors)

Configuration:
    All validation parameters are defined at the top of this module
    and can be customized without modifying function code:
    - REQUIRED_COLUMNS: Columns that must exist
    - CRITICAL_COLUMNS: Columns checked for null values
    - NULL_THRESHOLD_PERCENT: Maximum acceptable null percentage
    - EXPECTED_DTYPES: Expected data types for each column
    - RANGE_CONSTRAINTS: Min/max values for numeric columns
    - DUPLICATE_KEY_COLUMNS: Columns used for duplicate detection
"""

import logging
import sys
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Tuple, TYPE_CHECKING

if TYPE_CHECKING:
    import pandas as pd


# =============================================================================
# PUBLIC API
# =============================================================================

__all__ = [
    # Main entry points
    "validate_data",
    "validate_scorecard_data",
    # Individual validators
    "validate_schema",
    "check_nulls",
    "validate_and_coerce_types",
    "validate_ranges",
    "check_duplicates",
    # Result classes
    "ValidationResult",
    "ValidationIssue",
    # Exceptions
    "SchemaValidationError",
    "DataValidationError",
    # Configuration (read-only recommended)
    "REQUIRED_COLUMNS",
    "CRITICAL_COLUMNS",
    "NULL_THRESHOLD_PERCENT",
    "EXPECTED_DTYPES",
    "RANGE_CONSTRAINTS",
    "DUPLICATE_KEY_COLUMNS",
]


# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger(__name__)


# =============================================================================
# VALIDATION CONFIGURATION
# =============================================================================

# Required columns that MUST exist in the DataFrame
REQUIRED_COLUMNS = [
    "school_name",
    "school_state",
    "student_size",
    "admission_rate_overall",
    "completion_rate_overall",
]

# Critical columns that should not have nulls (or very few)
CRITICAL_COLUMNS = [
    "school_name",
    "school_state",
]

# Null threshold - if nulls exceed this percentage, raise warning
NULL_THRESHOLD_PERCENT = 5.0

# Expected data types for validation
EXPECTED_DTYPES = {
    "school_name": "string",
    "school_state": "string",
    "student_size": "numeric",  # Int64 or float
    "admission_rate_overall": "float",
    "completion_rate_overall": "float",
}

# Range constraints for numeric fields
RANGE_CONSTRAINTS = {
    "admission_rate_overall": {"min": 0.0, "max": 1.0},
    "completion_rate_overall": {"min": 0.0, "max": 1.0},
    "student_size": {"min": 0, "max": None},  # No upper limit
}


# =============================================================================
# VALIDATION RESULT CLASSES
# =============================================================================

@dataclass
class ValidationIssue:
    """Represents a single validation issue."""
    severity: str  # "ERROR", "WARNING", "INFO"
    check_name: str
    message: str
    affected_rows: int = 0
    affected_columns: List[str] = field(default_factory=list)
    details: Dict[str, Any] = field(default_factory=dict)


@dataclass
class ValidationResult:
    """Complete validation result for a DataFrame."""
    is_valid: bool = True
    total_rows: int = 0
    validated_rows: int = 0
    removed_rows: int = 0
    issues: List[ValidationIssue] = field(default_factory=list)
    corrections: List[str] = field(default_factory=list)
    
    @property
    def errors(self) -> List[ValidationIssue]:
        """Return only ERROR severity issues."""
        return [i for i in self.issues if i.severity == "ERROR"]
    
    @property
    def warnings(self) -> List[ValidationIssue]:
        """Return only WARNING severity issues."""
        return [i for i in self.issues if i.severity == "WARNING"]
    
    def add_issue(self, issue: ValidationIssue) -> None:
        """Add an issue and update validity status."""
        self.issues.append(issue)
        if issue.severity == "ERROR":
            self.is_valid = False
    
    def add_correction(self, correction: str) -> None:
        """Log a correction that was made."""
        self.corrections.append(correction)
    
    def summary(self) -> str:
        """Generate a summary string."""
        lines = [
            "=" * 60,
            "VALIDATION SUMMARY",
            "=" * 60,
            f"Valid: {self.is_valid}",
            f"Total rows: {self.total_rows}",
            f"Validated rows: {self.validated_rows}",
            f"Removed rows: {self.removed_rows}",
            f"Errors: {len(self.errors)}",
            f"Warnings: {len(self.warnings)}",
            f"Corrections: {len(self.corrections)}",
        ]
        return "\n".join(lines)


# =============================================================================
# VALIDATION EXCEPTION
# =============================================================================

class SchemaValidationError(Exception):
    """Raised when required columns are missing from the DataFrame."""
    pass


class DataValidationError(Exception):
    """Raised when data validation fails critically."""
    pass


# =============================================================================
# TASK 2: SCHEMA VALIDATION
# =============================================================================

def validate_schema(
    df: "pd.DataFrame",
    required_columns: List[str] = REQUIRED_COLUMNS,
) -> Tuple[bool, List[str]]:
    """
    Validate that all required columns exist in the DataFrame.
    
    Args:
        df: DataFrame to validate
        required_columns: List of column names that must exist
        
    Returns:
        Tuple of (is_valid, missing_columns)
        
    Raises:
        SchemaValidationError: If any required column is missing
    """
    logger.info("=" * 60)
    logger.info("SCHEMA VALIDATION")
    logger.info("=" * 60)
    
    actual_columns = set(df.columns)
    required_set = set(required_columns)
    missing = required_set - actual_columns
    
    if missing:
        missing_list = sorted(list(missing))
        logger.error(f"SCHEMA VALIDATION FAILED - Missing columns: {missing_list}")
        logger.error("Stopping execution - cannot proceed without required columns")
        raise SchemaValidationError(
            f"Missing required columns: {missing_list}. "
            f"Available columns: {sorted(list(actual_columns))}"
        )
    
    logger.info(f"✓ All {len(required_columns)} required columns present")
    for col in required_columns:
        logger.info(f"  - {col}: {df[col].dtype}")
    
    return True, []


# =============================================================================
# TASK 3: NULL CHECKS
# =============================================================================

def check_nulls(
    df: "pd.DataFrame",
    critical_columns: List[str] = CRITICAL_COLUMNS,
    threshold_percent: float = NULL_THRESHOLD_PERCENT,
) -> ValidationResult:
    """
    Check for null values in critical columns.
    
    Args:
        df: DataFrame to check
        critical_columns: Columns that should have minimal nulls
        threshold_percent: Percentage threshold for warning
        
    Returns:
        ValidationResult with null check findings
    """
    logger.info("=" * 60)
    logger.info("NULL VALUE CHECK")
    logger.info("=" * 60)
    
    result = ValidationResult(total_rows=len(df))
    
    for col in critical_columns:
        if col not in df.columns:
            continue
            
        null_count = df[col].isna().sum()
        null_percent = (null_count / len(df)) * 100 if len(df) > 0 else 0
        
        logger.info(f"Column '{col}': {null_count} nulls ({null_percent:.2f}%)")
        
        if null_percent > threshold_percent:
            issue = ValidationIssue(
                severity="WARNING",
                check_name="null_check",
                message=f"Column '{col}' has {null_percent:.2f}% null values (threshold: {threshold_percent}%)",
                affected_rows=null_count,
                affected_columns=[col],
                details={"null_count": null_count, "null_percent": null_percent},
            )
            result.add_issue(issue)
            logger.warning(f"⚠ {issue.message}")
        else:
            logger.info(f"  ✓ Within acceptable threshold ({threshold_percent}%)")
    
    return result


# =============================================================================
# TASK 4: DATA TYPE VALIDATION
# =============================================================================

def validate_and_coerce_types(
    df: "pd.DataFrame",
    expected_dtypes: Dict[str, str] = EXPECTED_DTYPES,
) -> Tuple["pd.DataFrame", ValidationResult]:
    """
    Validate and coerce data types where possible.
    
    Args:
        df: DataFrame to validate
        expected_dtypes: Dict mapping column names to expected type categories
                        ("string", "numeric", "float", "int")
        
    Returns:
        Tuple of (corrected_df, validation_result)
    """
    import pandas as pd
    
    logger.info("=" * 60)
    logger.info("DATA TYPE VALIDATION")
    logger.info("=" * 60)
    
    result = ValidationResult(total_rows=len(df))
    df = df.copy()  # Don't modify original
    
    for col, expected_type in expected_dtypes.items():
        if col not in df.columns:
            continue
            
        actual_dtype = str(df[col].dtype)
        logger.info(f"Column '{col}': expected={expected_type}, actual={actual_dtype}")
        
        try:
            if expected_type == "numeric":
                # Should be Int64 or float
                if actual_dtype not in ("Int64", "int64", "float64"):
                    original_nulls = df[col].isna().sum()
                    df[col] = pd.to_numeric(df[col], errors="coerce")
                    new_nulls = df[col].isna().sum()
                    coerced = new_nulls - original_nulls
                    
                    if coerced > 0:
                        correction = f"Coerced {coerced} invalid values to NaN in '{col}'"
                        result.add_correction(correction)
                        logger.info(f"  → {correction}")
                    else:
                        logger.info(f"  ✓ Type coercion successful")
                else:
                    logger.info(f"  ✓ Type is valid")
                    
            elif expected_type == "float":
                if actual_dtype != "float64":
                    original_nulls = df[col].isna().sum()
                    df[col] = pd.to_numeric(df[col], errors="coerce").astype("float64")  # type: ignore[union-attr]
                    new_nulls = df[col].isna().sum()
                    coerced = new_nulls - original_nulls
                    
                    if coerced > 0:
                        correction = f"Coerced {coerced} invalid values to NaN in '{col}'"
                        result.add_correction(correction)
                        logger.info(f"  → {correction}")
                    else:
                        logger.info(f"  ✓ Type coercion successful")
                else:
                    logger.info(f"  ✓ Type is valid")
                    
            elif expected_type == "string":
                if actual_dtype not in ("string", "object"):
                    df[col] = df[col].astype("string")
                    result.add_correction(f"Converted '{col}' to string type")
                    logger.info(f"  → Converted to string")
                else:
                    logger.info(f"  ✓ Type is valid")
                    
        except Exception as e:
            issue = ValidationIssue(
                severity="ERROR",
                check_name="dtype_validation",
                message=f"Failed to coerce column '{col}' to {expected_type}: {e}",
                affected_columns=[col],
            )
            result.add_issue(issue)
            logger.error(f"  ✗ {issue.message}")
    
    return df, result


# =============================================================================
# TASK 5: RANGE VALIDATION
# =============================================================================

def validate_ranges(
    df: "pd.DataFrame",
    range_constraints: Dict[str, Dict[str, Optional[float]]] = RANGE_CONSTRAINTS,
    remove_invalid: bool = True,
) -> Tuple["pd.DataFrame", ValidationResult]:
    """
    Validate that numeric values fall within expected ranges.
    
    Args:
        df: DataFrame to validate
        range_constraints: Dict mapping column names to {"min": x, "max": y}
        remove_invalid: If True, remove rows with invalid values; if False, flag them
        
    Returns:
        Tuple of (cleaned_df, validation_result)
    """
    import pandas as pd
    
    logger.info("=" * 60)
    logger.info("RANGE VALIDATION")
    logger.info("=" * 60)
    
    result = ValidationResult(total_rows=len(df))
    df = df.copy()
    
    # Track rows to remove
    invalid_mask = pd.Series([False] * len(df), index=df.index)
    
    for col, constraints in range_constraints.items():
        if col not in df.columns:
            continue
            
        min_val = constraints.get("min")
        max_val = constraints.get("max")
        
        logger.info(f"Column '{col}': range [{min_val}, {max_val}]")
        
        # Find violations (excluding nulls)
        col_data = df[col]
        non_null_mask = col_data.notna()
        
        violations = pd.Series([False] * len(df), index=df.index)
        
        if min_val is not None:
            below_min = (col_data < min_val) & non_null_mask
            below_count = below_min.sum()
            if below_count > 0:
                violations |= below_min
                logger.warning(f"  ⚠ {below_count} values below minimum ({min_val})")
                
                # Log sample of invalid values
                invalid_values = col_data[below_min].head(5).tolist()
                logger.warning(f"    Sample invalid values: {invalid_values}")
        
        if max_val is not None:
            above_max = (col_data > max_val) & non_null_mask
            above_count = above_max.sum()
            if above_count > 0:
                violations |= above_max
                logger.warning(f"  ⚠ {above_count} values above maximum ({max_val})")
                
                # Log sample of invalid values
                invalid_values = col_data[above_max].head(5).tolist()
                logger.warning(f"    Sample invalid values: {invalid_values}")
        
        violation_count = violations.sum()
        
        if violation_count > 0:
            issue = ValidationIssue(
                severity="WARNING",
                check_name="range_validation",
                message=f"Column '{col}' has {violation_count} out-of-range values",
                affected_rows=violation_count,
                affected_columns=[col],
                details={
                    "constraint_min": min_val,
                    "constraint_max": max_val,
                    "violation_count": violation_count,
                },
            )
            result.add_issue(issue)
            
            if remove_invalid:
                invalid_mask |= violations
        else:
            logger.info(f"  ✓ All non-null values within valid range")
    
    # Remove invalid rows if requested
    if remove_invalid:
        removed_count = invalid_mask.sum()
        if removed_count > 0:
            df = pd.DataFrame(df[~invalid_mask]).reset_index(drop=True)
            result.removed_rows = removed_count
            result.add_correction(f"Removed {removed_count} rows with out-of-range values")
            logger.info(f"→ Removed {removed_count} invalid rows")
    
    result.validated_rows = len(df)
    return df, result


# =============================================================================
# TASK 6: DUPLICATE DETECTION
# =============================================================================

# Columns used to identify unique records
DUPLICATE_KEY_COLUMNS = ["school_name", "school_state"]


def check_duplicates(
    df: "pd.DataFrame",
    key_columns: List[str] = DUPLICATE_KEY_COLUMNS,
    remove_duplicates: bool = True,
    keep: str = "first",
) -> Tuple["pd.DataFrame", ValidationResult]:
    """
    Check for and optionally remove duplicate records.
    
    Duplicates are identified based on the combination of key columns
    (default: school_name + school_state).
    
    Args:
        df: DataFrame to check for duplicates
        key_columns: Columns to use for duplicate detection
        remove_duplicates: If True, remove duplicate rows (keep first occurrence)
        keep: Which duplicate to keep ('first', 'last', or False to drop all)
        
    Returns:
        Tuple of (deduplicated_df, validation_result)
    """
    import pandas as pd
    
    logger.info("=" * 60)
    logger.info("DUPLICATE DETECTION")
    logger.info("=" * 60)
    logger.info(f"Checking duplicates on columns: {key_columns}")
    
    result = ValidationResult(total_rows=len(df))
    
    # Verify key columns exist
    missing_keys = [col for col in key_columns if col not in df.columns]
    if missing_keys:
        logger.error(f"Cannot check duplicates - missing key columns: {missing_keys}")
        result.add_issue(ValidationIssue(
            severity="ERROR",
            check_name="duplicate_detection",
            message=f"Missing key columns for duplicate check: {missing_keys}",
            affected_columns=missing_keys,
        ))
        return df, result
    
    # Find duplicates
    duplicate_mask = df.duplicated(subset=key_columns, keep=False)
    total_duplicate_rows = duplicate_mask.sum()
    
    # Count unique duplicate groups (how many records appear more than once)
    if total_duplicate_rows > 0:
        # Get rows that would be removed (duplicates to drop)
        rows_to_remove = df.duplicated(subset=key_columns, keep=keep)
        duplicate_count = rows_to_remove.sum()
        
        logger.warning(f"⚠ Found {total_duplicate_rows} rows involved in duplicates")
        logger.warning(f"  - Unique duplicate groups: {total_duplicate_rows - duplicate_count}")
        logger.warning(f"  - Rows to remove (keeping '{keep}'): {duplicate_count}")
        
        # Log sample duplicates
        sample_duplicates = df[duplicate_mask].head(10)
        if len(sample_duplicates) > 0:
            logger.info("Sample duplicate records:")
            for idx, row in sample_duplicates.iterrows():
                key_values = [str(row[col]) for col in key_columns]
                logger.info(f"    Row {idx}: {' | '.join(key_values)}")
        
        result.add_issue(ValidationIssue(
            severity="WARNING",
            check_name="duplicate_detection",
            message=f"Found {duplicate_count} duplicate records based on {key_columns}",
            affected_rows=duplicate_count,
            affected_columns=key_columns,
            details={
                "total_rows_involved": total_duplicate_rows,
                "rows_to_remove": duplicate_count,
                "keep_strategy": keep,
            },
        ))
        
        if remove_duplicates:
            df = df.drop_duplicates(subset=key_columns, keep=keep).reset_index(drop=True)
            result.removed_rows = duplicate_count
            result.add_correction(f"Removed {duplicate_count} duplicate rows (kept '{keep}')")
            logger.info(f"→ Removed {duplicate_count} duplicate rows")
            logger.info(f"  Remaining rows: {len(df)}")
    else:
        logger.info("✓ No duplicate records found")
    
    result.validated_rows = len(df)
    return df, result


# =============================================================================
# MAIN VALIDATION FUNCTION
# =============================================================================

def validate_scorecard_data(
    df: "pd.DataFrame",
    remove_invalid_ranges: bool = True,
    remove_duplicates: bool = True,
    raise_on_schema_error: bool = True,
) -> Tuple["pd.DataFrame", ValidationResult]:
    """
    Run complete validation pipeline on College Scorecard data.
    
    This is the main entry point for data validation.
    
    Args:
        df: DataFrame to validate (from fetch_scorecard_data)
        remove_invalid_ranges: Remove rows with out-of-range values
        remove_duplicates: Remove duplicate records
        raise_on_schema_error: Raise exception if required columns missing
        
    Returns:
        Tuple of (validated_df, validation_result)
        
    Raises:
        SchemaValidationError: If required columns are missing (when raise_on_schema_error=True)
    """
    logger.info("=" * 60)
    logger.info("COLLEGE SCORECARD DATA VALIDATION - START")
    logger.info("=" * 60)
    logger.info(f"Input DataFrame: {len(df)} rows, {len(df.columns)} columns")
    
    # Combined result
    final_result = ValidationResult(total_rows=len(df))
    validated_df = df.copy()
    
    # TASK 2: Schema validation
    try:
        validate_schema(validated_df, REQUIRED_COLUMNS)
    except SchemaValidationError as e:
        if raise_on_schema_error:
            raise
        final_result.add_issue(ValidationIssue(
            severity="ERROR",
            check_name="schema_validation",
            message=str(e),
        ))
        return validated_df, final_result
    
    # TASK 3: Null checks
    null_result = check_nulls(validated_df, CRITICAL_COLUMNS, NULL_THRESHOLD_PERCENT)
    final_result.issues.extend(null_result.issues)
    
    # TASK 4: Data type validation
    validated_df, dtype_result = validate_and_coerce_types(validated_df, EXPECTED_DTYPES)
    final_result.issues.extend(dtype_result.issues)
    final_result.corrections.extend(dtype_result.corrections)
    
    # TASK 5: Range validation
    validated_df, range_result = validate_ranges(
        validated_df, 
        RANGE_CONSTRAINTS, 
        remove_invalid=remove_invalid_ranges
    )
    final_result.issues.extend(range_result.issues)
    final_result.corrections.extend(range_result.corrections)
    final_result.removed_rows += range_result.removed_rows
    
    # TASK 6: Duplicate detection
    validated_df, dup_result = check_duplicates(
        validated_df,
        key_columns=DUPLICATE_KEY_COLUMNS,
        remove_duplicates=remove_duplicates,
    )
    final_result.issues.extend(dup_result.issues)
    final_result.corrections.extend(dup_result.corrections)
    final_result.removed_rows += dup_result.removed_rows
    
    # Final stats
    final_result.validated_rows = len(validated_df)
    final_result.is_valid = len(final_result.errors) == 0
    
    # Summary
    logger.info("=" * 60)
    logger.info("VALIDATION COMPLETE")
    logger.info("=" * 60)
    logger.info(f"Input rows: {final_result.total_rows}")
    logger.info(f"Output rows: {final_result.validated_rows}")
    logger.info(f"Removed rows: {final_result.removed_rows}")
    logger.info(f"Errors: {len(final_result.errors)}")
    logger.info(f"Warnings: {len(final_result.warnings)}")
    logger.info(f"Corrections: {len(final_result.corrections)}")
    logger.info(f"Overall valid: {final_result.is_valid}")
    
    return validated_df, final_result


# =============================================================================
# TASK 7: INTEGRATION ENTRY POINT
# =============================================================================

def validate_data(df: "pd.DataFrame") -> "pd.DataFrame":
    """
    Validate and clean a College Scorecard DataFrame.
    
    This is the primary entry point for pipeline integration.
    Runs all validations and returns a clean DataFrame.
    
    Args:
        df: Input DataFrame (from fetch_scorecard_data)
        
    Returns:
        Cleaned/validated DataFrame with:
        - Invalid rows removed
        - Duplicates removed
        - Types coerced where possible
        
    Raises:
        SchemaValidationError: If required columns are missing
        
    Example:
        >>> from scripts.extract_scorecard import fetch_scorecard_data
        >>> from scripts.validate import validate_data
        >>> 
        >>> raw_df = fetch_scorecard_data(min_records=1000)
        >>> clean_df = validate_data(raw_df)
    """
    logger.info("=" * 60)
    logger.info("VALIDATE_DATA ENTRY POINT")
    logger.info("=" * 60)
    
    validated_df, result = validate_scorecard_data(
        df,
        remove_invalid_ranges=True,
        remove_duplicates=True,
        raise_on_schema_error=True,
    )
    
    # Log summary for integration visibility
    logger.info("=" * 60)
    logger.info("VALIDATE_DATA SUMMARY")
    logger.info("=" * 60)
    logger.info(f"Input: {result.total_rows} rows → Output: {result.validated_rows} rows")
    logger.info(f"Removed: {result.removed_rows} rows ({result.removed_rows/result.total_rows*100:.2f}% of input)")
    logger.info(f"Status: {'VALID' if result.is_valid else 'INVALID - Check errors'}")
    
    if result.warnings:
        logger.warning(f"Warnings ({len(result.warnings)}):")
        for w in result.warnings:
            logger.warning(f"  - {w.message}")
    
    if result.corrections:
        logger.info(f"Corrections applied: {len(result.corrections)}")
        for c in result.corrections:
            logger.info(f"  - {c}")
    
    return validated_df


# =============================================================================
# CLI EXECUTION
# =============================================================================

def main() -> int:
    """Main entry point for CLI execution."""
    import pandas as pd
    import sys
    from pathlib import Path
    
    # Add parent directory to path for imports
    script_dir = Path(__file__).parent
    project_root = script_dir.parent
    if str(project_root) not in sys.path:
        sys.path.insert(0, str(project_root))
    
    # Import extraction module
    try:
        from scripts.extract_scorecard import fetch_scorecard_data
    except ImportError:
        # Try alternative import path
        try:
            from extract_scorecard import fetch_scorecard_data
        except ImportError:
            logger.error("Could not import fetch_scorecard_data")
            return 1
    
    logger.info("Fetching sample data for validation test...")
    
    # Fetch sample data
    df = fetch_scorecard_data(min_records=500, max_records=500, save_files=False)
    
    # Run validation
    validated_df, result = validate_scorecard_data(df)
    
    # Print result summary
    print("\n" + result.summary())
    
    # Show sample of validated data
    print("\n" + "=" * 60)
    print("VALIDATED DATA PREVIEW")
    print("=" * 60)
    print(validated_df[REQUIRED_COLUMNS].head(10).to_string())
    
    return 0 if result.is_valid else 1


if __name__ == "__main__":
    sys.exit(main())
