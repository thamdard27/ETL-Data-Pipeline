#!/usr/bin/env python3
"""
College Scorecard Data Transformation Module

Transforms validated College Scorecard data into a clean, structured,
analytics-ready dataset.

Implements:
- Column standardization (lowercase, snake_case)
- Data cleaning (whitespace, state codes, nulls)
- Derived metrics (percentages, size categories)
- Data integrity enforcement (duplicates, negative values)
- Output to processed CSV files

Usage:
    from scripts.transform import transform_data
    
    clean_df = transform_data(validated_df)

Configuration:
    Output directory: data/processed/
    File format: processed_YYYY_MM_DD_HHMMSS.csv
"""

import logging
import re
import sys
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple, TYPE_CHECKING

if TYPE_CHECKING:
    import pandas as pd

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger(__name__)


# =============================================================================
# PUBLIC API
# =============================================================================

__all__ = [
    # Main entry point
    "transform_data",
    # Individual transformers
    "standardize_columns",
    "clean_data",
    "create_derived_metrics",
    "enforce_data_integrity",
    "save_processed_data",
    # Configuration
    "COLUMN_RENAME_MAP",
    "SIZE_CATEGORY_THRESHOLDS",
    "OUTPUT_DIR",
]


# =============================================================================
# CONFIGURATION
# =============================================================================

# Column rename mapping (source -> target)
# Maps from validation module output to final analytics columns
COLUMN_RENAME_MAP = {
    "school_name": "school_name",
    "school_state": "school_state",
    "student_size": "student_size",
    "admission_rate_overall": "admission_rate",
    "completion_rate_overall": "completion_rate",
}

# Final expected columns after transformation
FINAL_COLUMNS = [
    "school_name",
    "school_state", 
    "student_size",
    "admission_rate",
    "completion_rate",
    "admission_rate_percentage",
    "completion_rate_percentage",
    "size_category",
]

# Size category thresholds
SIZE_CATEGORY_THRESHOLDS = {
    "small": {"max": 5000},       # < 5,000
    "medium": {"min": 5000, "max": 15000},  # 5,000 - 15,000
    "large": {"min": 15000},      # > 15,000
}

# Output configuration
OUTPUT_DIR = Path(__file__).parent.parent / "data" / "processed"


# =============================================================================
# TASK 3: COLUMN STANDARDIZATION
# =============================================================================

def to_snake_case(name: str) -> str:
    """
    Convert a string to snake_case.
    
    Examples:
        "schoolName" -> "school_name"
        "School Name" -> "school_name"
        "SCHOOL_NAME" -> "school_name"
    """
    # Replace spaces and hyphens with underscores
    s1 = re.sub(r'[\s\-]+', '_', name)
    # Insert underscore before uppercase letters (for camelCase)
    s2 = re.sub(r'([a-z0-9])([A-Z])', r'\1_\2', s1)
    # Convert to lowercase and remove duplicate underscores
    s3 = re.sub(r'_+', '_', s2.lower())
    # Strip leading/trailing underscores
    return s3.strip('_')


def standardize_columns(
    df: "pd.DataFrame",
    rename_map: Dict[str, str] = COLUMN_RENAME_MAP,
) -> "pd.DataFrame":
    """
    Standardize column names to lowercase snake_case and apply renames.
    
    Args:
        df: Input DataFrame
        rename_map: Dictionary mapping source columns to target names
        
    Returns:
        DataFrame with standardized column names
    """
    import pandas as pd
    
    logger.info("=" * 60)
    logger.info("COLUMN STANDARDIZATION")
    logger.info("=" * 60)
    
    original_columns = list(df.columns)
    logger.info(f"Input columns: {original_columns}")
    
    # Step 1: Convert all columns to snake_case
    snake_case_map = {col: to_snake_case(col) for col in df.columns}
    df = df.rename(columns=snake_case_map)
    
    # Log any conversions made
    conversions = [(k, v) for k, v in snake_case_map.items() if k != v]
    if conversions:
        logger.info(f"Snake_case conversions: {len(conversions)}")
        for old, new in conversions:
            logger.info(f"  {old} -> {new}")
    
    # Step 2: Apply explicit renames from config
    # Filter rename_map to only include columns that exist
    actual_renames = {k: v for k, v in rename_map.items() if k in df.columns and k != v}
    
    if actual_renames:
        df = df.rename(columns=actual_renames)
        logger.info(f"Explicit renames applied: {len(actual_renames)}")
        for old, new in actual_renames.items():
            logger.info(f"  {old} -> {new}")
    
    final_columns = list(df.columns)
    logger.info(f"Output columns: {final_columns}")
    
    return df


# =============================================================================
# TASK 4: DATA CLEANING
# =============================================================================

def clean_data(df: "pd.DataFrame") -> Tuple["pd.DataFrame", Dict[str, Any]]:
    """
    Clean the DataFrame by trimming whitespace, standardizing values,
    and handling nulls safely.
    
    Args:
        df: Input DataFrame
        
    Returns:
        Tuple of (cleaned DataFrame, cleaning stats dict)
    """
    import pandas as pd
    import numpy as np
    
    logger.info("=" * 60)
    logger.info("DATA CLEANING")
    logger.info("=" * 60)
    
    stats = {
        "whitespace_trimmed": 0,
        "state_codes_standardized": 0,
        "null_strings_converted": 0,
        "rows_before": len(df),
    }
    
    # Make a copy to avoid modifying original
    df = df.copy()
    
    # -------------------------------------------------------------------------
    # Trim whitespace from string fields
    # -------------------------------------------------------------------------
    string_columns = df.select_dtypes(include=['object']).columns.tolist()
    
    for col in string_columns:
        if col in df.columns:
            # Count cells that will change
            original = df[col].astype(str)
            trimmed = original.str.strip()
            changed = (original != trimmed).sum()
            stats["whitespace_trimmed"] += changed
            
            # Apply trim
            df[col] = df[col].astype(str).str.strip()
            
            # Convert 'nan', 'None', empty strings back to actual NaN
            df[col] = df[col].replace(['nan', 'None', 'NaN', '', 'null', 'NULL'], np.nan)
    
    if stats["whitespace_trimmed"] > 0:
        logger.info(f"✓ Trimmed whitespace from {stats['whitespace_trimmed']} cells")
    else:
        logger.info("✓ No whitespace trimming needed")
    
    # -------------------------------------------------------------------------
    # Standardize state codes (uppercase, 2-letter format)
    # -------------------------------------------------------------------------
    if "school_state" in df.columns:
        # Convert to uppercase
        original_states = df["school_state"].copy()
        df["school_state"] = df["school_state"].astype(str).str.upper().str.strip()
        
        # Handle 'NAN' string from conversion
        df["school_state"] = df["school_state"].replace(['NAN', 'NONE', ''], np.nan)
        
        # Count standardized
        changed_mask = (original_states != df["school_state"]) & df["school_state"].notna()
        stats["state_codes_standardized"] = changed_mask.sum()
        
        # Validate 2-letter format (log warning if not)
        if df["school_state"].notna().any():
            non_standard = df[
                df["school_state"].notna() & 
                (df["school_state"].str.len() != 2)
            ]
            if len(non_standard) > 0:
                logger.warning(f"⚠ {len(non_standard)} state codes are not 2-letter format")
                sample = non_standard["school_state"].head(5).tolist()
                logger.warning(f"  Sample: {sample}")
        
        logger.info(f"✓ Standardized {stats['state_codes_standardized']} state codes to uppercase")
    
    # -------------------------------------------------------------------------
    # Ensure no invalid strings in numeric columns
    # -------------------------------------------------------------------------
    numeric_expected = ["student_size", "admission_rate", "completion_rate"]
    
    for col in numeric_expected:
        if col in df.columns:
            # Check if column is not already numeric
            if not pd.api.types.is_numeric_dtype(df[col]):
                logger.warning(f"Column '{col}' is not numeric, attempting conversion")
                df[col] = pd.to_numeric(df[col], errors="coerce")
    
    # -------------------------------------------------------------------------
    # Handle remaining null values safely
    # -------------------------------------------------------------------------
    null_counts = df.isnull().sum()
    total_nulls = null_counts.sum()
    
    if total_nulls > 0:
        logger.info(f"Null values in dataset:")
        for col, count in null_counts.items():
            if count > 0:
                pct = count / len(df) * 100
                logger.info(f"  {col}: {count} ({pct:.2f}%)")
    else:
        logger.info("✓ No null values in dataset")
    
    stats["rows_after"] = len(df)
    stats["total_nulls"] = int(total_nulls)
    
    return df, stats


# =============================================================================
# TASK 5: DERIVED METRICS
# =============================================================================

def create_derived_metrics(df: "pd.DataFrame") -> "pd.DataFrame":
    """
    Create derived analytical columns.
    
    Creates:
    - admission_rate_percentage: admission_rate * 100
    - completion_rate_percentage: completion_rate * 100
    - size_category: small/medium/large based on student_size
    
    Args:
        df: Input DataFrame with admission_rate, completion_rate, student_size
        
    Returns:
        DataFrame with new derived columns
    """
    import pandas as pd
    import numpy as np
    
    logger.info("=" * 60)
    logger.info("DERIVED METRICS")
    logger.info("=" * 60)
    
    df = df.copy()
    created_columns = []
    
    # -------------------------------------------------------------------------
    # Admission Rate Percentage
    # -------------------------------------------------------------------------
    if "admission_rate" in df.columns:
        df["admission_rate_percentage"] = df["admission_rate"] * 100
        created_columns.append("admission_rate_percentage")
        
        # Log statistics
        valid_values = df["admission_rate_percentage"].dropna()
        if len(valid_values) > 0:
            logger.info(f"✓ Created 'admission_rate_percentage'")
            logger.info(f"    Range: {valid_values.min():.2f}% - {valid_values.max():.2f}%")
            logger.info(f"    Mean: {valid_values.mean():.2f}%")
    else:
        logger.warning("⚠ Cannot create admission_rate_percentage - 'admission_rate' column missing")
    
    # -------------------------------------------------------------------------
    # Completion Rate Percentage
    # -------------------------------------------------------------------------
    if "completion_rate" in df.columns:
        df["completion_rate_percentage"] = df["completion_rate"] * 100
        created_columns.append("completion_rate_percentage")
        
        # Log statistics
        valid_values = df["completion_rate_percentage"].dropna()
        if len(valid_values) > 0:
            logger.info(f"✓ Created 'completion_rate_percentage'")
            logger.info(f"    Range: {valid_values.min():.2f}% - {valid_values.max():.2f}%")
            logger.info(f"    Mean: {valid_values.mean():.2f}%")
    else:
        logger.warning("⚠ Cannot create completion_rate_percentage - 'completion_rate' column missing")
    
    # -------------------------------------------------------------------------
    # Size Category
    # -------------------------------------------------------------------------
    if "student_size" in df.columns:
        def categorize_size(size: Any) -> Optional[str]:
            """Categorize student size into small/medium/large."""
            if pd.isna(size):
                return None
            try:
                size = float(size)
                if size < SIZE_CATEGORY_THRESHOLDS["small"]["max"]:
                    return "small"
                elif size <= SIZE_CATEGORY_THRESHOLDS["medium"]["max"]:
                    return "medium"
                else:
                    return "large"
            except (ValueError, TypeError):
                return None
        
        df["size_category"] = df["student_size"].apply(categorize_size)
        created_columns.append("size_category")
        
        # Log distribution
        size_dist = df["size_category"].value_counts(dropna=False)
        logger.info(f"✓ Created 'size_category'")
        logger.info(f"    Thresholds: small < 5,000 | medium 5,000-15,000 | large > 15,000")
        logger.info(f"    Distribution:")
        for category, count in size_dist.items():
            pct = count / len(df) * 100
            cat_label = category if category is not None else "null"
            logger.info(f"      {cat_label}: {count} ({pct:.1f}%)")
    else:
        logger.warning("⚠ Cannot create size_category - 'student_size' column missing")
    
    logger.info(f"Total derived columns created: {len(created_columns)}")
    
    return df


# =============================================================================
# TASK 6: DATA INTEGRITY ENFORCEMENT
# =============================================================================

def enforce_data_integrity(df: "pd.DataFrame") -> Tuple["pd.DataFrame", Dict[str, int]]:
    """
    Enforce data integrity by removing duplicates and negative values.
    
    Args:
        df: Input DataFrame
        
    Returns:
        Tuple of (cleaned DataFrame, removal stats dict)
    """
    import pandas as pd
    
    logger.info("=" * 60)
    logger.info("DATA INTEGRITY ENFORCEMENT")
    logger.info("=" * 60)
    
    df = df.copy()
    stats = {
        "duplicates_removed": 0,
        "negative_value_rows_removed": 0,
        "rows_before": len(df),
    }
    
    # -------------------------------------------------------------------------
    # Remove duplicate rows
    # -------------------------------------------------------------------------
    duplicate_count = df.duplicated().sum()
    
    if duplicate_count > 0:
        df = df.drop_duplicates().reset_index(drop=True)
        stats["duplicates_removed"] = duplicate_count
        logger.info(f"✓ Removed {duplicate_count} exact duplicate rows")
    else:
        logger.info("✓ No exact duplicate rows found")
    
    # -------------------------------------------------------------------------
    # Remove rows with negative values in numeric columns
    # -------------------------------------------------------------------------
    numeric_columns = ["student_size", "admission_rate", "completion_rate",
                       "admission_rate_percentage", "completion_rate_percentage"]
    
    # Only check columns that exist
    existing_numeric = [col for col in numeric_columns if col in df.columns]
    
    if existing_numeric:
        # Create mask for rows with negative values
        negative_mask = pd.Series([False] * len(df), index=df.index)
        
        for col in existing_numeric:
            col_negative = (df[col] < 0) & df[col].notna()
            negative_count = col_negative.sum()
            
            if negative_count > 0:
                logger.warning(f"⚠ Found {negative_count} negative values in '{col}'")
                sample_values = df.loc[col_negative, col].head(5).tolist()
                logger.warning(f"    Sample values: {sample_values}")
                negative_mask |= col_negative
        
        negative_row_count = negative_mask.sum()
        
        if negative_row_count > 0:
            df = pd.DataFrame(df[~negative_mask]).reset_index(drop=True)
            stats["negative_value_rows_removed"] = negative_row_count
            logger.info(f"✓ Removed {negative_row_count} rows with negative values")
        else:
            logger.info("✓ No negative values found in numeric columns")
    
    stats["rows_after"] = len(df)
    stats["total_removed"] = stats["rows_before"] - stats["rows_after"]
    
    logger.info(f"Integrity check complete: {stats['rows_before']} -> {stats['rows_after']} rows")
    
    return df, stats


# =============================================================================
# TASK 7: OUTPUT DATASET
# =============================================================================

def save_processed_data(
    df: "pd.DataFrame",
    output_dir: Path = OUTPUT_DIR,
    prefix: str = "processed",
) -> Optional[Path]:
    """
    Save the transformed dataset to a CSV file with timestamp.
    
    File format: {prefix}_YYYY_MM_DD_HHMMSS.csv
    Does NOT overwrite existing files.
    
    Args:
        df: DataFrame to save
        output_dir: Directory to save to
        prefix: Filename prefix
        
    Returns:
        Path to saved file, or None if save failed
    """
    logger.info("=" * 60)
    logger.info("SAVING PROCESSED DATA")
    logger.info("=" * 60)
    
    # Ensure output directory exists
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    
    # Generate timestamp-based filename
    timestamp = datetime.now().strftime("%Y_%m_%d_%H%M%S")
    filename = f"{prefix}_{timestamp}.csv"
    filepath = output_dir / filename
    
    # Check if file already exists (should not happen with timestamp, but be safe)
    if filepath.exists():
        logger.error(f"File already exists: {filepath}")
        # Add milliseconds to make unique
        timestamp_ms = datetime.now().strftime("%Y_%m_%d_%H%M%S_%f")
        filename = f"{prefix}_{timestamp_ms}.csv"
        filepath = output_dir / filename
        logger.info(f"Using alternative filename: {filename}")
    
    # Save to CSV
    try:
        df.to_csv(filepath, index=False, encoding="utf-8")
        file_size = filepath.stat().st_size
        file_size_kb = file_size / 1024
        
        logger.info(f"✓ Saved processed data to: {filepath}")
        logger.info(f"  Rows: {len(df)}")
        logger.info(f"  Columns: {len(df.columns)}")
        logger.info(f"  File size: {file_size_kb:.2f} KB")
        
        return filepath
        
    except Exception as e:
        logger.error(f"Failed to save processed data: {e}")
        return None


# =============================================================================
# TASK 8: MAIN TRANSFORMATION FUNCTION
# =============================================================================

def transform_data(
    df: "pd.DataFrame",
    save_output: bool = True,
    output_dir: Optional[Path] = None,
) -> "pd.DataFrame":
    """
    Transform validated College Scorecard data into analytics-ready format.
    
    This is the primary entry point for pipeline integration.
    
    Transformation pipeline:
    1. Standardize column names (lowercase, snake_case)
    2. Clean data (whitespace, state codes, nulls)
    3. Create derived metrics (percentages, categories)
    4. Enforce data integrity (duplicates, negatives)
    5. Save output to CSV (optional)
    
    Args:
        df: Validated DataFrame from validate_data()
        save_output: Whether to save the processed CSV file
        output_dir: Custom output directory (default: data/processed/)
        
    Returns:
        Transformed, analytics-ready DataFrame
        
    Example:
        >>> from scripts.validate import validate_data
        >>> from scripts.transform import transform_data
        >>> 
        >>> raw_df = fetch_scorecard_data(min_records=1000)
        >>> validated_df = validate_data(raw_df)
        >>> analytics_df = transform_data(validated_df)
    """
    import pandas as pd
    
    logger.info("=" * 60)
    logger.info("TRANSFORMATION PIPELINE - START")
    logger.info("=" * 60)
    logger.info(f"Input DataFrame: {len(df)} rows, {len(df.columns)} columns")
    logger.info(f"Input columns: {list(df.columns)}")
    
    rows_start = len(df)
    
    # Track transformation statistics (using Any for mixed value types)
    transform_stats: Dict[str, Any] = {
        "input_rows": rows_start,
        "input_columns": len(df.columns),
    }
    
    # -------------------------------------------------------------------------
    # Step 1: Standardize columns
    # -------------------------------------------------------------------------
    df = standardize_columns(df, COLUMN_RENAME_MAP)
    
    # -------------------------------------------------------------------------
    # Step 2: Clean data
    # -------------------------------------------------------------------------
    df, clean_stats = clean_data(df)
    transform_stats["cleaning"] = clean_stats
    
    # -------------------------------------------------------------------------
    # Step 3: Create derived metrics
    # -------------------------------------------------------------------------
    df = create_derived_metrics(df)
    
    # -------------------------------------------------------------------------
    # Step 4: Enforce data integrity
    # -------------------------------------------------------------------------
    df, integrity_stats = enforce_data_integrity(df)
    transform_stats["integrity"] = integrity_stats
    
    # -------------------------------------------------------------------------
    # Step 5: Save output (optional)
    # -------------------------------------------------------------------------
    saved_path = None
    if save_output:
        save_dir = output_dir if output_dir else OUTPUT_DIR
        saved_path = save_processed_data(df, output_dir=save_dir)
        transform_stats["output_file"] = str(saved_path) if saved_path else None
    
    # -------------------------------------------------------------------------
    # Final summary
    # -------------------------------------------------------------------------
    rows_end = len(df)
    transform_stats["output_rows"] = rows_end
    transform_stats["output_columns"] = len(df.columns)
    transform_stats["rows_removed"] = rows_start - rows_end
    
    logger.info("=" * 60)
    logger.info("TRANSFORMATION PIPELINE - COMPLETE")
    logger.info("=" * 60)
    logger.info(f"Input:  {rows_start} rows, {transform_stats['input_columns']} columns")
    logger.info(f"Output: {rows_end} rows, {len(df.columns)} columns")
    logger.info(f"Rows removed: {rows_start - rows_end}")
    logger.info(f"Output columns: {list(df.columns)}")
    
    if saved_path:
        logger.info(f"Saved to: {saved_path}")
    
    return df


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
    
    # Import pipeline modules
    try:
        from scripts.extract_scorecard import fetch_scorecard_data
        from scripts.validate import validate_data
    except ImportError as e:
        logger.error(f"Could not import pipeline modules: {e}")
        return 1
    
    logger.info("Running transformation pipeline demo...")
    
    # Step 1: Extract
    logger.info("\n" + "=" * 60)
    logger.info("STEP 1: EXTRACTION")
    logger.info("=" * 60)
    raw_df = fetch_scorecard_data(min_records=500, max_records=500, save_files=False)
    
    # Step 2: Validate
    logger.info("\n" + "=" * 60)
    logger.info("STEP 2: VALIDATION")
    logger.info("=" * 60)
    validated_df = validate_data(raw_df)
    
    # Step 3: Transform
    logger.info("\n" + "=" * 60)
    logger.info("STEP 3: TRANSFORMATION")
    logger.info("=" * 60)
    final_df = transform_data(validated_df, save_output=True)
    
    # Preview result
    print("\n" + "=" * 60)
    print("FINAL DATASET PREVIEW")
    print("=" * 60)
    print(f"\nColumns: {list(final_df.columns)}")
    print(f"\nSample data:")
    print(final_df.head(10).to_string())
    
    # Show derived metrics summary
    print("\n" + "=" * 60)
    print("DERIVED METRICS SUMMARY")
    print("=" * 60)
    if "size_category" in final_df.columns:
        print("\nSize Category Distribution:")
        print(final_df["size_category"].value_counts(dropna=False).to_string())
    
    if "admission_rate_percentage" in final_df.columns:
        print(f"\nAdmission Rate: {final_df['admission_rate_percentage'].mean():.2f}% avg")
    
    if "completion_rate_percentage" in final_df.columns:
        print(f"Completion Rate: {final_df['completion_rate_percentage'].mean():.2f}% avg")
    
    return 0


if __name__ == "__main__":
    sys.exit(main())
