#!/usr/bin/env python3
"""
College Scorecard Data Extraction Script

Extracts school data from the College Scorecard API with:
- Session-based requests
- 10-second timeout
- Retry logic (max 3 retries)
- Comprehensive error handling
- Pagination support (100 records/page)
"""

import json
import sys
import time
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, TYPE_CHECKING

import requests

if TYPE_CHECKING:
    import pandas as pd
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# Configuration
API_BASE_URL = "https://api.data.gov/ed/collegescorecard/v1/schools"
API_KEY = "oHByS1HOahWshBi23IkWXGpqeYyU77lYmP7XY0Qz"
PER_PAGE = 100
TIMEOUT = 10  # seconds
MAX_RETRIES = 3
MIN_RECORDS = 1000  # Fetch at least this many records

# Fields to extract (required + additional useful fields)
FIELDS = [
    "id",
    "school.name",
    "school.state",
    "school.city",
    "school.zip",
    "school.school_url",
    "school.ownership",
    "school.region_id",
    "school.locale",
    "school.operating",
    "latest.student.size",
    "latest.student.grad_students",
    "latest.admissions.admission_rate.overall",
    "latest.completion.rate_suppressed.overall",
    "latest.completion.rate_suppressed.four_year",
    "latest.cost.tuition.in_state",
    "latest.cost.tuition.out_of_state",
    "latest.aid.median_debt.completers.overall",
    "latest.earnings.10_yrs_after_entry.median",
    "location.lat",
    "location.lon",
]


class ExtractionError(Exception):
    """Custom exception for extraction errors."""
    pass


# Column name mapping: API field -> snake_case name
COLUMN_NAME_MAP = {
    "id": "school_id",
    "school.name": "school_name",
    "school.state": "school_state",
    "school.city": "school_city",
    "school.zip": "school_zip",
    "school.school_url": "school_url",
    "school.ownership": "ownership",
    "school.region_id": "region_id",
    "school.locale": "locale",
    "school.operating": "operating",
    "latest.student.size": "student_size",
    "latest.student.grad_students": "grad_students",
    "latest.admissions.admission_rate.overall": "admission_rate_overall",
    "latest.completion.rate_suppressed.overall": "completion_rate_overall",
    "latest.completion.rate_suppressed.four_year": "completion_rate_4yr",
    "latest.cost.tuition.in_state": "tuition_in_state",
    "latest.cost.tuition.out_of_state": "tuition_out_of_state",
    "latest.aid.median_debt.completers.overall": "median_debt",
    "latest.earnings.10_yrs_after_entry.median": "earnings_10yr_median",
    "location.lat": "latitude",
    "location.lon": "longitude",
}

# Numeric column types
INTEGER_COLUMNS = [
    "school_id", "student_size", "grad_students", "ownership",
    "region_id", "locale", "operating", "tuition_in_state",
    "tuition_out_of_state", "median_debt", "earnings_10yr_median",
]

FLOAT_COLUMNS = [
    "admission_rate_overall", "completion_rate_overall",
    "completion_rate_4yr", "latitude", "longitude",
]


def create_session() -> requests.Session:
    """
    Create a requests session with retry logic.
    
    Returns:
        Configured requests.Session with retry adapter
    """
    session = requests.Session()
    
    # Configure retry strategy
    retry_strategy = Retry(
        total=MAX_RETRIES,
        backoff_factor=1,  # Wait 1, 2, 4 seconds between retries
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET"],
    )
    
    adapter = HTTPAdapter(max_retries=retry_strategy)
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    
    return session


def fetch_page(
    session: requests.Session,
    page: int,
    fields: List[str],
) -> Dict[str, Any]:
    """
    Fetch a single page of results from the API.
    
    Args:
        session: requests.Session object
        page: Page number (0-indexed)
        fields: List of fields to fetch
        
    Returns:
        API response as dictionary
        
    Raises:
        ExtractionError: On HTTP errors, empty responses, or JSON parsing errors
    """
    params = {
        "api_key": API_KEY,
        "page": page,
        "per_page": PER_PAGE,
        "fields": ",".join(fields),
        "school.operating": 1,  # Only currently operating schools
    }
    
    try:
        response = session.get(
            API_BASE_URL,
            params=params,
            timeout=TIMEOUT,
        )
        
        # Handle HTTP errors
        response.raise_for_status()
        
    except requests.exceptions.Timeout:
        raise ExtractionError(f"Request timed out after {TIMEOUT} seconds (page {page})")
    except requests.exceptions.ConnectionError as e:
        raise ExtractionError(f"Connection error on page {page}: {e}")
    except requests.exceptions.HTTPError as e:
        raise ExtractionError(f"HTTP error on page {page}: {e}")
    except requests.exceptions.RequestException as e:
        raise ExtractionError(f"Request failed on page {page}: {e}")
    
    # Parse JSON response
    try:
        data = response.json()
    except json.JSONDecodeError as e:
        raise ExtractionError(f"JSON parsing error on page {page}: {e}")
    
    # Validate response structure
    if not isinstance(data, dict):
        raise ExtractionError(f"Unexpected response format on page {page}: expected dict, got {type(data)}")
    
    if "results" not in data:
        raise ExtractionError(f"Missing 'results' key in API response on page {page}")
    
    # Handle empty responses
    if data["results"] is None:
        data["results"] = []
    
    return data


def extract_all_records(
    min_records: int = MIN_RECORDS,
    max_records: Optional[int] = None,
) -> List[Dict[str, Any]]:
    """
    Extract records from the API with pagination.
    
    Args:
        min_records: Minimum number of records to fetch
        max_records: Maximum number of records (None for no limit)
        
    Returns:
        List of all extracted records
    """
    session = create_session()
    all_records: List[Dict[str, Any]] = []
    page = 0
    total_available = 0
    
    print(f"\n{'='*60}")
    print("COLLEGE SCORECARD DATA EXTRACTION")
    print(f"{'='*60}")
    print(f"Start time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"Target: At least {min_records} records")
    print(f"Per page: {PER_PAGE}")
    print(f"Timeout: {TIMEOUT}s")
    print(f"Max retries: {MAX_RETRIES}")
    print(f"{'='*60}\n")
    
    while True:
        try:
            print(f"Fetching page {page + 1}...", end=" ", flush=True)
            start_time = time.time()
            
            data = fetch_page(session, page, FIELDS)
            
            elapsed = time.time() - start_time
            records = data.get("results", [])
            metadata = data.get("metadata", {})
            total_available = metadata.get("total", 0)
            
            print(f"✓ {len(records)} records ({elapsed:.2f}s)")
            
            # Stop if no more data
            if not records:
                print("\n→ No more records available.")
                break
            
            all_records.extend(records)
            
            # Progress update
            print(f"  Progress: {len(all_records)}/{total_available} "
                  f"({len(all_records)/total_available*100:.1f}%)")
            
            # Check if we've reached our target
            if max_records and len(all_records) >= max_records:
                print(f"\n→ Reached max records limit ({max_records}).")
                break
            
            # Check if we have enough records
            if len(all_records) >= min_records:
                # Continue until we don't have more or hit a page boundary
                if len(records) < PER_PAGE:
                    print("\n→ Reached end of results.")
                    break
            
            # Check if we've fetched all available records
            if len(all_records) >= total_available:
                print("\n→ Fetched all available records.")
                break
            
            page += 1
            
            # Rate limiting - be nice to the API
            time.sleep(0.1)
            
        except ExtractionError as e:
            print(f"\n✗ Error: {e}")
            if len(all_records) >= min_records:
                print(f"  Continuing with {len(all_records)} records collected so far.")
                break
            else:
                raise
    
    return all_records


def normalize_column_name(col: str) -> str:
    """
    Convert API field name to snake_case.
    
    Args:
        col: Original column name (e.g., 'school.name')
        
    Returns:
        Snake case column name (e.g., 'school_name')
    """
    # Use mapping if available
    if col in COLUMN_NAME_MAP:
        return COLUMN_NAME_MAP[col]
    
    # Fallback: replace dots and convert to snake_case
    return col.replace(".", "_").replace("-", "_").lower()


def process_dataframe(records: List[Dict[str, Any]]) -> "pd.DataFrame":
    """
    Convert records to DataFrame with proper column names and types.
    
    Args:
        records: List of raw API records
        
    Returns:
        Processed pandas DataFrame
    """
    import pandas as pd
    
    # Create DataFrame
    df = pd.DataFrame(records)
    
    # Normalize column names to snake_case
    df.columns = [normalize_column_name(col) for col in df.columns]
    
    # Apply integer types (nullable Int64 to handle NaN)
    for col in INTEGER_COLUMNS:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").astype("Int64")  # type: ignore[union-attr]
    
    # Apply float types
    for col in FLOAT_COLUMNS:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").astype("float64")  # type: ignore[union-attr]
    
    # String columns - ensure proper type without dropping data
    string_columns = ["school_name", "school_state", "school_city", "school_zip", "school_url"]
    for col in string_columns:
        if col in df.columns:
            df[col] = df[col].astype("string")
    
    # Add metadata columns
    df["_extracted_at"] = datetime.utcnow()
    df["_source"] = "college_scorecard_api"
    
    return df


def get_unique_filepath(base_path: Path) -> Path:
    """
    Get a unique filepath, appending a counter if file exists.
    
    Args:
        base_path: Desired file path
        
    Returns:
        Unique file path that doesn't exist
    """
    if not base_path.exists():
        return base_path
    
    # Add counter suffix
    stem = base_path.stem
    suffix = base_path.suffix
    parent = base_path.parent
    
    counter = 1
    while True:
        new_path = parent / f"{stem}_{counter}{suffix}"
        if not new_path.exists():
            return new_path
        counter += 1


def save_raw_json(
    records: List[Dict[str, Any]],
    raw_dir: Path,
) -> Path:
    """
    Save raw JSON to data/raw directory.
    
    Format: scorecard_YYYY_MM_DD_HHMMSS.json
    Does NOT overwrite existing files.
    
    Args:
        records: Raw API records
        raw_dir: Path to data/raw directory
        
    Returns:
        Path to saved file
    """
    raw_dir.mkdir(parents=True, exist_ok=True)
    
    # Format: scorecard_YYYY_MM_DD_HHMMSS.json
    timestamp = datetime.now().strftime("%Y_%m_%d_%H%M%S")
    base_path = raw_dir / f"scorecard_{timestamp}.json"
    
    # Ensure we don't overwrite
    filepath = get_unique_filepath(base_path)
    
    with open(filepath, "w") as f:
        json.dump(records, f, indent=2)
    
    return filepath


def save_processed_data(
    df: "pd.DataFrame",
    staging_dir: Path,
) -> Dict[str, Path]:
    """
    Save processed DataFrame to staging directory.
    
    Args:
        df: Processed DataFrame
        staging_dir: Path to staging directory
        
    Returns:
        Dictionary of format -> filepath
    """
    staging_dir.mkdir(parents=True, exist_ok=True)
    timestamp = datetime.now().strftime("%Y_%m_%d_%H%M%S")
    saved_files = {}
    
    # Save as Parquet
    parquet_path = staging_dir / f"scorecard_{timestamp}.parquet"
    parquet_path = get_unique_filepath(parquet_path)
    df.to_parquet(parquet_path, index=False)
    saved_files["parquet"] = parquet_path
    
    # Save as CSV for easy inspection
    csv_path = staging_dir / f"scorecard_{timestamp}.csv"
    csv_path = get_unique_filepath(csv_path)
    df.to_csv(csv_path, index=False)
    saved_files["csv"] = csv_path
    
    return saved_files


def save_results(
    records: List[Dict[str, Any]],
    output_dir: Path,
) -> Dict[str, Path]:
    """
    Save extracted records to files.
    
    Args:
        records: List of records to save
        output_dir: Base output directory (parent of raw/staging)
        
    Returns:
        Dictionary of format -> filepath
    """
    saved_files = {}
    base_dir = output_dir.parent  # Go up from staging to data/
    
    # Step 6: Save raw JSON to data/raw
    raw_dir = base_dir / "raw"
    raw_path = save_raw_json(records, raw_dir)
    saved_files["raw_json"] = raw_path
    print(f"  Raw JSON: {raw_path}")
    
    # Step 5: Process to DataFrame and save
    try:
        import pandas as pd
        
        print("\nProcessing data...")
        df = process_dataframe(records)
        
        print(f"  Columns normalized to snake_case: {len(df.columns)}")
        print(f"  Integer columns: {sum(1 for c in INTEGER_COLUMNS if c in df.columns)}")
        print(f"  Float columns: {sum(1 for c in FLOAT_COLUMNS if c in df.columns)}")
        print(f"  Total rows: {len(df)} (no data dropped)")
        
        # Save processed data to staging
        staging_dir = base_dir / "staging"
        staging_files = save_processed_data(df, staging_dir)
        saved_files.update(staging_files)
        
        print(f"\nProcessed files:")
        print(f"  Parquet: {staging_files['parquet']}")
        print(f"  CSV: {staging_files['csv']}")
        
    except ImportError:
        print("  (pandas not available - skipping processed data)")
    
    return saved_files


def print_summary(records: List[Dict[str, Any]]) -> None:
    """Print extraction summary statistics."""
    print(f"\n{'='*60}")
    print("EXTRACTION SUMMARY")
    print(f"{'='*60}")
    print(f"Total records: {len(records)}")
    
    if not records:
        return
    
    # Count non-null values for key fields (using original API field names)
    key_fields = [
        ("school.name", "school_name", "School Name"),
        ("school.state", "school_state", "State"),
        ("latest.student.size", "student_size", "Student Size"),
        ("latest.admissions.admission_rate.overall", "admission_rate_overall", "Admission Rate"),
        ("latest.completion.rate_suppressed.overall", "completion_rate_overall", "Completion Rate"),
    ]
    
    print("\nField completeness:")
    for api_field, snake_field, display_name in key_fields:
        non_null = sum(1 for r in records if r.get(api_field) is not None)
        pct = non_null / len(records) * 100
        bar = "█" * int(pct / 5) + "░" * (20 - int(pct / 5))
        print(f"  {display_name:<20} {bar} {pct:5.1f}% ({non_null}/{len(records)})")
    
    # State distribution
    states: Dict[str, int] = {}
    for r in records:
        state = r.get("school.state", "Unknown")
        if state:
            states[state] = states.get(state, 0) + 1
    
    print(f"\nStates covered: {len(states)}")
    top_states = sorted(states.items(), key=lambda x: x[1], reverse=True)[:5]
    print("Top 5 states:")
    for state, count in top_states:
        print(f"  {state}: {count} schools")


def main() -> int:
    """Main entry point."""
    try:
        # Extract records
        records = extract_all_records(
            min_records=MIN_RECORDS,
            max_records=None,  # No limit - fetch all
        )
        
        if not records:
            print("No records extracted!")
            return 1
        
        # Print summary
        print_summary(records)
        
        # Save results
        output_dir = Path(__file__).parent.parent / "data" / "staging"
        print(f"\nSaving to: {output_dir}")
        saved_files = save_results(records, output_dir)
        
        print(f"\n{'='*60}")
        print("EXTRACTION COMPLETE")
        print(f"{'='*60}")
        print(f"End time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"Records extracted: {len(records)}")
        print(f"Files saved: {len(saved_files)}")
        
        return 0
        
    except ExtractionError as e:
        print(f"\n{'='*60}")
        print(f"EXTRACTION FAILED: {e}")
        print(f"{'='*60}")
        return 1
    except KeyboardInterrupt:
        print("\n\nExtraction cancelled by user.")
        return 130


if __name__ == "__main__":
    sys.exit(main())
