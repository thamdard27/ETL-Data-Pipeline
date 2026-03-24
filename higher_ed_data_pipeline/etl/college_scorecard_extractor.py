"""
College Scorecard Data Extractor
================================

Production-grade data extraction module for the College Scorecard API.
Handles extraction, normalization, and validation of higher education data.

Features:
- Nested field flattening
- Data type coercion
- Null value handling
- Schema validation
- Progress tracking

Author: Data Engineering Team
"""

from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple, Union

import pandas as pd
from loguru import logger

from higher_ed_data_pipeline.config.settings import Settings, get_settings
from higher_ed_data_pipeline.etl.college_scorecard_client import (
    CollegeScorecardClient,
    APIConfig,
)


class CollegeScorecardExtractor:
    """
    Extractor for College Scorecard data.
    
    Handles the complete extraction workflow:
    1. Fetch data from API with pagination
    2. Flatten nested JSON structures
    3. Apply data type conversions
    4. Validate extracted data
    5. Save to staging area
    
    Usage:
        extractor = CollegeScorecardExtractor(api_key="your_key")
        
        # Extract all schools
        df = extractor.extract_all()
        
        # Extract with filters
        df = extractor.extract_by_state("CA")
        
        # Extract core fields only
        df = extractor.extract_core_fields()
    """
    
    # Core fields as specified in requirements with their API field names
    # API returns FLAT keys (e.g., "school.name") when using fields parameter
    CORE_FIELD_MAPPING = {
        # Primary identifiers
        "school_id": "id",
        
        # School information
        "school_name": "school.name",
        "school_state": "school.state",
        "school_city": "school.city",
        "school_zip": "school.zip",
        "school_url": "school.school_url",
        "ownership": "school.ownership",
        "region_id": "school.region_id",
        "locale": "school.locale",
        "carnegie_basic": "school.carnegie_basic",
        "predominant_degree": "school.degrees_awarded.predominant",
        "highest_degree": "school.degrees_awarded.highest",
        
        # Student information
        "student_size": "latest.student.size",
        "grad_students": "latest.student.grad_students",
        "demographics_men": "latest.student.demographics.men",
        "demographics_women": "latest.student.demographics.women",
        "demographics_white": "latest.student.demographics.race_ethnicity.white",
        "demographics_black": "latest.student.demographics.race_ethnicity.black",
        "demographics_hispanic": "latest.student.demographics.race_ethnicity.hispanic",
        "demographics_asian": "latest.student.demographics.race_ethnicity.asian",
        
        # Admissions
        "admission_rate_overall": "latest.admissions.admission_rate.overall",
        "sat_average": "latest.admissions.sat_scores.average.overall",
        "act_midpoint": "latest.admissions.act_scores.midpoint.cumulative",
        
        # Completion
        "completion_rate_overall": "latest.completion.rate_suppressed.overall",
        "completion_rate_4yr": "latest.completion.rate_suppressed.four_year",
        
        # Cost
        "tuition_in_state": "latest.cost.tuition.in_state",
        "tuition_out_of_state": "latest.cost.tuition.out_of_state",
        
        # Aid & Earnings
        "median_debt": "latest.aid.median_debt.completers.overall",
        "earnings_10yr_median": "latest.earnings.10_yrs_after_entry.median",
        
        # Location
        "latitude": "location.lat",
        "longitude": "location.lon",
    }
    
    # Ownership code mapping
    OWNERSHIP_MAP = {
        1: "Public",
        2: "Private nonprofit",
        3: "Private for-profit",
    }
    
    # Region mapping
    REGION_MAP = {
        0: "U.S. Service Schools",
        1: "New England",
        2: "Mid East",
        3: "Great Lakes",
        4: "Plains",
        5: "Southeast",
        6: "Southwest",
        7: "Rocky Mountains",
        8: "Far West",
        9: "Outlying Areas",
    }
    
    def __init__(
        self,
        api_key: str,
        settings: Optional[Settings] = None,
        config: Optional[APIConfig] = None,
    ) -> None:
        """
        Initialize the extractor.
        
        Args:
            api_key: College Scorecard API key
            settings: Application settings
            config: API configuration
        """
        self.settings = settings or get_settings()
        self.client = CollegeScorecardClient(
            api_key=api_key,
            config=config,
            settings=self.settings,
        )
        
        logger.info("Initialized CollegeScorecardExtractor")
    
    def _get_nested_value(
        self,
        data: Dict[str, Any],
        path: Union[Tuple, str],
    ) -> Any:
        """
        Safely extract a nested value from a dictionary.
        
        Args:
            data: Source dictionary
            path: Tuple of keys or single key string
            
        Returns:
            Value at path or None if not found
        """
        if isinstance(path, str):
            return data.get(path)
        
        current = data
        for key in path:
            if isinstance(current, dict) and key in current:
                current = current[key]
            else:
                return None
        return current
    
    def _flatten_record(
        self,
        record: Dict[str, Any],
        field_mapping: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        """
        Flatten a nested API record into a flat dictionary.
        
        Args:
            record: Raw API record
            field_mapping: Field name to path mapping
            
        Returns:
            dict: Flattened record
        """
        mapping = field_mapping or self.CORE_FIELD_MAPPING
        flattened = {}
        
        for field_name, path in mapping.items():
            value = self._get_nested_value(record, path)
            flattened[field_name] = value
        
        return flattened
    
    def _process_records(
        self,
        records: List[Dict[str, Any]],
        field_mapping: Optional[Dict[str, Any]] = None,
    ) -> pd.DataFrame:
        """
        Process raw API records into a DataFrame.
        
        Args:
            records: List of raw API records
            field_mapping: Optional custom field mapping
            
        Returns:
            pd.DataFrame: Processed DataFrame
        """
        flattened = [
            self._flatten_record(record, field_mapping)
            for record in records
        ]
        
        df = pd.DataFrame(flattened)
        
        # Add metadata
        df["_extracted_at"] = datetime.utcnow()
        df["_source"] = "college_scorecard_api"
        
        return df
    
    def _apply_type_conversions(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Apply appropriate data type conversions.
        
        Args:
            df: Input DataFrame
            
        Returns:
            pd.DataFrame: DataFrame with corrected types
        """
        # Integer columns
        int_cols = [
            "school_id", "student_size", "grad_students",
            "tuition_in_state", "tuition_out_of_state",
            "sat_average", "act_midpoint", "ownership",
            "region_id", "locale", "carnegie_basic",
            "predominant_degree", "highest_degree",
        ]
        
        for col in int_cols:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors="coerce").astype("Int64")  # type: ignore[union-attr]
        
        # Float columns
        float_cols = [
            "admission_rate_overall", "completion_rate_overall",
            "completion_rate_4yr", "demographics_men", "demographics_women",
            "demographics_white", "demographics_black",
            "demographics_hispanic", "demographics_asian",
            "median_debt", "earnings_10yr_median",
            "latitude", "longitude",
        ]
        
        for col in float_cols:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors="coerce")
        
        # String columns
        str_cols = [
            "school_name", "school_state", "school_city",
            "school_zip", "school_url",
        ]
        
        for col in str_cols:
            if col in df.columns:
                df[col] = df[col].astype(str).replace("None", pd.NA)
        
        return df
    
    def _add_derived_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Add derived/lookup columns.
        
        Args:
            df: Input DataFrame
            
        Returns:
            pd.DataFrame: DataFrame with derived columns
        """
        # Ownership label
        if "ownership" in df.columns:
            df["ownership_label"] = df["ownership"].map(self.OWNERSHIP_MAP)
        
        # Region label
        if "region_id" in df.columns:
            df["region_label"] = df["region_id"].map(self.REGION_MAP)
        
        # Derived metrics
        if "student_size" in df.columns and "grad_students" in df.columns:
            df["undergrad_size"] = df["student_size"] - df["grad_students"].fillna(0)
        
        if "tuition_in_state" in df.columns and "tuition_out_of_state" in df.columns:
            df["tuition_difference"] = (
                df["tuition_out_of_state"].fillna(0) - 
                df["tuition_in_state"].fillna(0)
            )
        
        return df
    
    def extract_core_fields(
        self,
        state: Optional[str] = None,
        max_pages: Optional[int] = None,
        save_to_staging: bool = True,
    ) -> pd.DataFrame:
        """
        Extract core institution fields from the API.
        
        This method extracts the verified fields:
        - school.name, school.state
        - latest.student.size
        - latest.admissions.admission_rate.overall
        - latest.completion.rate_suppressed.overall
        
        Plus additional useful fields for analysis.
        
        Args:
            state: Optional state filter (e.g., "CA")
            max_pages: Maximum pages to fetch
            save_to_staging: Save to staging directory
            
        Returns:
            pd.DataFrame: Extracted and processed data
        """
        logger.info(f"Starting extraction (state={state}, max_pages={max_pages})")
        
        # Validate connection
        if not self.client.validate_connection():
            raise ConnectionError("Failed to validate API connection")
        
        # Get total count
        total = self.client.get_total_count(state=state)
        logger.info(f"Total records to extract: {total}")
        
        # Build field list for API
        api_fields = [
            "id",
            "school.name",
            "school.state",
            "school.city",
            "school.zip",
            "school.school_url",
            "school.ownership",
            "school.region_id",
            "school.locale",
            "school.carnegie_basic",
            "school.degrees_awarded.predominant",
            "school.degrees_awarded.highest",
            "latest.student.size",
            "latest.student.grad_students",
            "latest.student.demographics.men",
            "latest.student.demographics.women",
            "latest.student.demographics.race_ethnicity.white",
            "latest.student.demographics.race_ethnicity.black",
            "latest.student.demographics.race_ethnicity.hispanic",
            "latest.student.demographics.race_ethnicity.asian",
            "latest.admissions.admission_rate.overall",
            "latest.admissions.sat_scores.average.overall",
            "latest.admissions.act_scores.midpoint.cumulative",
            "latest.completion.rate_suppressed.overall",
            "latest.completion.rate_suppressed.four_year",
            "latest.cost.tuition.in_state",
            "latest.cost.tuition.out_of_state",
            "latest.aid.median_debt.completers.overall",
            "latest.earnings.10_yrs_after_entry.median",
            "location.lat",
            "location.lon",
        ]
        
        # Fetch all data
        all_records = []
        for batch in self.client.fetch_schools_paginated(
            fields=api_fields,
            state=state,
            max_pages=max_pages,
        ):
            all_records.extend(batch)
        
        logger.info(f"Fetched {len(all_records)} total records")
        
        # Process records
        df = self._process_records(all_records)
        df = self._apply_type_conversions(df)
        df = self._add_derived_columns(df)
        
        # Reorder columns
        priority_cols = [
            "school_id", "school_name", "school_state", "school_city",
            "student_size", "admission_rate_overall", "completion_rate_overall",
        ]
        other_cols = [c for c in df.columns if c not in priority_cols]
        df = pd.DataFrame(df[priority_cols + other_cols])
        
        logger.info(
            f"Extraction complete: {len(df)} rows, {len(df.columns)} columns"
        )
        
        # Save to staging
        if save_to_staging:
            self._save_to_staging(df, state)
        
        return df
    
    def extract_all(
        self,
        save_to_staging: bool = True,
    ) -> pd.DataFrame:
        """
        Extract all schools from the API.
        
        Args:
            save_to_staging: Save to staging directory
            
        Returns:
            pd.DataFrame: All school data
        """
        return self.extract_core_fields(
            state=None,
            max_pages=None,
            save_to_staging=save_to_staging,
        )
    
    def extract_by_state(
        self,
        state: str,
        save_to_staging: bool = True,
    ) -> pd.DataFrame:
        """
        Extract schools for a specific state.
        
        Args:
            state: State abbreviation (e.g., "CA", "NY")
            save_to_staging: Save to staging directory
            
        Returns:
            pd.DataFrame: School data for the state
        """
        return self.extract_core_fields(
            state=state,
            save_to_staging=save_to_staging,
        )
    
    def extract_sample(
        self,
        n_pages: int = 2,
        save_to_staging: bool = False,
    ) -> pd.DataFrame:
        """
        Extract a sample of schools for testing.
        
        Args:
            n_pages: Number of pages to fetch
            save_to_staging: Save to staging directory
            
        Returns:
            pd.DataFrame: Sample school data
        """
        return self.extract_core_fields(
            max_pages=n_pages,
            save_to_staging=save_to_staging,
        )
    
    def _save_to_staging(
        self,
        df: pd.DataFrame,
        state: Optional[str] = None,
    ) -> Path:
        """
        Save extracted data to staging directory.
        
        Args:
            df: DataFrame to save
            state: State filter used (for filename)
            
        Returns:
            Path: Path to saved file
        """
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        state_suffix = f"_{state}" if state else ""
        filename = f"college_scorecard{state_suffix}_{timestamp}.parquet"
        
        output_path = self.settings.data_staging_path / filename
        output_path.parent.mkdir(parents=True, exist_ok=True)
        
        df.to_parquet(output_path, index=False)
        logger.info(f"Saved to staging: {output_path}")
        
        # Also save metadata
        metadata = {
            "extracted_at": timestamp,
            "rows": len(df),
            "columns": list(df.columns),
            "state_filter": state,
            "source": "college_scorecard_api",
        }
        
        metadata_path = output_path.with_suffix(".json")
        import json
        with open(metadata_path, "w") as f:
            json.dump(metadata, f, indent=2)
        
        return output_path
    
    def get_extraction_stats(self, df: pd.DataFrame) -> Dict[str, Any]:
        """
        Generate statistics about the extracted data.
        
        Args:
            df: Extracted DataFrame
            
        Returns:
            dict: Statistics summary
        """
        stats = {
            "total_records": len(df),
            "columns": len(df.columns),
            "states_covered": df["school_state"].nunique() if "school_state" in df.columns else 0,
            "missing_values": df.isnull().sum().to_dict(),
            "completeness": {
                col: (1 - df[col].isnull().sum() / len(df)) * 100
                for col in df.columns
            },
        }
        
        # Key metrics
        if "student_size" in df.columns:
            stats["total_students"] = df["student_size"].sum()
            stats["avg_student_size"] = df["student_size"].mean()
        
        if "admission_rate_overall" in df.columns:
            stats["avg_admission_rate"] = df["admission_rate_overall"].mean()
        
        if "completion_rate_overall" in df.columns:
            stats["avg_completion_rate"] = df["completion_rate_overall"].mean()
        
        return stats
