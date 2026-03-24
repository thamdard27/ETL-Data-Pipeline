#!/usr/bin/env python3
"""
College Scorecard ETL Pipeline Orchestrator
============================================

Production-grade pipeline runner that orchestrates all ETL steps:
1. Extract - Fetch data from College Scorecard API
2. Validate - Validate and clean extracted data
3. Transform - Transform data into analytics-ready format
4. Load - Load transformed data into SQL Server

Usage:
    python main.py

Features:
- Centralized orchestration
- Proper error handling with pipeline halt on failure
- Comprehensive logging at each stage
- Execution summary with record counts

Author: ETL Pipeline Team
Date: 2026-03-24
"""

import logging
import sys
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Optional

# Add scripts directory to path for imports
sys.path.insert(0, str(Path(__file__).parent))

# Configure logging BEFORE imports to ensure consistent format
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    handlers=[
        logging.StreamHandler(sys.stdout),
    ]
)

# Create pipeline-specific logger
logger = logging.getLogger("pipeline.orchestrator")


# =============================================================================
# PIPELINE RESULT TRACKING
# =============================================================================

@dataclass
class StageResult:
    """Result of a single pipeline stage."""
    stage_name: str
    status: str  # "success", "failed", "skipped"
    records_in: int = 0
    records_out: int = 0
    duration_seconds: float = 0.0
    error_message: Optional[str] = None
    details: Dict[str, Any] = field(default_factory=dict)


@dataclass
class PipelineResult:
    """Complete pipeline execution result."""
    status: str = "pending"  # "success", "failed"
    stages: list = field(default_factory=list)
    started_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None
    total_records_extracted: int = 0
    total_records_loaded: int = 0
    error_stage: Optional[str] = None
    error_message: Optional[str] = None
    
    @property
    def duration_seconds(self) -> float:
        if self.started_at and self.completed_at:
            return (self.completed_at - self.started_at).total_seconds()
        return 0.0
    
    def add_stage(self, result: StageResult) -> None:
        self.stages.append(result)
        if result.status == "failed":
            self.status = "failed"
            self.error_stage = result.stage_name
            self.error_message = result.error_message


# =============================================================================
# PIPELINE STAGES
# =============================================================================

def run_extract_stage(min_records: int = 1000) -> tuple:
    """
    Execute the extraction stage.
    
    Args:
        min_records: Minimum records to extract
        
    Returns:
        Tuple of (DataFrame or None, StageResult)
    """
    from scripts.extract_scorecard import fetch_scorecard_data, ExtractionError
    
    stage_name = "EXTRACT"
    start_time = datetime.now()
    
    logger.info("=" * 70)
    logger.info(f"STAGE 1: {stage_name} - Starting")
    logger.info("=" * 70)
    logger.info(f"Target: Fetch at least {min_records} records from College Scorecard API")
    
    try:
        df = fetch_scorecard_data(
            min_records=min_records,
            save_files=True,
        )
        
        duration = (datetime.now() - start_time).total_seconds()
        records_out = len(df) if df is not None else 0
        
        logger.info(f"✓ {stage_name} completed: {records_out} records extracted")
        
        return df, StageResult(
            stage_name=stage_name,
            status="success",
            records_in=0,
            records_out=records_out,
            duration_seconds=duration,
        )
        
    except ExtractionError as e:
        duration = (datetime.now() - start_time).total_seconds()
        logger.error(f"✗ {stage_name} FAILED: {e}")
        
        return None, StageResult(
            stage_name=stage_name,
            status="failed",
            duration_seconds=duration,
            error_message=str(e),
        )
    except Exception as e:
        duration = (datetime.now() - start_time).total_seconds()
        logger.error(f"✗ {stage_name} FAILED with unexpected error: {e}")
        
        return None, StageResult(
            stage_name=stage_name,
            status="failed",
            duration_seconds=duration,
            error_message=f"Unexpected error: {e}",
        )


def run_validate_stage(df) -> tuple:
    """
    Execute the validation stage.
    
    Args:
        df: DataFrame from extraction stage
        
    Returns:
        Tuple of (validated DataFrame or None, StageResult)
    """
    from scripts.validate import validate_data, SchemaValidationError
    
    stage_name = "VALIDATE"
    start_time = datetime.now()
    records_in = len(df) if df is not None else 0
    
    logger.info("")
    logger.info("=" * 70)
    logger.info(f"STAGE 2: {stage_name} - Starting")
    logger.info("=" * 70)
    logger.info(f"Input: {records_in} records to validate")
    
    if df is None or len(df) == 0:
        logger.error(f"✗ {stage_name} FAILED: No data to validate")
        return None, StageResult(
            stage_name=stage_name,
            status="failed",
            records_in=0,
            error_message="No data provided for validation",
        )
    
    try:
        validated_df = validate_data(df)
        
        duration = (datetime.now() - start_time).total_seconds()
        records_out = len(validated_df) if validated_df is not None else 0
        removed = records_in - records_out
        
        logger.info(f"✓ {stage_name} completed: {records_out} records validated")
        logger.info(f"  Removed: {removed} invalid records ({removed/records_in*100:.1f}%)")
        
        return validated_df, StageResult(
            stage_name=stage_name,
            status="success",
            records_in=records_in,
            records_out=records_out,
            duration_seconds=duration,
            details={"records_removed": removed},
        )
        
    except SchemaValidationError as e:
        duration = (datetime.now() - start_time).total_seconds()
        logger.error(f"✗ {stage_name} FAILED - Schema error: {e}")
        
        return None, StageResult(
            stage_name=stage_name,
            status="failed",
            records_in=records_in,
            duration_seconds=duration,
            error_message=f"Schema validation error: {e}",
        )
    except Exception as e:
        duration = (datetime.now() - start_time).total_seconds()
        logger.error(f"✗ {stage_name} FAILED with unexpected error: {e}")
        
        return None, StageResult(
            stage_name=stage_name,
            status="failed",
            records_in=records_in,
            duration_seconds=duration,
            error_message=f"Unexpected error: {e}",
        )


def run_transform_stage(df) -> tuple:
    """
    Execute the transformation stage.
    
    Args:
        df: Validated DataFrame
        
    Returns:
        Tuple of (transformed DataFrame or None, StageResult)
    """
    from scripts.transform import transform_data
    
    stage_name = "TRANSFORM"
    start_time = datetime.now()
    records_in = len(df) if df is not None else 0
    
    logger.info("")
    logger.info("=" * 70)
    logger.info(f"STAGE 3: {stage_name} - Starting")
    logger.info("=" * 70)
    logger.info(f"Input: {records_in} records to transform")
    
    if df is None or len(df) == 0:
        logger.error(f"✗ {stage_name} FAILED: No data to transform")
        return None, StageResult(
            stage_name=stage_name,
            status="failed",
            records_in=0,
            error_message="No data provided for transformation",
        )
    
    try:
        transformed_df = transform_data(df, save_output=True)
        
        duration = (datetime.now() - start_time).total_seconds()
        records_out = len(transformed_df) if transformed_df is not None else 0
        
        logger.info(f"✓ {stage_name} completed: {records_out} records transformed")
        
        return transformed_df, StageResult(
            stage_name=stage_name,
            status="success",
            records_in=records_in,
            records_out=records_out,
            duration_seconds=duration,
        )
        
    except Exception as e:
        duration = (datetime.now() - start_time).total_seconds()
        logger.error(f"✗ {stage_name} FAILED with error: {e}")
        
        return None, StageResult(
            stage_name=stage_name,
            status="failed",
            records_in=records_in,
            duration_seconds=duration,
            error_message=str(e),
        )


def run_load_stage(df) -> tuple:
    """
    Execute the loading stage.
    
    Args:
        df: Transformed DataFrame
        
    Returns:
        Tuple of (load stats dict or None, StageResult)
    """
    from scripts.load import load_data, LoadError
    
    stage_name = "LOAD"
    start_time = datetime.now()
    records_in = len(df) if df is not None else 0
    
    logger.info("")
    logger.info("=" * 70)
    logger.info(f"STAGE 4: {stage_name} - Starting")
    logger.info("=" * 70)
    logger.info(f"Input: {records_in} records to load into SQL Server")
    
    if df is None or len(df) == 0:
        logger.error(f"✗ {stage_name} FAILED: No data to load")
        return None, StageResult(
            stage_name=stage_name,
            status="failed",
            records_in=0,
            error_message="No data provided for loading",
        )
    
    try:
        load_stats = load_data(df)
        
        duration = (datetime.now() - start_time).total_seconds()
        records_out = load_stats.get("rows_inserted", 0)
        
        logger.info(f"✓ {stage_name} completed: {records_out} records loaded to database")
        
        return load_stats, StageResult(
            stage_name=stage_name,
            status="success",
            records_in=records_in,
            records_out=records_out,
            duration_seconds=duration,
            details=load_stats,
        )
        
    except LoadError as e:
        duration = (datetime.now() - start_time).total_seconds()
        logger.error(f"✗ {stage_name} FAILED - Load error: {e}")
        
        return None, StageResult(
            stage_name=stage_name,
            status="failed",
            records_in=records_in,
            duration_seconds=duration,
            error_message=str(e),
        )
    except Exception as e:
        duration = (datetime.now() - start_time).total_seconds()
        logger.error(f"✗ {stage_name} FAILED with unexpected error: {e}")
        
        return None, StageResult(
            stage_name=stage_name,
            status="failed",
            records_in=records_in,
            duration_seconds=duration,
            error_message=f"Unexpected error: {e}",
        )


# =============================================================================
# PIPELINE ORCHESTRATOR
# =============================================================================

def run_pipeline(min_records: int = 1000) -> PipelineResult:
    """
    Execute the complete ETL pipeline.
    
    Pipeline flow:
    1. Extract data from College Scorecard API
    2. Validate extracted data
    3. Transform validated data
    4. Load transformed data into SQL Server
    
    If any stage fails, the pipeline stops immediately and does not
    proceed to subsequent stages.
    
    Args:
        min_records: Minimum number of records to extract
        
    Returns:
        PipelineResult with execution details
    """
    result = PipelineResult(started_at=datetime.now())
    
    logger.info("")
    logger.info("#" * 70)
    logger.info("#" + " " * 68 + "#")
    logger.info("#" + "    COLLEGE SCORECARD ETL PIPELINE - STARTING".center(68) + "#")
    logger.info("#" + " " * 68 + "#")
    logger.info("#" * 70)
    logger.info("")
    logger.info(f"Pipeline started at: {result.started_at.strftime('%Y-%m-%d %H:%M:%S')}")
    logger.info(f"Target records: {min_records}")
    logger.info("")
    
    # Stage 1: Extract
    extracted_df, extract_result = run_extract_stage(min_records)
    result.add_stage(extract_result)
    
    if extract_result.status == "failed":
        logger.error("Pipeline halted due to extraction failure")
        result.completed_at = datetime.now()
        return result
    
    result.total_records_extracted = extract_result.records_out
    
    # Stage 2: Validate
    validated_df, validate_result = run_validate_stage(extracted_df)
    result.add_stage(validate_result)
    
    if validate_result.status == "failed":
        logger.error("Pipeline halted due to validation failure")
        result.completed_at = datetime.now()
        return result
    
    # Stage 3: Transform
    transformed_df, transform_result = run_transform_stage(validated_df)
    result.add_stage(transform_result)
    
    if transform_result.status == "failed":
        logger.error("Pipeline halted due to transformation failure")
        result.completed_at = datetime.now()
        return result
    
    # Stage 4: Load
    load_stats, load_result = run_load_stage(transformed_df)
    result.add_stage(load_result)
    
    if load_result.status == "failed":
        logger.error("Pipeline halted due to loading failure")
        result.completed_at = datetime.now()
        return result
    
    result.total_records_loaded = load_result.records_out
    
    # Mark as successful
    result.status = "success"
    result.completed_at = datetime.now()
    
    return result


def print_execution_summary(result: PipelineResult) -> None:
    """
    Print detailed execution summary.
    
    Args:
        result: PipelineResult with execution details
    """
    logger.info("")
    logger.info("#" * 70)
    logger.info("#" + " " * 68 + "#")
    logger.info("#" + "    PIPELINE EXECUTION SUMMARY".center(68) + "#")
    logger.info("#" + " " * 68 + "#")
    logger.info("#" * 70)
    logger.info("")
    
    # Overall status
    status_symbol = "✓" if result.status == "success" else "✗"
    status_text = "SUCCESS" if result.status == "success" else "FAILED"
    logger.info(f"Overall Status: {status_symbol} {status_text}")
    logger.info("")
    
    # Timing
    if result.started_at:
        logger.info(f"Started:  {result.started_at.strftime('%Y-%m-%d %H:%M:%S')}")
    if result.completed_at:
        logger.info(f"Completed: {result.completed_at.strftime('%Y-%m-%d %H:%M:%S')}")
    logger.info(f"Duration: {result.duration_seconds:.2f} seconds")
    logger.info("")
    
    # Stage breakdown
    logger.info("Stage Results:")
    logger.info("-" * 60)
    logger.info(f"{'Stage':<15} {'Status':<10} {'Records In':<12} {'Records Out':<12} {'Time (s)':<10}")
    logger.info("-" * 60)
    
    for stage in result.stages:
        status_display = "✓ Pass" if stage.status == "success" else "✗ Fail"
        logger.info(
            f"{stage.stage_name:<15} {status_display:<10} "
            f"{stage.records_in:<12} {stage.records_out:<12} "
            f"{stage.duration_seconds:<10.2f}"
        )
    
    logger.info("-" * 60)
    logger.info("")
    
    # Record counts
    logger.info("Record Counts:")
    logger.info(f"  Total extracted: {result.total_records_extracted:,}")
    logger.info(f"  Total loaded:    {result.total_records_loaded:,}")
    logger.info("")
    
    # Error information if failed
    if result.status == "failed":
        logger.error("Error Details:")
        logger.error(f"  Failed stage: {result.error_stage}")
        logger.error(f"  Error: {result.error_message}")
        logger.info("")
    
    logger.info("#" * 70)


# =============================================================================
# CLI ENTRY POINT
# =============================================================================

def main() -> int:
    """
    Main entry point for CLI execution.
    
    Returns:
        Exit code (0 for success, 1 for failure)
    """
    try:
        # Run the pipeline
        result = run_pipeline(min_records=1000)
        
        # Print summary
        print_execution_summary(result)
        
        # Return appropriate exit code
        if result.status == "success":
            logger.info("Pipeline completed successfully!")
            return 0
        else:
            logger.error("Pipeline failed!")
            return 1
            
    except KeyboardInterrupt:
        logger.warning("\nPipeline interrupted by user")
        return 130
    except Exception as e:
        logger.exception(f"Unexpected error in pipeline: {e}")
        return 1


if __name__ == "__main__":
    sys.exit(main())
