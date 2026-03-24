#!/usr/bin/env python3
"""
College Scorecard Data Loading Module

Loads transformed data into SQL Server database.

Implements:
- Database connection management using SQLAlchemy
- Table creation with proper schema
- Efficient bulk data insertion
- Transaction management for data integrity
- Comprehensive error handling and logging

Usage:
    from scripts.load import load_data
    
    success = load_data(transformed_df)

Configuration:
    Database connection string is read from DATABASE_URL environment variable.
    Table name: college_scorecard
"""

import logging
import os
import sys
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Optional, TYPE_CHECKING

from dotenv import load_dotenv

if TYPE_CHECKING:
    import pandas as pd

# Load environment variables
load_dotenv()

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
    "load_data",
    "load_to_sql_server",
    "create_engine_from_env",
    "LoadError",
    "TABLE_NAME",
    "BATCH_SIZE",
]


# =============================================================================
# CONFIGURATION
# =============================================================================

TABLE_NAME = "college_scorecard"
BATCH_SIZE = 500  # Records per batch for insertion (smaller for SQL Server compatibility)
SCHEMA_NAME = "dbo"  # Default SQL Server schema


# =============================================================================
# EXCEPTIONS
# =============================================================================

class LoadError(Exception):
    """Custom exception for data loading errors."""
    pass


# =============================================================================
# DATABASE CONNECTION
# =============================================================================

def create_engine_from_env():
    """
    Create SQLAlchemy engine from DATABASE_URL environment variable.
    
    Returns:
        SQLAlchemy Engine object
        
    Raises:
        LoadError: If DATABASE_URL is not set or connection fails
    """
    from sqlalchemy import create_engine
    
    database_url = os.getenv("DATABASE_URL")
    
    if not database_url:
        raise LoadError(
            "DATABASE_URL environment variable is not set. "
            "Please set it in your .env file or environment."
        )
    
    try:
        engine = create_engine(
            database_url,
            echo=False,
            pool_pre_ping=True,  # Verify connections before use
            pool_size=5,
            max_overflow=10,
        )
        return engine
    except Exception as e:
        raise LoadError(f"Failed to create database engine: {e}")


def test_connection(engine) -> bool:
    """
    Test database connection.
    
    Args:
        engine: SQLAlchemy Engine
        
    Returns:
        True if connection successful
        
    Raises:
        LoadError: If connection fails
    """
    from sqlalchemy import text
    
    try:
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        return True
    except Exception as e:
        raise LoadError(f"Database connection test failed: {e}")


# =============================================================================
# TABLE MANAGEMENT
# =============================================================================

def drop_table_if_exists(engine, table_name: str = TABLE_NAME) -> None:
    """
    Drop the table if it exists (for full refresh strategy).
    
    Args:
        engine: SQLAlchemy Engine
        table_name: Name of the table to drop
    """
    from sqlalchemy import text
    
    try:
        with engine.connect() as conn:
            conn.execute(text(f"DROP TABLE IF EXISTS {SCHEMA_NAME}.{table_name}"))
            conn.commit()
        logger.info(f"✓ Dropped existing table {SCHEMA_NAME}.{table_name} (if any)")
    except Exception as e:
        logger.warning(f"Could not drop table: {e}")


def get_table_row_count(engine, table_name: str = TABLE_NAME) -> int:
    """
    Get the current row count of a table.
    
    Args:
        engine: SQLAlchemy Engine
        table_name: Name of the table
        
    Returns:
        Number of rows in the table, or 0 if table doesn't exist
    """
    from sqlalchemy import text
    
    try:
        with engine.connect() as conn:
            result = conn.execute(
                text(f"SELECT COUNT(*) FROM {SCHEMA_NAME}.{table_name}")
            )
            return result.scalar() or 0
    except Exception:
        return 0


# =============================================================================
# DATA LOADING
# =============================================================================

def load_to_sql_server(
    df: "pd.DataFrame",
    engine,
    table_name: str = TABLE_NAME,
    if_exists: str = "replace",
    batch_size: int = BATCH_SIZE,
) -> Dict[str, Any]:
    """
    Load DataFrame to SQL Server table.
    
    Args:
        df: DataFrame to load
        engine: SQLAlchemy Engine
        table_name: Target table name
        if_exists: How to behave if table exists ('fail', 'replace', 'append')
        batch_size: Number of rows per batch
        
    Returns:
        Dictionary with loading statistics
        
    Raises:
        LoadError: If loading fails
    """
    import pandas as pd
    
    logger.info("=" * 60)
    logger.info("DATA LOADING TO SQL SERVER")
    logger.info("=" * 60)
    
    stats: Dict[str, Any] = {
        "table_name": f"{SCHEMA_NAME}.{table_name}",
        "input_rows": len(df),
        "input_columns": len(df.columns),
        "started_at": datetime.now().isoformat(),
        "status": "pending",
    }
    
    # Prepare DataFrame for SQL Server
    # Convert problematic columns and handle NA values
    df_to_load = df.copy()
    
    # Convert datetime columns to string for compatibility
    for col in df_to_load.select_dtypes(include=['datetime64']).columns:
        df_to_load[col] = df_to_load[col].astype(str)
    
    # Convert nullable Int64 columns to float64 for SQL compatibility
    # SQL Server handles NULL floats better than pandas Int64
    for col in df_to_load.columns:
        dtype_str = str(df_to_load[col].dtype)
        if dtype_str.startswith('Int') or dtype_str.startswith('UInt'):
            df_to_load[col] = df_to_load[col].astype('float64')
    
    # Convert string columns with pd.NA to regular None
    for col in df_to_load.select_dtypes(include=['string', 'object']).columns:
        df_to_load[col] = df_to_load[col].astype(object).where(df_to_load[col].notna(), None)
    
    logger.info(f"Input: {len(df_to_load)} rows, {len(df_to_load.columns)} columns")
    logger.info(f"Target table: {SCHEMA_NAME}.{table_name}")
    logger.info(f"Mode: {if_exists}")
    logger.info(f"Batch size: {batch_size}")
    
    # Get row count before (for append mode)
    rows_before = get_table_row_count(engine, table_name)
    stats["rows_before"] = rows_before
    
    try:
        # Use pandas to_sql with chunking for large datasets
        # Note: Using default method (not 'multi') for SQL Server compatibility
        # The 'multi' method can exceed ODBC parameter limits
        start_time = datetime.now()
        
        df_to_load.to_sql(
            name=table_name,
            con=engine,
            schema=SCHEMA_NAME,
            if_exists=if_exists,
            index=False,
            chunksize=batch_size,
            method=None,  # Use default single-row INSERT for SQL Server compatibility
        )
        
        end_time = datetime.now()
        duration = (end_time - start_time).total_seconds()
        
        # Verify row count
        rows_after = get_table_row_count(engine, table_name)
        rows_inserted = rows_after - rows_before if if_exists == 'append' else rows_after
        
        stats.update({
            "rows_after": rows_after,
            "rows_inserted": rows_inserted,
            "duration_seconds": duration,
            "rows_per_second": len(df_to_load) / duration if duration > 0 else 0,
            "completed_at": datetime.now().isoformat(),
            "status": "success",
        })
        
        logger.info(f"✓ Successfully loaded {rows_inserted} rows")
        logger.info(f"  Duration: {duration:.2f} seconds")
        logger.info(f"  Rate: {stats['rows_per_second']:.0f} rows/second")
        
        return stats
        
    except Exception as e:
        stats["status"] = "failed"
        stats["error"] = str(e)
        raise LoadError(f"Failed to load data to SQL Server: {e}")


# =============================================================================
# MAIN ENTRY POINT
# =============================================================================

def load_data(
    df: "pd.DataFrame",
    table_name: str = TABLE_NAME,
    if_exists: str = "replace",
) -> Dict[str, Any]:
    """
    Load transformed data into SQL Server database.
    
    This is the primary entry point for pipeline integration.
    
    Args:
        df: Transformed DataFrame from transform_data()
        table_name: Target table name (default: college_scorecard)
        if_exists: How to behave if table exists:
            - 'fail': Raise error if table exists
            - 'replace': Drop table and recreate
            - 'append': Add rows to existing table
            
    Returns:
        Dictionary with loading statistics including:
            - table_name: Full table name with schema
            - input_rows: Number of rows in input DataFrame
            - rows_inserted: Number of rows actually inserted
            - duration_seconds: Time taken to load
            - status: 'success' or 'failed'
            
    Raises:
        LoadError: If database connection or loading fails
        
    Example:
        >>> from scripts.transform import transform_data
        >>> from scripts.load import load_data
        >>> 
        >>> analytics_df = transform_data(validated_df)
        >>> result = load_data(analytics_df)
        >>> print(f"Loaded {result['rows_inserted']} rows")
    """
    logger.info("=" * 60)
    logger.info("LOAD_DATA ENTRY POINT")
    logger.info("=" * 60)
    
    if df is None or len(df) == 0:
        raise LoadError("Cannot load empty DataFrame")
    
    logger.info(f"DataFrame to load: {len(df)} rows, {len(df.columns)} columns")
    
    # Create database engine
    logger.info("Creating database connection...")
    engine = create_engine_from_env()
    
    # Test connection
    logger.info("Testing database connection...")
    test_connection(engine)
    logger.info("✓ Database connection successful")
    
    # Load data
    stats = load_to_sql_server(
        df=df,
        engine=engine,
        table_name=table_name,
        if_exists=if_exists,
        batch_size=BATCH_SIZE,
    )
    
    # Log summary
    logger.info("=" * 60)
    logger.info("LOAD_DATA SUMMARY")
    logger.info("=" * 60)
    logger.info(f"Table: {stats['table_name']}")
    logger.info(f"Rows loaded: {stats['rows_inserted']}")
    logger.info(f"Duration: {stats['duration_seconds']:.2f}s")
    logger.info(f"Status: {stats['status'].upper()}")
    
    return stats


# =============================================================================
# CLI EXECUTION
# =============================================================================

def main() -> int:
    """Main entry point for CLI testing."""
    import pandas as pd
    
    logger.info("Load module test - creating sample data")
    
    # Create sample DataFrame for testing
    sample_data = {
        "school_name": ["Test University", "Sample College"],
        "school_state": ["CA", "NY"],
        "student_size": [10000, 5000],
        "admission_rate": [0.75, 0.85],
        "completion_rate": [0.65, 0.70],
    }
    df = pd.DataFrame(sample_data)
    
    try:
        result = load_data(df, table_name="test_college_scorecard")
        logger.info(f"Test load successful: {result['rows_inserted']} rows")
        return 0
    except LoadError as e:
        logger.error(f"Load failed: {e}")
        return 1


if __name__ == "__main__":
    sys.exit(main())
