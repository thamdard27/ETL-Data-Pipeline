"""
Database Connection Test Script
===============================

Tests connection to SQL Server using SQLAlchemy and pyodbc.
Validates connectivity before proceeding with data loading.

Usage:
    python scripts/test_db_connection.py
"""

import sys
import socket
from pathlib import Path

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))

from dotenv import load_dotenv
import os
import logging

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Load environment variables
load_dotenv()


def check_port_accessible(host: str = "localhost", port: int = 1433) -> bool:
    """
    Check if the SQL Server port is accessible.
    
    Args:
        host: Hostname to check
        port: Port number to check
        
    Returns:
        True if port is accessible, False otherwise
    """
    try:
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(5)
        result = sock.connect_ex((host, port))
        sock.close()
        return result == 0
    except Exception as e:
        logger.error(f"Port check failed: {e}")
        return False


def check_odbc_driver() -> bool:
    """
    Check if ODBC Driver 17 for SQL Server is installed.
    
    Returns:
        True if driver is found, False otherwise
    """
    try:
        import pyodbc
        drivers = pyodbc.drivers()
        logger.info(f"Available ODBC drivers: {drivers}")
        
        # Check for ODBC Driver 17 or 18
        has_driver_17 = "ODBC Driver 17 for SQL Server" in drivers
        has_driver_18 = "ODBC Driver 18 for SQL Server" in drivers
        
        if has_driver_17:
            logger.info("✓ ODBC Driver 17 for SQL Server is installed")
            return True
        elif has_driver_18:
            logger.info("✓ ODBC Driver 18 for SQL Server is installed (alternative)")
            return True
        else:
            logger.error("✗ Neither ODBC Driver 17 nor 18 for SQL Server is installed")
            return False
    except ImportError:
        logger.error("✗ pyodbc is not installed. Run: pip install pyodbc")
        return False
    except Exception as e:
        logger.error(f"✗ Error checking ODBC drivers: {e}")
        return False


def test_connection_sqlalchemy() -> bool:
    """
    Test database connection using SQLAlchemy.
    Executes SELECT 1 to verify connectivity.
    
    Returns:
        True if connection successful, False otherwise
    """
    from sqlalchemy import create_engine, text
    from sqlalchemy.exc import SQLAlchemyError
    
    database_url = os.getenv("DATABASE_URL")
    
    if not database_url:
        logger.error("✗ DATABASE_URL environment variable is not set")
        return False
    
    logger.info(f"Attempting connection to database...")
    # Mask password in log output
    masked_url = database_url.replace(database_url.split('@')[0].split(':')[-1], '****')
    logger.info(f"Connection string: {masked_url}")
    
    try:
        engine = create_engine(database_url, echo=False)
        
        with engine.connect() as connection:
            # Execute SELECT 1 to verify connection
            result = connection.execute(text("SELECT 1 AS test_value"))
            row = result.fetchone()
            
            if row and row[0] == 1:
                logger.info("✓ Successfully executed: SELECT 1")
                logger.info("✓ DATABASE CONNECTION SUCCESSFUL!")
                
                # Get server version for additional verification
                version_result = connection.execute(text("SELECT @@VERSION"))
                version_row = version_result.fetchone()
                if version_row:
                    version = version_row[0]
                    logger.info(f"✓ SQL Server Version: {version[:80]}...")
                
                return True
            else:
                logger.error("✗ SELECT 1 returned unexpected result")
                return False
                
    except SQLAlchemyError as e:
        logger.error(f"✗ SQLAlchemy connection error: {e}")
        return False
    except Exception as e:
        logger.error(f"✗ Unexpected error during connection: {e}")
        return False


def run_validation_checklist() -> dict:
    """
    Run the complete validation checklist.
    
    Returns:
        Dictionary with validation results
    """
    results = {
        "port_accessible": False,
        "odbc_driver_installed": False,
        "connection_string_valid": False,
        "connection_successful": False,
        "all_checks_passed": False
    }
    
    logger.info("=" * 60)
    logger.info("DATABASE CONNECTION VALIDATION CHECKLIST")
    logger.info("=" * 60)
    
    # Check 1: Port 1433 accessible
    logger.info("\n[1/4] Checking if port 1433 is accessible...")
    results["port_accessible"] = check_port_accessible()
    if results["port_accessible"]:
        logger.info("✓ Port 1433 is accessible")
    else:
        logger.error("✗ Port 1433 is NOT accessible")
        logger.error("  - Ensure SQL Server container is running")
        logger.error("  - Run: docker ps | grep sql")
    
    # Check 2: ODBC Driver installed
    logger.info("\n[2/4] Checking ODBC driver installation...")
    results["odbc_driver_installed"] = check_odbc_driver()
    
    # Check 3: Connection string valid
    logger.info("\n[3/4] Checking connection string...")
    database_url = os.getenv("DATABASE_URL")
    if database_url and "mssql+pyodbc" in database_url:
        results["connection_string_valid"] = True
        logger.info("✓ Connection string format is valid")
    else:
        logger.error("✗ Connection string is invalid or missing")
    
    # Check 4: Database connection
    logger.info("\n[4/4] Testing database connection...")
    if results["port_accessible"] and results["odbc_driver_installed"]:
        results["connection_successful"] = test_connection_sqlalchemy()
    else:
        logger.warning("⚠ Skipping connection test due to previous failures")
    
    # Summary
    results["all_checks_passed"] = all([
        results["port_accessible"],
        results["odbc_driver_installed"],
        results["connection_string_valid"],
        results["connection_successful"]
    ])
    
    logger.info("\n" + "=" * 60)
    logger.info("VALIDATION SUMMARY")
    logger.info("=" * 60)
    
    for check, passed in results.items():
        status = "✓ PASSED" if passed else "✗ FAILED"
        logger.info(f"  {check}: {status}")
    
    if results["all_checks_passed"]:
        logger.info("\n✓ ALL VALIDATION CHECKS PASSED!")
        logger.info("  Database is ready for use.")
    else:
        logger.error("\n✗ SOME VALIDATION CHECKS FAILED")
        logger.error("  Please resolve issues before proceeding.")
    
    logger.info("=" * 60)
    
    return results


if __name__ == "__main__":
    results = run_validation_checklist()
    
    # Exit with appropriate code
    sys.exit(0 if results["all_checks_passed"] else 1)
