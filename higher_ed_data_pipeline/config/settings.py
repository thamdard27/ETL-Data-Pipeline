"""
Settings Module
===============

Centralized configuration management using Pydantic for validation.
Supports environment variables, .env files, and YAML configuration.

Usage:
    from higher_ed_data_pipeline.config.settings import Settings
    
    settings = Settings()
    print(settings.database_url)
"""

import os
from pathlib import Path
from typing import Any, Optional

from pydantic import Field, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """
    Application settings with validation and environment variable support.
    
    Configuration is loaded in the following order (later sources override earlier):
    1. Default values defined in this class
    2. Environment variables
    3. .env file (if present)
    
    Attributes:
        app_name: Name of the application
        environment: Deployment environment (development, staging, production)
        debug: Enable debug mode
        log_level: Logging level (DEBUG, INFO, WARNING, ERROR, CRITICAL)
        
        database_url: Primary database connection URL
        database_pool_size: Connection pool size
        database_max_overflow: Maximum overflow connections
        
        aws_access_key_id: AWS access key
        aws_secret_access_key: AWS secret key
        aws_region: AWS region
        s3_bucket: S3 bucket for data storage
        
        gcp_project_id: GCP project identifier
        gcs_bucket: GCS bucket for data storage
        
        data_raw_path: Path to raw data directory
        data_staging_path: Path to staging data directory
        data_processed_path: Path to processed data directory
        logs_path: Path to logs directory
        
        batch_size: Default batch size for processing
        max_workers: Maximum parallel workers
        retry_attempts: Number of retry attempts for failed operations
        retry_delay: Delay between retries in seconds
    """
    
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore",
    )
    
    # -------------------------------------------------------------------------
    # Application Settings
    # -------------------------------------------------------------------------
    app_name: str = Field(
        default="higher_ed_data_pipeline",
        description="Application name"
    )
    environment: str = Field(
        default="development",
        description="Deployment environment"
    )
    debug: bool = Field(
        default=False,
        description="Enable debug mode"
    )
    log_level: str = Field(
        default="INFO",
        description="Logging level"
    )
    
    # -------------------------------------------------------------------------
    # Database Settings
    # -------------------------------------------------------------------------
    database_url: Optional[str] = Field(
        default=None,
        description="Primary database connection URL"
    )
    database_pool_size: int = Field(
        default=5,
        description="Connection pool size"
    )
    database_max_overflow: int = Field(
        default=10,
        description="Maximum overflow connections"
    )
    
    # -------------------------------------------------------------------------
    # AWS Settings
    # -------------------------------------------------------------------------
    aws_access_key_id: Optional[str] = Field(
        default=None,
        description="AWS access key ID"
    )
    aws_secret_access_key: Optional[str] = Field(
        default=None,
        description="AWS secret access key"
    )
    aws_region: str = Field(
        default="us-east-1",
        description="AWS region"
    )
    s3_bucket: Optional[str] = Field(
        default=None,
        description="S3 bucket for data storage"
    )
    
    # -------------------------------------------------------------------------
    # GCP Settings
    # -------------------------------------------------------------------------
    gcp_project_id: Optional[str] = Field(
        default=None,
        description="GCP project identifier"
    )
    gcs_bucket: Optional[str] = Field(
        default=None,
        description="GCS bucket for data storage"
    )
    
    # -------------------------------------------------------------------------
    # Data Source API Keys
    # -------------------------------------------------------------------------
    college_scorecard_api_key: Optional[str] = Field(
        default=None,
        description="College Scorecard API key (get free at https://api.data.gov/signup/)"
    )
    gcs_bucket: Optional[str] = Field(
        default=None,
        description="GCS bucket for data storage"
    )
    
    # -------------------------------------------------------------------------
    # Path Settings
    # -------------------------------------------------------------------------
    base_path: Path = Field(
        default_factory=lambda: Path(__file__).parent.parent.parent,
        description="Base path of the project"
    )
    data_raw_path: Path = Field(
        default=None,
        description="Path to raw data directory"
    )
    data_staging_path: Path = Field(
        default=None,
        description="Path to staging data directory"
    )
    data_processed_path: Path = Field(
        default=None,
        description="Path to processed data directory"
    )
    logs_path: Path = Field(
        default=None,
        description="Path to logs directory"
    )
    
    # -------------------------------------------------------------------------
    # Processing Settings
    # -------------------------------------------------------------------------
    batch_size: int = Field(
        default=10000,
        description="Default batch size for processing"
    )
    max_workers: int = Field(
        default=4,
        description="Maximum parallel workers"
    )
    retry_attempts: int = Field(
        default=3,
        description="Number of retry attempts"
    )
    retry_delay: float = Field(
        default=1.0,
        description="Delay between retries in seconds"
    )
    
    def __init__(self, **kwargs: Any) -> None:
        """Initialize settings and set default paths."""
        super().__init__(**kwargs)
        
        # Set default paths relative to base_path if not provided
        if self.data_raw_path is None:
            self.data_raw_path = self.base_path / "data" / "raw"
        if self.data_staging_path is None:
            self.data_staging_path = self.base_path / "data" / "staging"
        if self.data_processed_path is None:
            self.data_processed_path = self.base_path / "data" / "processed"
        if self.logs_path is None:
            self.logs_path = self.base_path / "logs"
    
    @field_validator("environment")
    @classmethod
    def validate_environment(cls, v: str) -> str:
        """Validate environment is one of allowed values."""
        allowed = {"development", "staging", "production", "testing"}
        if v.lower() not in allowed:
            raise ValueError(f"environment must be one of {allowed}")
        return v.lower()
    
    @field_validator("log_level")
    @classmethod
    def validate_log_level(cls, v: str) -> str:
        """Validate log level is a valid logging level."""
        allowed = {"DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"}
        if v.upper() not in allowed:
            raise ValueError(f"log_level must be one of {allowed}")
        return v.upper()
    
    def ensure_directories(self) -> None:
        """Create all required directories if they don't exist."""
        for path in [
            self.data_raw_path,
            self.data_staging_path,
            self.data_processed_path,
            self.logs_path,
        ]:
            path.mkdir(parents=True, exist_ok=True)
    
    def to_dict(self) -> dict[str, Any]:
        """Convert settings to dictionary, masking sensitive values."""
        data = self.model_dump()
        sensitive_keys = {
            "database_url",
            "aws_access_key_id",
            "aws_secret_access_key",
            "college_scorecard_api_key",
        }
        for key in sensitive_keys:
            if data.get(key):
                data[key] = "***MASKED***"
        return data


# Singleton instance for easy import
_settings: Optional[Settings] = None


def get_settings() -> Settings:
    """
    Get the global settings instance.
    
    Returns:
        Settings: The global settings instance
    """
    global _settings
    if _settings is None:
        _settings = Settings()
    return _settings
