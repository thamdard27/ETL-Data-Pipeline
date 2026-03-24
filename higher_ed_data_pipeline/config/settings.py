"""
Settings Module
===============

Lightweight configuration management for the ETL pipeline.
"""

import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Optional


@dataclass
class Settings:
    """
    Application settings with sensible defaults.
    
    Can be configured via environment variables.
    """
    
    # Application
    app_name: str = "higher_ed_data_pipeline"
    environment: str = field(default_factory=lambda: os.getenv("ENVIRONMENT", "development"))
    debug: bool = field(default_factory=lambda: os.getenv("DEBUG", "false").lower() == "true")
    log_level: str = field(default_factory=lambda: os.getenv("LOG_LEVEL", "INFO"))
    
    # Paths
    base_path: Path = field(default_factory=lambda: Path(__file__).parent.parent.parent)
    
    # Processing
    batch_size: int = field(default_factory=lambda: int(os.getenv("BATCH_SIZE", "10000")))
    max_workers: int = field(default_factory=lambda: int(os.getenv("MAX_WORKERS", "4")))
    retry_attempts: int = 3
    retry_delay: float = 1.0
    
    # Database (optional)
    database_url: Optional[str] = field(default_factory=lambda: os.getenv("DATABASE_URL"))
    database_pool_size: int = 5
    database_max_overflow: int = 10
    
    # AWS (optional)
    aws_access_key_id: Optional[str] = field(default_factory=lambda: os.getenv("AWS_ACCESS_KEY_ID"))
    aws_secret_access_key: Optional[str] = field(default_factory=lambda: os.getenv("AWS_SECRET_ACCESS_KEY"))
    aws_region: str = field(default_factory=lambda: os.getenv("AWS_REGION", "us-east-1"))
    s3_bucket: Optional[str] = field(default_factory=lambda: os.getenv("S3_BUCKET"))
    
    # GCP (optional)
    gcp_project_id: Optional[str] = field(default_factory=lambda: os.getenv("GCP_PROJECT_ID"))
    gcs_bucket: Optional[str] = field(default_factory=lambda: os.getenv("GCS_BUCKET"))
    
    def __post_init__(self):
        """Initialize computed paths."""
        self.data_raw_path = self.base_path / "data" / "raw"
        self.data_staging_path = self.base_path / "data" / "staging"
        self.data_processed_path = self.base_path / "data" / "processed"
        self.logs_path = self.base_path / "logs"
    
    def ensure_directories(self) -> None:
        """Create all required directories."""
        for path in [
            self.data_raw_path,
            self.data_staging_path,
            self.data_processed_path,
            self.logs_path,
        ]:
            path.mkdir(parents=True, exist_ok=True)
    
    def to_dict(self) -> dict[str, Any]:
        """Convert settings to dictionary, masking sensitive values."""
        data = {
            "app_name": self.app_name,
            "environment": self.environment,
            "debug": self.debug,
            "log_level": self.log_level,
            "base_path": str(self.base_path),
            "batch_size": self.batch_size,
            "max_workers": self.max_workers,
        }
        return data


# Singleton instance
_settings: Optional[Settings] = None


def get_settings() -> Settings:
    """Get the global settings instance."""
    global _settings
    if _settings is None:
        _settings = Settings()
        _settings.ensure_directories()
    return _settings
