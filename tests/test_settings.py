"""
Test Settings Module
==================

Tests for configuration and settings management.
"""

import os
from pathlib import Path

import pytest

from higher_ed_data_pipeline.config.settings import Settings, get_settings


class TestSettings:
    """Test cases for Settings class."""
    
    def test_default_settings(self):
        """Test that default settings are properly initialized."""
        settings = Settings()
        
        assert settings.app_name == "higher_ed_data_pipeline"
        assert settings.environment == "development"
        assert settings.debug is False
        assert settings.log_level == "INFO"
        assert settings.batch_size == 10000
        assert settings.max_workers == 4
    
    def test_environment_validation(self):
        """Test that environment is validated."""
        # Valid environments should work
        for env in ["development", "staging", "production", "testing"]:
            settings = Settings(environment=env)
            assert settings.environment == env
        
        # Invalid environment should raise error
        with pytest.raises(ValueError):
            Settings(environment="invalid")
    
    def test_log_level_validation(self):
        """Test that log level is validated."""
        # Valid log levels should work
        for level in ["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"]:
            settings = Settings(log_level=level)
            assert settings.log_level == level
        
        # Invalid log level should raise error
        with pytest.raises(ValueError):
            Settings(log_level="INVALID")
    
    def test_paths_initialized(self):
        """Test that paths are properly initialized."""
        settings = Settings()
        
        assert settings.data_raw_path is not None
        assert settings.data_staging_path is not None
        assert settings.data_processed_path is not None
        assert settings.logs_path is not None
        
        assert isinstance(settings.data_raw_path, Path)
    
    def test_ensure_directories(self, tmp_path):
        """Test that directories are created."""
        settings = Settings(
            data_raw_path=tmp_path / "raw",
            data_staging_path=tmp_path / "staging",
            data_processed_path=tmp_path / "processed",
            logs_path=tmp_path / "logs",
        )
        
        settings.ensure_directories()
        
        assert (tmp_path / "raw").exists()
        assert (tmp_path / "staging").exists()
        assert (tmp_path / "processed").exists()
        assert (tmp_path / "logs").exists()
    
    def test_to_dict_masks_sensitive_values(self):
        """Test that sensitive values are masked in dict output."""
        settings = Settings(
            database_url="postgresql://user:pass@localhost/db",
            aws_access_key_id="AKIAIOSFODNN7EXAMPLE",
            aws_secret_access_key="wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
        )
        
        result = settings.to_dict()
        
        assert result["database_url"] == "***MASKED***"
        assert result["aws_access_key_id"] == "***MASKED***"
        assert result["aws_secret_access_key"] == "***MASKED***"
    
    def test_get_settings_singleton(self):
        """Test that get_settings returns singleton instance."""
        settings1 = get_settings()
        settings2 = get_settings()
        
        # Both should reference the same object
        assert settings1 is settings2


class TestSettingsFromEnvironment:
    """Test settings loaded from environment variables."""
    
    def test_loads_from_env(self, monkeypatch):
        """Test that settings can be loaded from environment."""
        monkeypatch.setenv("ENVIRONMENT", "production")
        monkeypatch.setenv("DEBUG", "true")
        monkeypatch.setenv("LOG_LEVEL", "DEBUG")
        monkeypatch.setenv("BATCH_SIZE", "5000")
        
        # Create new settings to pick up env vars
        settings = Settings()
        
        assert settings.environment == "production"
        assert settings.debug is True
        assert settings.log_level == "DEBUG"
        assert settings.batch_size == 5000
