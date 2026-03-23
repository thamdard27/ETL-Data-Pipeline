"""
Higher Ed Data Pipeline
=======================

A production-grade ETL pipeline for higher education data processing.

This package provides modular components for:
- Data extraction from multiple sources (files, APIs, databases)
- Data transformation and cleaning
- Data loading to various destinations
- Configuration management
- Logging and monitoring

Author: Data Engineering Team
Version: 1.0.0
"""

__version__ = "1.0.0"
__author__ = "Data Engineering Team"

from higher_ed_data_pipeline.config.settings import Settings
from higher_ed_data_pipeline.etl.extract import Extractor
from higher_ed_data_pipeline.etl.transform import Transformer
from higher_ed_data_pipeline.etl.load import Loader
from higher_ed_data_pipeline.etl.pipeline import ETLPipeline

__all__ = [
    "Settings",
    "Extractor",
    "Transformer",
    "Loader",
    "ETLPipeline",
]
