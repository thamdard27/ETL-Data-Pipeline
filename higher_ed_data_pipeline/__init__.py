"""
Higher Ed Data Pipeline
=======================

A production-grade ETL pipeline for higher education data processing.
All data is fetched programmatically from live sources - no manual downloads.

This package provides modular components for:
- Live data extraction from APIs (College Scorecard, IPEDS)
- Data transformation and cleaning
- Data loading to various destinations
- Raw data lake storage with versioning
- Automated pipeline execution

Data Sources:
- College Scorecard API (US Department of Education)
- IPEDS (Integrated Postsecondary Education Data System)

Author: Data Engineering Team
Version: 1.1.0
"""

__version__ = "1.1.0"
__author__ = "Data Engineering Team"

from higher_ed_data_pipeline.config.settings import Settings
from higher_ed_data_pipeline.etl.extract import Extractor
from higher_ed_data_pipeline.etl.transform import Transformer
from higher_ed_data_pipeline.etl.load import Loader
from higher_ed_data_pipeline.etl.pipeline import ETLPipeline
from higher_ed_data_pipeline.storage import DataLake
from higher_ed_data_pipeline.runner import AutomatedRunner

__all__ = [
    "Settings",
    "Extractor",
    "Transformer",
    "Loader",
    "ETLPipeline",
    "DataLake",
    "AutomatedRunner",
]
