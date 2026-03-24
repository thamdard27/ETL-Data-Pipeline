"""
ETL Module
==========

Core ETL components for College Scorecard data extraction.
"""

from higher_ed_data_pipeline.etl.college_scorecard_client import (
    CollegeScorecardClient,
    APIConfig,
    APIResponse,
)
from higher_ed_data_pipeline.etl.college_scorecard_extractor import (
    CollegeScorecardExtractor,
)

__all__ = [
    "CollegeScorecardClient",
    "CollegeScorecardExtractor",
    "APIConfig",
    "APIResponse",
]
