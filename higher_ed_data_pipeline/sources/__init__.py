"""
Data Sources Module
===================

Live data source connectors for higher education data.
All data is fetched programmatically - no manual downloads required.

Available Sources:
- College Scorecard API (US Department of Education)
- IPEDS (Integrated Postsecondary Education Data System)
- Open data APIs for institutional data
"""

from higher_ed_data_pipeline.sources.college_scorecard import CollegeScorecardSource
from higher_ed_data_pipeline.sources.ipeds import IPEDSSource
from higher_ed_data_pipeline.sources.base import BaseDataSource, DataSourceRegistry

__all__ = [
    "BaseDataSource",
    "DataSourceRegistry",
    "CollegeScorecardSource",
    "IPEDSSource",
]
