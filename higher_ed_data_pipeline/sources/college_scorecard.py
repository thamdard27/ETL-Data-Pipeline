"""
College Scorecard Data Source
=============================

Fetches higher education data from the US Department of Education's
College Scorecard API.

API Documentation: https://collegescorecard.ed.gov/data/documentation/

Available Data:
- Institution information (name, location, type)
- Enrollment statistics
- Costs and financial aid
- Completion rates
- Earnings outcomes
- Student demographics

Usage:
    from higher_ed_data_pipeline.sources import CollegeScorecardSource
    
    source = CollegeScorecardSource(api_key="your_api_key")
    df = source.fetch(state="CA", fields=["school.name", "latest.student.size"])
"""

import time
from typing import Any, Optional

import pandas as pd
import requests
from loguru import logger

from higher_ed_data_pipeline.config.settings import Settings, get_settings
from higher_ed_data_pipeline.sources.base import BaseDataSource, DataSourceRegistry


@DataSourceRegistry.register
class CollegeScorecardSource(BaseDataSource):
    """
    College Scorecard API data source.
    
    The College Scorecard API provides data on US colleges and universities
    from the Department of Education. Data includes costs, graduation rates,
    employment outcomes, and more.
    
    API Key: Get free key at https://api.data.gov/signup/
    """
    
    SOURCE_NAME = "college_scorecard"
    SOURCE_DESCRIPTION = "US Department of Education College Scorecard"
    SOURCE_URL = "https://api.data.gov/ed/collegescorecard/v1/schools"
    REQUIRES_API_KEY = True
    
    # Rate limiting
    REQUESTS_PER_SECOND = 5
    PAGE_SIZE = 100
    MAX_PAGES = 100  # Safety limit
    
    # Common field groups for convenience
    FIELD_GROUPS = {
        "basic": [
            "id",
            "school.name",
            "school.city",
            "school.state",
            "school.zip",
            "school.school_url",
            "school.ownership",
            "school.locale",
            "school.region_id",
        ],
        "enrollment": [
            "latest.student.size",
            "latest.student.enrollment.all",
            "latest.student.enrollment.undergrad_12_month",
            "latest.student.enrollment.grad_12_month",
            "latest.student.part_time_share",
        ],
        "demographics": [
            "latest.student.demographics.race_ethnicity.white",
            "latest.student.demographics.race_ethnicity.black",
            "latest.student.demographics.race_ethnicity.hispanic",
            "latest.student.demographics.race_ethnicity.asian",
            "latest.student.demographics.age_entry",
            "latest.student.demographics.female_share",
            "latest.student.share_firstgeneration",
        ],
        "cost": [
            "latest.cost.tuition.in_state",
            "latest.cost.tuition.out_of_state",
            "latest.cost.avg_net_price.overall",
            "latest.cost.attendance.academic_year",
            "latest.aid.median_debt.completers.overall",
            "latest.aid.pell_grant_rate",
        ],
        "outcomes": [
            "latest.completion.rate_suppressed.overall",
            "latest.completion.rate_suppressed.four_year",
            "latest.earnings.10_yrs_after_entry.median",
            "latest.earnings.6_yrs_after_entry.median",
            "latest.admissions.admission_rate.overall",
            "latest.admissions.sat_scores.average.overall",
        ],
        "programs": [
            "latest.academics.program_percentage.education",
            "latest.academics.program_percentage.business_marketing",
            "latest.academics.program_percentage.health",
            "latest.academics.program_percentage.computer",
            "latest.academics.program_percentage.engineering",
        ],
    }
    
    def __init__(
        self,
        settings: Optional[Settings] = None,
        api_key: Optional[str] = None,
    ) -> None:
        """
        Initialize College Scorecard source.
        
        Args:
            settings: Application settings
            api_key: API key (or set COLLEGE_SCORECARD_API_KEY env var)
        """
        super().__init__(settings)
        
        import os
        self.api_key = (
            api_key 
            or self.settings.college_scorecard_api_key
            or os.environ.get("COLLEGE_SCORECARD_API_KEY")
        )
        
        if not self.api_key:
            logger.warning(
                "No API key provided. Get a free key at https://api.data.gov/signup/ "
                "Set via api_key parameter, settings, or COLLEGE_SCORECARD_API_KEY env var."
            )
    
    def fetch(
        self,
        fields: Optional[list[str]] = None,
        field_groups: Optional[list[str]] = None,
        state: Optional[str] = None,
        institution_type: Optional[str] = None,
        degree_type: Optional[str] = None,
        min_enrollment: Optional[int] = None,
        max_pages: Optional[int] = None,
        **filters: Any,
    ) -> pd.DataFrame:
        """
        Fetch data from College Scorecard API.
        
        Args:
            fields: Specific fields to retrieve
            field_groups: Predefined field groups ("basic", "enrollment", etc.)
            state: Filter by state (2-letter code)
            institution_type: Filter by type (1=Public, 2=Private nonprofit, 3=Private for-profit)
            degree_type: Filter by predominant degree (1=Certificate, 2=Associate, 3=Bachelor's, 4=Graduate)
            min_enrollment: Minimum student enrollment
            max_pages: Maximum pages to fetch (default: all)
            **filters: Additional API filters
            
        Returns:
            pd.DataFrame: Institution data
        """
        # Build field list
        all_fields = set()
        
        if field_groups:
            for group in field_groups:
                if group in self.FIELD_GROUPS:
                    all_fields.update(self.FIELD_GROUPS[group])
        
        if fields:
            all_fields.update(fields)
        
        # Default to basic + enrollment if nothing specified
        if not all_fields:
            all_fields.update(self.FIELD_GROUPS["basic"])
            all_fields.update(self.FIELD_GROUPS["enrollment"])
        
        # Build query parameters
        params = {
            "api_key": self.api_key,
            "fields": ",".join(sorted(all_fields)),
            "per_page": self.PAGE_SIZE,
        }
        
        # Add filters
        if state:
            params["school.state"] = state
        if institution_type:
            params["school.ownership"] = institution_type
        if degree_type:
            params["school.degrees_awarded.predominant"] = degree_type
        if min_enrollment:
            params["latest.student.size__range"] = f"{min_enrollment}.."
        
        # Add any additional filters
        params.update(filters)
        
        # Fetch all pages
        max_pages = max_pages or self.MAX_PAGES
        all_results = []
        page = 0
        
        while page < max_pages:
            params["page"] = page
            
            try:
                response = requests.get(
                    self.SOURCE_URL,
                    params=params,
                    timeout=30,
                )
                response.raise_for_status()
                data = response.json()
                
                results = data.get("results", [])
                if not results:
                    break
                
                all_results.extend(results)
                
                # Check if more pages
                metadata = data.get("metadata", {})
                total = metadata.get("total", 0)
                
                logger.debug(
                    f"Fetched page {page + 1}: {len(results)} records "
                    f"({len(all_results):,}/{total:,} total)"
                )
                
                if len(all_results) >= total:
                    break
                
                page += 1
                
                # Rate limiting
                time.sleep(1 / self.REQUESTS_PER_SECOND)
                
            except requests.exceptions.RequestException as e:
                logger.error(f"API request failed: {e}")
                break
        
        logger.info(f"Fetched {len(all_results):,} institutions from College Scorecard")
        
        # Convert to DataFrame
        if not all_results:
            return pd.DataFrame()
        
        df = pd.json_normalize(all_results)
        
        # Clean column names (remove prefixes for readability)
        df.columns = [self._clean_column_name(col) for col in df.columns]
        
        return df
    
    def _clean_column_name(self, col: str) -> str:
        """Clean up column names from API."""
        # Remove common prefixes
        prefixes = ["latest.", "school."]
        for prefix in prefixes:
            if col.startswith(prefix):
                col = col[len(prefix):]
        return col.replace(".", "_")
    
    def get_available_datasets(self) -> list[dict[str, Any]]:
        """List available dataset configurations."""
        return [
            {
                "name": "all_institutions",
                "description": "All US higher education institutions",
                "field_groups": ["basic", "enrollment", "cost", "outcomes"],
            },
            {
                "name": "public_universities",
                "description": "Public universities only",
                "field_groups": ["basic", "enrollment", "cost", "outcomes"],
                "filters": {"institution_type": "1"},
            },
            {
                "name": "private_nonprofit",
                "description": "Private non-profit institutions",
                "field_groups": ["basic", "enrollment", "cost", "outcomes"],
                "filters": {"institution_type": "2"},
            },
            {
                "name": "by_state",
                "description": "Institutions by state (requires state parameter)",
                "field_groups": ["basic", "enrollment", "demographics"],
            },
            {
                "name": "enrollment_demographics",
                "description": "Enrollment and demographic data",
                "field_groups": ["basic", "enrollment", "demographics"],
            },
            {
                "name": "costs_and_aid",
                "description": "Tuition, costs, and financial aid data",
                "field_groups": ["basic", "cost"],
            },
            {
                "name": "outcomes_and_earnings",
                "description": "Completion rates and earnings outcomes",
                "field_groups": ["basic", "outcomes"],
            },
        ]
    
    def fetch_all_institutions(
        self,
        include_demographics: bool = True,
        include_costs: bool = True,
        include_outcomes: bool = True,
    ) -> pd.DataFrame:
        """
        Fetch comprehensive data for all institutions.
        
        Args:
            include_demographics: Include demographic data
            include_costs: Include cost/aid data
            include_outcomes: Include outcomes data
            
        Returns:
            pd.DataFrame: All institution data
        """
        groups = ["basic", "enrollment"]
        
        if include_demographics:
            groups.append("demographics")
        if include_costs:
            groups.append("cost")
        if include_outcomes:
            groups.append("outcomes")
        
        return self.fetch(field_groups=groups)
    
    def fetch_by_state(self, state: str, **kwargs: Any) -> pd.DataFrame:
        """
        Fetch institutions for a specific state.
        
        Args:
            state: Two-letter state code
            **kwargs: Additional parameters
            
        Returns:
            pd.DataFrame: State institutions
        """
        return self.fetch(state=state, **kwargs)
    
    def fetch_large_institutions(
        self,
        min_enrollment: int = 10000,
        **kwargs: Any
    ) -> pd.DataFrame:
        """
        Fetch large institutions by enrollment.
        
        Args:
            min_enrollment: Minimum student enrollment
            **kwargs: Additional parameters
            
        Returns:
            pd.DataFrame: Large institutions
        """
        return self.fetch(min_enrollment=min_enrollment, **kwargs)
