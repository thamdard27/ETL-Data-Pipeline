"""
College Scorecard API Client
============================

A robust, production-grade client for the U.S. Department of Education's
College Scorecard API. Supports pagination, rate limiting, and error handling.

API Documentation: https://collegescorecard.ed.gov/data/documentation/

Author: Data Engineering Team
"""

import time
from dataclasses import dataclass, field
from typing import Any, Dict, Iterator, List, Optional

import requests
from loguru import logger

from higher_ed_data_pipeline.config.settings import Settings, get_settings


@dataclass
class APIConfig:
    """Configuration for College Scorecard API."""
    
    base_url: str = "https://api.data.gov/ed/collegescorecard/v1/schools"
    api_key: str = ""
    per_page: int = 100  # Max allowed by API
    timeout: int = 30
    max_retries: int = 3
    retry_delay: float = 1.0
    rate_limit_delay: float = 0.1  # Delay between requests to avoid rate limiting


@dataclass
class APIResponse:
    """Structured response from the API."""
    
    success: bool
    data: List[Dict[str, Any]] = field(default_factory=list)
    metadata: Dict[str, Any] = field(default_factory=dict)
    error: Optional[str] = None
    total_records: int = 0
    page: int = 0


class CollegeScorecardClient:
    """
    Client for the College Scorecard API.
    
    Features:
    - Automatic pagination handling
    - Retry logic with exponential backoff
    - Rate limiting compliance
    - Comprehensive error handling
    - Field selection for optimized requests
    
    Usage:
        client = CollegeScorecardClient(api_key="your_key")
        
        # Fetch all schools
        for batch in client.fetch_schools_paginated():
            process(batch)
        
        # Fetch with filters
        schools = client.fetch_schools(state="CA", page=0)
    """
    
    # Default fields to fetch (mapped to API field names)
    DEFAULT_FIELDS = [
        "id",
        "school.name",
        "school.state",
        "school.city",
        "school.zip",
        "school.school_url",
        "school.ownership",
        "school.region_id",
        "school.locale",
        "school.carnegie_basic",
        "school.degrees_awarded.predominant",
        "school.degrees_awarded.highest",
        "latest.student.size",
        "latest.student.grad_students",
        "latest.student.demographics.men",
        "latest.student.demographics.women",
        "latest.student.demographics.race_ethnicity.white",
        "latest.student.demographics.race_ethnicity.black",
        "latest.student.demographics.race_ethnicity.hispanic",
        "latest.student.demographics.race_ethnicity.asian",
        "latest.admissions.admission_rate.overall",
        "latest.admissions.sat_scores.average.overall",
        "latest.admissions.act_scores.midpoint.cumulative",
        "latest.completion.rate_suppressed.overall",
        "latest.completion.rate_suppressed.four_year",
        "latest.cost.tuition.in_state",
        "latest.cost.tuition.out_of_state",
        "latest.aid.median_debt.completers.overall",
        "latest.earnings.10_yrs_after_entry.median",
        "location.lat",
        "location.lon",
    ]
    
    def __init__(
        self,
        api_key: str,
        config: Optional[APIConfig] = None,
        settings: Optional[Settings] = None,
    ) -> None:
        """
        Initialize the College Scorecard API client.
        
        Args:
            api_key: API key from api.data.gov
            config: Optional API configuration
            settings: Optional application settings
        """
        self.settings = settings or get_settings()
        self.config = config or APIConfig()
        self.config.api_key = api_key
        
        self._session = requests.Session()
        self._session.headers.update({
            "Accept": "application/json",
            "User-Agent": "HigherEdDataPipeline/1.0",
        })
        
        logger.info(
            f"Initialized CollegeScorecardClient "
            f"(base_url={self.config.base_url}, per_page={self.config.per_page})"
        )
    
    def _make_request(
        self,
        params: Dict[str, Any],
        retry_count: int = 0,
    ) -> APIResponse:
        """
        Make an API request with retry logic.
        
        Args:
            params: Query parameters
            retry_count: Current retry attempt
            
        Returns:
            APIResponse: Structured response
        """
        params["api_key"] = self.config.api_key
        
        try:
            logger.debug(f"API request: page={params.get('page', 0)}")
            
            response = self._session.get(
                self.config.base_url,
                params=params,
                timeout=self.config.timeout,
            )
            
            # Handle rate limiting
            if response.status_code == 429:
                if retry_count < self.config.max_retries:
                    wait_time = self.config.retry_delay * (2 ** retry_count)
                    logger.warning(f"Rate limited. Waiting {wait_time}s before retry...")
                    time.sleep(wait_time)
                    return self._make_request(params, retry_count + 1)
                else:
                    return APIResponse(
                        success=False,
                        error="Rate limit exceeded after max retries",
                    )
            
            # Handle other errors
            if response.status_code != 200:
                error_msg = f"API error: {response.status_code} - {response.text[:200]}"
                logger.error(error_msg)
                return APIResponse(success=False, error=error_msg)
            
            data = response.json()
            
            return APIResponse(
                success=True,
                data=data.get("results", []),
                metadata=data.get("metadata", {}),
                total_records=data.get("metadata", {}).get("total", 0),
                page=data.get("metadata", {}).get("page", 0),
            )
            
        except requests.exceptions.Timeout:
            if retry_count < self.config.max_retries:
                logger.warning(f"Timeout. Retry {retry_count + 1}/{self.config.max_retries}")
                time.sleep(self.config.retry_delay)
                return self._make_request(params, retry_count + 1)
            return APIResponse(success=False, error="Request timeout after max retries")
            
        except requests.exceptions.RequestException as e:
            logger.error(f"Request failed: {e}")
            return APIResponse(success=False, error=str(e))
        
        except Exception as e:
            logger.error(f"Unexpected error: {e}")
            return APIResponse(success=False, error=str(e))
    
    def fetch_schools(
        self,
        page: int = 0,
        per_page: Optional[int] = None,
        fields: Optional[List[str]] = None,
        state: Optional[str] = None,
        operating: bool = True,
        **filters: Any,
    ) -> APIResponse:
        """
        Fetch schools from the API.
        
        Args:
            page: Page number (0-indexed)
            per_page: Results per page (max 100)
            fields: List of fields to fetch (uses defaults if None)
            state: Filter by state abbreviation (e.g., "CA")
            operating: Only include currently operating schools
            **filters: Additional API filters
            
        Returns:
            APIResponse: API response with school data
        """
        params: Dict[str, Any] = {
            "page": page,
            "per_page": per_page or self.config.per_page,
        }
        
        # Add field selection
        selected_fields = fields or self.DEFAULT_FIELDS
        params["fields"] = ",".join(selected_fields)
        
        # Add filters
        if operating:
            params["school.operating"] = 1
        
        if state:
            params["school.state"] = state
        
        # Add any additional filters
        for key, value in filters.items():
            params[key] = value
        
        return self._make_request(params)
    
    def fetch_schools_paginated(
        self,
        fields: Optional[List[str]] = None,
        state: Optional[str] = None,
        max_pages: Optional[int] = None,
        **filters: Any,
    ) -> Iterator[List[Dict[str, Any]]]:
        """
        Fetch all schools with automatic pagination.
        
        Args:
            fields: List of fields to fetch
            state: Filter by state
            max_pages: Maximum pages to fetch (None for all)
            **filters: Additional filters
            
        Yields:
            list[dict]: Batches of school records
        """
        page = 0
        total_fetched = 0
        
        while True:
            response = self.fetch_schools(
                page=page,
                fields=fields,
                state=state,
                **filters,
            )
            
            if not response.success:
                logger.error(f"Pagination stopped due to error: {response.error}")
                break
            
            if not response.data:
                logger.info("No more data available")
                break
            
            total_fetched += len(response.data)
            logger.info(
                f"Fetched page {page + 1}: {len(response.data)} records "
                f"({total_fetched}/{response.total_records} total)"
            )
            
            yield response.data
            
            # Check if we've fetched all records
            if total_fetched >= response.total_records:
                logger.info(f"Completed fetching all {total_fetched} records")
                break
            
            # Check max pages limit
            if max_pages and page + 1 >= max_pages:
                logger.info(f"Reached max_pages limit ({max_pages})")
                break
            
            page += 1
            
            # Rate limiting delay
            time.sleep(self.config.rate_limit_delay)
    
    def fetch_all_schools(
        self,
        fields: Optional[List[str]] = None,
        state: Optional[str] = None,
        **filters: Any,
    ) -> List[Dict[str, Any]]:
        """
        Fetch all schools into a single list.
        
        Warning: This loads all data into memory. For large datasets,
        use fetch_schools_paginated() instead.
        
        Args:
            fields: List of fields to fetch
            state: Filter by state
            **filters: Additional filters
            
        Returns:
            list[dict]: All school records
        """
        all_schools = []
        
        for batch in self.fetch_schools_paginated(
            fields=fields,
            state=state,
            **filters,
        ):
            all_schools.extend(batch)
        
        return all_schools
    
    def get_total_count(
        self,
        state: Optional[str] = None,
        **filters: Any,
    ) -> int:
        """
        Get total count of schools matching filters without fetching data.
        
        Args:
            state: Filter by state
            **filters: Additional filters
            
        Returns:
            int: Total count of matching schools
        """
        response = self.fetch_schools(
            page=0,
            per_page=1,
            fields=["id"],
            state=state,
            **filters,
        )
        
        return response.total_records if response.success else 0
    
    def validate_connection(self) -> bool:
        """
        Validate API connection and key.
        
        Returns:
            bool: True if connection is valid
        """
        response = self.fetch_schools(page=0, per_page=1, fields=["id"])
        
        if response.success:
            logger.info("API connection validated successfully")
            return True
        else:
            logger.error(f"API validation failed: {response.error}")
            return False
