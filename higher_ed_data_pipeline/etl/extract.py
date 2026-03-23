"""
Extractor Module
================

Handles data extraction from various sources including:
- Local files (CSV, JSON, Parquet, Excel)
- Databases (PostgreSQL, MySQL, SQLite)
- APIs (REST, GraphQL)
- Cloud storage (S3, GCS, Azure Blob)

Design Patterns:
- Strategy pattern for different extraction methods
- Factory pattern for source-specific extractors

Usage:
    from higher_ed_data_pipeline.etl.extract import Extractor
    
    extractor = Extractor(settings)
    df = extractor.from_csv("path/to/file.csv")
    df = extractor.from_database("SELECT * FROM students")
"""

import os
from abc import ABC, abstractmethod
from pathlib import Path
from typing import Any, Iterator, Optional, Union

import pandas as pd
from loguru import logger

from higher_ed_data_pipeline.config.settings import Settings, get_settings


class BaseExtractor(ABC):
    """Abstract base class for all extractors."""
    
    @abstractmethod
    def extract(self, source: str, **kwargs: Any) -> pd.DataFrame:
        """
        Extract data from the source.
        
        Args:
            source: Source identifier (path, query, URL, etc.)
            **kwargs: Additional extraction parameters
            
        Returns:
            pd.DataFrame: Extracted data
        """
        pass


class FileExtractor(BaseExtractor):
    """Extractor for local file sources."""
    
    SUPPORTED_FORMATS = {".csv", ".json", ".parquet", ".xlsx", ".xls"}
    
    def __init__(self, settings: Settings) -> None:
        """
        Initialize the file extractor.
        
        Args:
            settings: Application settings
        """
        self.settings = settings
    
    def extract(self, source: str, **kwargs: Any) -> pd.DataFrame:
        """
        Extract data from a local file.
        
        Args:
            source: Path to the file
            **kwargs: Additional parameters passed to pandas read functions
            
        Returns:
            pd.DataFrame: Extracted data
            
        Raises:
            FileNotFoundError: If the file doesn't exist
            ValueError: If the file format is not supported
        """
        path = Path(source)
        
        if not path.exists():
            raise FileNotFoundError(f"File not found: {source}")
        
        suffix = path.suffix.lower()
        if suffix not in self.SUPPORTED_FORMATS:
            raise ValueError(
                f"Unsupported file format: {suffix}. "
                f"Supported formats: {self.SUPPORTED_FORMATS}"
            )
        
        logger.info(f"Extracting data from file: {source}")
        
        readers = {
            ".csv": pd.read_csv,
            ".json": pd.read_json,
            ".parquet": pd.read_parquet,
            ".xlsx": pd.read_excel,
            ".xls": pd.read_excel,
        }
        
        df = readers[suffix](source, **kwargs)
        logger.info(f"Extracted {len(df):,} rows from {source}")
        
        return df
    
    def extract_chunked(
        self,
        source: str,
        chunk_size: Optional[int] = None,
        **kwargs: Any
    ) -> Iterator[pd.DataFrame]:
        """
        Extract data in chunks for memory-efficient processing.
        
        Args:
            source: Path to the file
            chunk_size: Number of rows per chunk (defaults to settings.batch_size)
            **kwargs: Additional parameters passed to pandas read functions
            
        Yields:
            pd.DataFrame: Data chunks
        """
        chunk_size = chunk_size or self.settings.batch_size
        path = Path(source)
        suffix = path.suffix.lower()
        
        logger.info(f"Extracting data in chunks from: {source}")
        
        if suffix == ".csv":
            for i, chunk in enumerate(
                pd.read_csv(source, chunksize=chunk_size, **kwargs)
            ):
                logger.debug(f"Processing chunk {i + 1} with {len(chunk):,} rows")
                yield chunk
        else:
            # For non-CSV files, read full file and yield chunks
            df = self.extract(source, **kwargs)
            for i in range(0, len(df), chunk_size):
                yield df.iloc[i:i + chunk_size]


class DatabaseExtractor(BaseExtractor):
    """Extractor for database sources."""
    
    def __init__(self, settings: Settings) -> None:
        """
        Initialize the database extractor.
        
        Args:
            settings: Application settings
        """
        self.settings = settings
        self._engine = None
    
    @property
    def engine(self):
        """Lazy initialization of database engine."""
        if self._engine is None:
            from sqlalchemy import create_engine
            
            if not self.settings.database_url:
                raise ValueError("database_url is not configured")
            
            self._engine = create_engine(
                self.settings.database_url,
                pool_size=self.settings.database_pool_size,
                max_overflow=self.settings.database_max_overflow,
            )
        return self._engine
    
    def extract(self, source: str, **kwargs: Any) -> pd.DataFrame:
        """
        Extract data using a SQL query.
        
        Args:
            source: SQL query string
            **kwargs: Additional parameters passed to pd.read_sql
            
        Returns:
            pd.DataFrame: Query results
        """
        logger.info(f"Executing SQL query: {source[:100]}...")
        
        df = pd.read_sql(source, self.engine, **kwargs)
        logger.info(f"Extracted {len(df):,} rows from database")
        
        return df
    
    def extract_table(self, table_name: str, **kwargs: Any) -> pd.DataFrame:
        """
        Extract entire table.
        
        Args:
            table_name: Name of the table
            **kwargs: Additional parameters
            
        Returns:
            pd.DataFrame: Table data
        """
        return self.extract(f"SELECT * FROM {table_name}", **kwargs)


class APIExtractor(BaseExtractor):
    """Extractor for REST API sources."""
    
    def __init__(self, settings: Settings) -> None:
        """
        Initialize the API extractor.
        
        Args:
            settings: Application settings
        """
        self.settings = settings
    
    def extract(
        self,
        source: str,
        method: str = "GET",
        headers: Optional[dict[str, str]] = None,
        params: Optional[dict[str, Any]] = None,
        json_path: Optional[str] = None,
        **kwargs: Any
    ) -> pd.DataFrame:
        """
        Extract data from a REST API endpoint.
        
        Args:
            source: API endpoint URL
            method: HTTP method (GET, POST, etc.)
            headers: Request headers
            params: Query parameters
            json_path: JSON path to extract data from response
            **kwargs: Additional parameters passed to requests
            
        Returns:
            pd.DataFrame: API response as DataFrame
        """
        import requests
        
        logger.info(f"Fetching data from API: {source}")
        
        response = requests.request(
            method=method,
            url=source,
            headers=headers,
            params=params,
            timeout=kwargs.pop("timeout", 30),
            **kwargs
        )
        response.raise_for_status()
        
        data = response.json()
        
        # Navigate to nested data if json_path provided
        if json_path:
            for key in json_path.split("."):
                data = data[key]
        
        df = pd.DataFrame(data)
        logger.info(f"Extracted {len(df):,} rows from API")
        
        return df


class Extractor:
    """
    Main extractor class that delegates to specialized extractors.
    
    This class provides a unified interface for extracting data from
    various sources, abstracting away the complexity of different
    extraction methods.
    
    Usage:
        extractor = Extractor(settings)
        
        # From CSV file
        df = extractor.from_csv("data/raw/students.csv")
        
        # From database
        df = extractor.from_database("SELECT * FROM enrollments")
        
        # From API
        df = extractor.from_api("https://api.example.com/data")
    """
    
    def __init__(self, settings: Optional[Settings] = None) -> None:
        """
        Initialize the extractor with settings.
        
        Args:
            settings: Application settings (uses global settings if None)
        """
        self.settings = settings or get_settings()
        self._file_extractor = FileExtractor(self.settings)
        self._db_extractor = DatabaseExtractor(self.settings)
        self._api_extractor = APIExtractor(self.settings)
    
    def from_csv(self, path: Union[str, Path], **kwargs: Any) -> pd.DataFrame:
        """Extract data from a CSV file."""
        return self._file_extractor.extract(str(path), **kwargs)
    
    def from_json(self, path: Union[str, Path], **kwargs: Any) -> pd.DataFrame:
        """Extract data from a JSON file."""
        return self._file_extractor.extract(str(path), **kwargs)
    
    def from_parquet(self, path: Union[str, Path], **kwargs: Any) -> pd.DataFrame:
        """Extract data from a Parquet file."""
        return self._file_extractor.extract(str(path), **kwargs)
    
    def from_excel(self, path: Union[str, Path], **kwargs: Any) -> pd.DataFrame:
        """Extract data from an Excel file."""
        return self._file_extractor.extract(str(path), **kwargs)
    
    def from_file(self, path: Union[str, Path], **kwargs: Any) -> pd.DataFrame:
        """Extract data from any supported file format."""
        return self._file_extractor.extract(str(path), **kwargs)
    
    def from_file_chunked(
        self,
        path: Union[str, Path],
        chunk_size: Optional[int] = None,
        **kwargs: Any
    ) -> Iterator[pd.DataFrame]:
        """Extract data from a file in chunks."""
        return self._file_extractor.extract_chunked(
            str(path), chunk_size, **kwargs
        )
    
    def from_database(self, query: str, **kwargs: Any) -> pd.DataFrame:
        """Extract data using a SQL query."""
        return self._db_extractor.extract(query, **kwargs)
    
    def from_table(self, table_name: str, **kwargs: Any) -> pd.DataFrame:
        """Extract entire table from database."""
        return self._db_extractor.extract_table(table_name, **kwargs)
    
    def from_api(
        self,
        url: str,
        method: str = "GET",
        **kwargs: Any
    ) -> pd.DataFrame:
        """Extract data from a REST API."""
        return self._api_extractor.extract(url, method, **kwargs)
    
    def from_s3(
        self,
        bucket: str,
        key: str,
        file_format: str = "csv",
        **kwargs: Any
    ) -> pd.DataFrame:
        """
        Extract data from AWS S3.
        
        Args:
            bucket: S3 bucket name
            key: Object key (path within bucket)
            file_format: File format (csv, json, parquet)
            **kwargs: Additional parameters
            
        Returns:
            pd.DataFrame: Extracted data
        """
        import boto3
        from io import BytesIO
        
        logger.info(f"Extracting from S3: s3://{bucket}/{key}")
        
        s3 = boto3.client(
            "s3",
            aws_access_key_id=self.settings.aws_access_key_id,
            aws_secret_access_key=self.settings.aws_secret_access_key,
            region_name=self.settings.aws_region,
        )
        
        obj = s3.get_object(Bucket=bucket, Key=key)
        data = BytesIO(obj["Body"].read())
        
        readers = {
            "csv": pd.read_csv,
            "json": pd.read_json,
            "parquet": pd.read_parquet,
        }
        
        if file_format not in readers:
            raise ValueError(f"Unsupported format: {file_format}")
        
        df = readers[file_format](data, **kwargs)
        logger.info(f"Extracted {len(df):,} rows from S3")
        
        return df
    
    # -------------------------------------------------------------------------
    # Live Data Source Methods
    # -------------------------------------------------------------------------
    
    def from_live_source(
        self,
        source_name: str,
        dataset: Optional[str] = None,
        store_raw: bool = True,
        **kwargs: Any
    ) -> pd.DataFrame:
        """
        Extract data from a registered live data source.
        
        Args:
            source_name: Name of the data source (e.g., "college_scorecard", "ipeds")
            dataset: Optional dataset name for storage
            store_raw: Whether to store in raw data lake
            **kwargs: Source-specific parameters
            
        Returns:
            pd.DataFrame: Extracted data
        """
        from higher_ed_data_pipeline.sources.base import DataSourceRegistry
        from higher_ed_data_pipeline.storage import DataLake
        
        # Get the data source
        source = DataSourceRegistry.get(source_name, self.settings)
        
        # Fetch data
        df = source.fetch(**kwargs)
        
        # Store in data lake if requested
        if store_raw and len(df) > 0:
            lake = DataLake(self.settings)
            dataset_name = dataset or f"extract_{source_name}"
            lake.store(df, source=source_name, dataset=dataset_name)
        
        return df
    
    def from_college_scorecard(
        self,
        fields: Optional[list[str]] = None,
        field_groups: Optional[list[str]] = None,
        state: Optional[str] = None,
        store_raw: bool = True,
        **kwargs: Any
    ) -> pd.DataFrame:
        """
        Extract data from College Scorecard API.
        
        Args:
            fields: Specific fields to retrieve
            field_groups: Predefined field groups
            state: Filter by state code
            store_raw: Store in raw data lake
            **kwargs: Additional filters
            
        Returns:
            pd.DataFrame: Institution data
        """
        from higher_ed_data_pipeline.sources import CollegeScorecardSource
        from higher_ed_data_pipeline.storage import DataLake
        
        source = CollegeScorecardSource(self.settings)
        df = source.fetch(
            fields=fields,
            field_groups=field_groups,
            state=state,
            **kwargs
        )
        
        if store_raw and len(df) > 0:
            lake = DataLake(self.settings)
            dataset_name = f"institutions_{state}" if state else "all_institutions"
            lake.store(df, source="college_scorecard", dataset=dataset_name)
        
        return df
    
    def from_ipeds(
        self,
        survey: str,
        year: Optional[int] = None,
        store_raw: bool = True,
        **kwargs: Any
    ) -> pd.DataFrame:
        """
        Extract data from IPEDS.
        
        Args:
            survey: IPEDS survey code (HD, IC, EF, SFA, GR, etc.)
            year: Data year
            store_raw: Store in raw data lake
            **kwargs: Additional parameters
            
        Returns:
            pd.DataFrame: Survey data
        """
        from higher_ed_data_pipeline.sources import IPEDSSource
        from higher_ed_data_pipeline.storage import DataLake
        
        source = IPEDSSource(self.settings)
        df = source.fetch(survey=survey, year=year, **kwargs)
        
        if store_raw and len(df) > 0:
            lake = DataLake(self.settings)
            year_str = str(year) if year else "latest"
            lake.store(df, source="ipeds", dataset=f"{survey.lower()}_{year_str}")
        
        return df
    
    def from_data_lake(
        self,
        source: str,
        dataset: str,
        version: Optional[str] = None,
    ) -> Optional[pd.DataFrame]:
        """
        Extract data from the raw data lake.
        
        Args:
            source: Data source name
            dataset: Dataset name
            version: Specific version timestamp (defaults to latest)
            
        Returns:
            pd.DataFrame or None
        """
        from higher_ed_data_pipeline.storage import DataLake
        
        lake = DataLake(self.settings)
        
        if version:
            return lake.load_version(source, dataset, version)
        else:
            return lake.load_latest(source, dataset)
    
    def list_available_sources(self) -> list[dict[str, Any]]:
        """
        List all available live data sources.
        
        Returns:
            List of source metadata
        """
        from higher_ed_data_pipeline.sources.base import DataSourceRegistry
        return DataSourceRegistry.list_sources()
    
    def list_data_lake_contents(self) -> list[dict[str, Any]]:
        """
        List all datasets in the raw data lake.
        
        Returns:
            List of dataset metadata
        """
        from higher_ed_data_pipeline.storage import DataLake
        lake = DataLake(self.settings)
        return lake.list_datasets()
