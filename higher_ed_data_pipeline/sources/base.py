"""
Base Data Source
================

Abstract base class and registry for all live data sources.
Provides common functionality for data fetching, caching, and storage.

Design Patterns:
- Template Method for extraction workflow
- Registry pattern for source discovery
- Strategy pattern for different source types
"""

from abc import ABC, abstractmethod
from datetime import datetime
from pathlib import Path
from typing import Any, Optional, Type

import pandas as pd
from loguru import logger

from higher_ed_data_pipeline.config.settings import Settings, get_settings


class BaseDataSource(ABC):
    """
    Abstract base class for all live data sources.
    
    Provides common functionality:
    - Data fetching with retry logic
    - Raw data storage with timestamps
    - Metadata tracking
    - Rate limiting support
    """
    
    # Source metadata - override in subclasses
    SOURCE_NAME: str = "base"
    SOURCE_DESCRIPTION: str = "Base data source"
    SOURCE_URL: str = ""
    REQUIRES_API_KEY: bool = False
    
    def __init__(self, settings: Optional[Settings] = None) -> None:
        """
        Initialize the data source.
        
        Args:
            settings: Application settings
        """
        self.settings = settings or get_settings()
        self._last_fetch: Optional[datetime] = None
        self._fetch_count: int = 0
    
    @abstractmethod
    def fetch(self, **kwargs: Any) -> pd.DataFrame:
        """
        Fetch data from the live source.
        
        Args:
            **kwargs: Source-specific parameters
            
        Returns:
            pd.DataFrame: Fetched data
        """
        pass
    
    @abstractmethod
    def get_available_datasets(self) -> list[dict[str, Any]]:
        """
        List available datasets from this source.
        
        Returns:
            list: Available dataset metadata
        """
        pass
    
    def fetch_and_store(
        self,
        dataset_name: str,
        **kwargs: Any
    ) -> tuple[pd.DataFrame, Path]:
        """
        Fetch data and store in raw data lake layer.
        
        Args:
            dataset_name: Name for the dataset
            **kwargs: Source-specific parameters
            
        Returns:
            tuple: (DataFrame, path to stored file)
        """
        logger.info(f"Fetching {dataset_name} from {self.SOURCE_NAME}")
        
        # Fetch the data
        df = self.fetch(**kwargs)
        
        # Generate timestamped filename
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"{self.SOURCE_NAME}_{dataset_name}_{timestamp}.parquet"
        
        # Store in raw data lake
        raw_path = self.settings.data_raw_path / self.SOURCE_NAME
        raw_path.mkdir(parents=True, exist_ok=True)
        
        file_path = raw_path / filename
        df.to_parquet(file_path, index=False)
        
        # Also store metadata
        self._store_metadata(file_path, df, dataset_name, kwargs)
        
        logger.info(f"Stored {len(df):,} rows to {file_path}")
        
        self._last_fetch = datetime.now()
        self._fetch_count += 1
        
        return df, file_path
    
    def _store_metadata(
        self,
        file_path: Path,
        df: pd.DataFrame,
        dataset_name: str,
        params: dict[str, Any],
    ) -> None:
        """Store metadata alongside the raw data file."""
        import json
        
        metadata = {
            "source": self.SOURCE_NAME,
            "source_url": self.SOURCE_URL,
            "dataset": dataset_name,
            "fetch_timestamp": datetime.now().isoformat(),
            "row_count": len(df),
            "column_count": len(df.columns),
            "columns": list(df.columns),
            "dtypes": {col: str(dtype) for col, dtype in df.dtypes.items()},
            "parameters": params,
            "file_path": str(file_path),
            "file_size_bytes": file_path.stat().st_size if file_path.exists() else 0,
        }
        
        metadata_path = file_path.with_suffix(".metadata.json")
        with open(metadata_path, "w") as f:
            json.dump(metadata, f, indent=2, default=str)
    
    def get_latest_raw_file(self, dataset_name: str) -> Optional[Path]:
        """
        Get the most recent raw file for a dataset.
        
        Args:
            dataset_name: Name of the dataset
            
        Returns:
            Path to latest file or None
        """
        raw_path = self.settings.data_raw_path / self.SOURCE_NAME
        if not raw_path.exists():
            return None
        
        pattern = f"{self.SOURCE_NAME}_{dataset_name}_*.parquet"
        files = sorted(raw_path.glob(pattern), reverse=True)
        
        return files[0] if files else None
    
    def list_raw_files(self, dataset_name: Optional[str] = None) -> list[Path]:
        """
        List all raw files from this source.
        
        Args:
            dataset_name: Optional filter by dataset name
            
        Returns:
            list: Paths to raw files
        """
        raw_path = self.settings.data_raw_path / self.SOURCE_NAME
        if not raw_path.exists():
            return []
        
        if dataset_name:
            pattern = f"{self.SOURCE_NAME}_{dataset_name}_*.parquet"
        else:
            pattern = "*.parquet"
        
        return sorted(raw_path.glob(pattern))
    
    @property
    def info(self) -> dict[str, Any]:
        """Get source information."""
        return {
            "name": self.SOURCE_NAME,
            "description": self.SOURCE_DESCRIPTION,
            "url": self.SOURCE_URL,
            "requires_api_key": self.REQUIRES_API_KEY,
            "last_fetch": self._last_fetch.isoformat() if self._last_fetch else None,
            "fetch_count": self._fetch_count,
        }


class DataSourceRegistry:
    """
    Registry for available data sources.
    
    Provides discovery and instantiation of data sources.
    """
    
    _sources: dict[str, Type[BaseDataSource]] = {}
    
    @classmethod
    def register(cls, source_class: Type[BaseDataSource]) -> Type[BaseDataSource]:
        """
        Register a data source class.
        
        Args:
            source_class: Data source class to register
            
        Returns:
            The registered class (for use as decorator)
        """
        cls._sources[source_class.SOURCE_NAME] = source_class
        return source_class
    
    @classmethod
    def get(cls, name: str, settings: Optional[Settings] = None) -> BaseDataSource:
        """
        Get an instance of a registered data source.
        
        Args:
            name: Source name
            settings: Optional settings
            
        Returns:
            Data source instance
            
        Raises:
            KeyError: If source not found
        """
        if name not in cls._sources:
            available = ", ".join(cls._sources.keys())
            raise KeyError(f"Unknown data source: {name}. Available: {available}")
        
        return cls._sources[name](settings)
    
    @classmethod
    def list_sources(cls) -> list[dict[str, Any]]:
        """
        List all registered data sources.
        
        Returns:
            list: Source metadata
        """
        return [
            {
                "name": source_class.SOURCE_NAME,
                "description": source_class.SOURCE_DESCRIPTION,
                "url": source_class.SOURCE_URL,
                "requires_api_key": source_class.REQUIRES_API_KEY,
            }
            for source_class in cls._sources.values()
        ]
    
    @classmethod
    def available_sources(cls) -> list[str]:
        """Get list of available source names."""
        return list(cls._sources.keys())
