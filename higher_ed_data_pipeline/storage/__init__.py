"""
Data Lake Module
================

Manages the raw data layer with timestamp-based organization.
Provides versioned storage, data catalog, and lineage tracking.

Architecture:
    data/raw/
    └── {source}/
        └── {dataset}_{timestamp}.parquet
        └── {dataset}_{timestamp}.metadata.json

Features:
- Automatic versioning with timestamps
- Metadata catalog for discovery
- Data lineage tracking
- Retention policies
- Deduplication support
"""

import json
import shutil
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Optional

import pandas as pd
from loguru import logger

from higher_ed_data_pipeline.config.settings import Settings, get_settings


class DataLake:
    """
    Data Lake manager for raw data storage.
    
    Treats raw storage as immutable - data is never updated, only new
    versions are created. This enables:
    - Complete audit trail
    - Point-in-time queries
    - Easy debugging and reprocessing
    
    Usage:
        lake = DataLake(settings)
        
        # Store new data
        path = lake.store(df, source="ipeds", dataset="enrollment")
        
        # Get latest version
        df = lake.load_latest(source="ipeds", dataset="enrollment")
        
        # Get specific version
        df = lake.load_version(source="ipeds", dataset="enrollment", timestamp="20240101_120000")
    """
    
    def __init__(self, settings: Optional[Settings] = None) -> None:
        """
        Initialize the data lake.
        
        Args:
            settings: Application settings
        """
        self.settings = settings or get_settings()
        self.raw_path = self.settings.data_raw_path
        self._ensure_structure()
    
    def _ensure_structure(self) -> None:
        """Ensure the data lake directory structure exists."""
        self.raw_path.mkdir(parents=True, exist_ok=True)
        
        # Create catalog file if not exists
        catalog_path = self.raw_path / "_catalog.json"
        if not catalog_path.exists():
            self._save_catalog({})
    
    def store(
        self,
        df: pd.DataFrame,
        source: str,
        dataset: str,
        metadata: Optional[dict[str, Any]] = None,
        file_format: str = "parquet",
    ) -> Path:
        """
        Store data in the raw layer with timestamp.
        
        Args:
            df: DataFrame to store
            source: Data source name
            dataset: Dataset name
            metadata: Additional metadata
            file_format: Storage format (parquet, csv)
            
        Returns:
            Path: Path to stored file
        """
        # Create source directory
        source_path = self.raw_path / source
        source_path.mkdir(parents=True, exist_ok=True)
        
        # Generate timestamped filename
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"{dataset}_{timestamp}.{file_format}"
        file_path = source_path / filename
        
        # Store data
        logger.info(f"Storing {len(df):,} rows to data lake: {file_path}")
        
        if file_format == "parquet":
            df.to_parquet(file_path, index=False)
        elif file_format == "csv":
            df.to_csv(file_path, index=False)
        else:
            raise ValueError(f"Unsupported format: {file_format}")
        
        # Create metadata
        file_metadata = {
            "source": source,
            "dataset": dataset,
            "timestamp": timestamp,
            "created_at": datetime.now().isoformat(),
            "row_count": len(df),
            "column_count": len(df.columns),
            "columns": list(df.columns),
            "dtypes": {col: str(dtype) for col, dtype in df.dtypes.items()},
            "file_size_bytes": file_path.stat().st_size,
            "file_format": file_format,
            "file_path": str(file_path),
            **(metadata or {}),
        }
        
        # Save metadata
        metadata_path = file_path.with_suffix(f".{file_format}.metadata.json")
        with open(metadata_path, "w") as f:
            json.dump(file_metadata, f, indent=2, default=str)
        
        # Update catalog
        self._update_catalog(source, dataset, timestamp, file_metadata)
        
        return file_path
    
    def load_latest(
        self,
        source: str,
        dataset: str,
    ) -> Optional[pd.DataFrame]:
        """
        Load the most recent version of a dataset.
        
        Args:
            source: Data source name
            dataset: Dataset name
            
        Returns:
            DataFrame or None if not found
        """
        latest_path = self.get_latest_path(source, dataset)
        
        if latest_path is None:
            logger.warning(f"No data found for {source}/{dataset}")
            return None
        
        return self._load_file(latest_path)
    
    def load_version(
        self,
        source: str,
        dataset: str,
        timestamp: str,
    ) -> Optional[pd.DataFrame]:
        """
        Load a specific version of a dataset.
        
        Args:
            source: Data source name
            dataset: Dataset name
            timestamp: Version timestamp (YYYYMMDD_HHMMSS)
            
        Returns:
            DataFrame or None if not found
        """
        source_path = self.raw_path / source
        
        # Find file matching pattern
        for ext in ["parquet", "csv"]:
            pattern = f"{dataset}_{timestamp}.{ext}"
            matches = list(source_path.glob(pattern))
            if matches:
                return self._load_file(matches[0])
        
        logger.warning(f"Version not found: {source}/{dataset}@{timestamp}")
        return None
    
    def _load_file(self, path: Path) -> pd.DataFrame:
        """Load a data file."""
        logger.info(f"Loading from data lake: {path}")
        
        if path.suffix == ".parquet":
            return pd.read_parquet(path)
        elif path.suffix == ".csv":
            return pd.read_csv(path)
        else:
            raise ValueError(f"Unknown file format: {path.suffix}")
    
    def get_latest_path(
        self,
        source: str,
        dataset: str,
    ) -> Optional[Path]:
        """
        Get the path to the latest version of a dataset.
        
        Args:
            source: Data source name
            dataset: Dataset name
            
        Returns:
            Path or None
        """
        source_path = self.raw_path / source
        if not source_path.exists():
            return None
        
        # Find all versions
        patterns = [f"{dataset}_*.parquet", f"{dataset}_*.csv"]
        files = []
        for pattern in patterns:
            files.extend(source_path.glob(pattern))
        
        # Filter out metadata files
        files = [f for f in files if not ".metadata." in str(f)]
        
        if not files:
            return None
        
        # Return most recent
        return max(files, key=lambda p: p.stem.split("_")[-1])
    
    def list_datasets(
        self,
        source: Optional[str] = None,
    ) -> list[dict[str, Any]]:
        """
        List available datasets in the data lake.
        
        Args:
            source: Filter by source (optional)
            
        Returns:
            List of dataset metadata
        """
        datasets = []
        
        sources = [source] if source else self.list_sources()
        
        for src in sources:
            source_path = self.raw_path / src
            if not source_path.exists():
                continue
            
            # Group files by dataset name
            seen_datasets = set()
            for file in source_path.glob("*.parquet"):
                if ".metadata." in str(file):
                    continue
                
                # Extract dataset name (everything before the timestamp)
                parts = file.stem.rsplit("_", 2)
                if len(parts) >= 3:
                    ds_name = "_".join(parts[:-2])
                else:
                    ds_name = parts[0]
                
                if ds_name not in seen_datasets:
                    seen_datasets.add(ds_name)
                    datasets.append({
                        "source": src,
                        "dataset": ds_name,
                        "versions": self.count_versions(src, ds_name),
                        "latest": self.get_latest_timestamp(src, ds_name),
                    })
        
        return datasets
    
    def list_sources(self) -> list[str]:
        """List all data sources in the lake."""
        sources = []
        for item in self.raw_path.iterdir():
            if item.is_dir() and not item.name.startswith("_"):
                sources.append(item.name)
        return sorted(sources)
    
    def list_versions(
        self,
        source: str,
        dataset: str,
    ) -> list[dict[str, Any]]:
        """
        List all versions of a dataset.
        
        Args:
            source: Data source name
            dataset: Dataset name
            
        Returns:
            List of version metadata
        """
        source_path = self.raw_path / source
        if not source_path.exists():
            return []
        
        versions = []
        for pattern in [f"{dataset}_*.parquet", f"{dataset}_*.csv"]:
            for file in source_path.glob(pattern):
                if ".metadata." in str(file):
                    continue
                
                # Extract timestamp
                parts = file.stem.rsplit("_", 2)
                if len(parts) >= 3:
                    timestamp = f"{parts[-2]}_{parts[-1]}"
                else:
                    continue
                
                # Load metadata if available
                metadata_path = file.with_suffix(f"{file.suffix}.metadata.json")
                metadata = {}
                if metadata_path.exists():
                    with open(metadata_path) as f:
                        metadata = json.load(f)
                
                versions.append({
                    "timestamp": timestamp,
                    "path": str(file),
                    "size_bytes": file.stat().st_size,
                    "row_count": metadata.get("row_count"),
                    "created_at": metadata.get("created_at"),
                })
        
        # Sort by timestamp descending
        return sorted(versions, key=lambda x: x["timestamp"], reverse=True)
    
    def count_versions(self, source: str, dataset: str) -> int:
        """Count versions of a dataset."""
        return len(self.list_versions(source, dataset))
    
    def get_latest_timestamp(
        self,
        source: str,
        dataset: str,
    ) -> Optional[str]:
        """Get the timestamp of the latest version."""
        versions = self.list_versions(source, dataset)
        return versions[0]["timestamp"] if versions else None
    
    def get_metadata(
        self,
        source: str,
        dataset: str,
        timestamp: Optional[str] = None,
    ) -> Optional[dict[str, Any]]:
        """
        Get metadata for a dataset version.
        
        Args:
            source: Data source name
            dataset: Dataset name
            timestamp: Version timestamp (defaults to latest)
            
        Returns:
            Metadata dictionary or None
        """
        if timestamp is None:
            path = self.get_latest_path(source, dataset)
        else:
            source_path = self.raw_path / source
            for ext in ["parquet", "csv"]:
                p = source_path / f"{dataset}_{timestamp}.{ext}"
                if p.exists():
                    path = p
                    break
            else:
                return None
        
        if path is None:
            return None
        
        metadata_path = path.with_suffix(f"{path.suffix}.metadata.json")
        if not metadata_path.exists():
            return None
        
        with open(metadata_path) as f:
            return json.load(f)
    
    def _get_catalog_path(self) -> Path:
        """Get the catalog file path."""
        return self.raw_path / "_catalog.json"
    
    def _load_catalog(self) -> dict[str, Any]:
        """Load the catalog."""
        catalog_path = self._get_catalog_path()
        if catalog_path.exists():
            with open(catalog_path) as f:
                return json.load(f)
        return {}
    
    def _save_catalog(self, catalog: dict[str, Any]) -> None:
        """Save the catalog."""
        with open(self._get_catalog_path(), "w") as f:
            json.dump(catalog, f, indent=2, default=str)
    
    def _update_catalog(
        self,
        source: str,
        dataset: str,
        timestamp: str,
        metadata: dict[str, Any],
    ) -> None:
        """Update the catalog with new dataset version."""
        catalog = self._load_catalog()
        
        key = f"{source}/{dataset}"
        if key not in catalog:
            catalog[key] = {
                "source": source,
                "dataset": dataset,
                "versions": [],
                "first_seen": datetime.now().isoformat(),
            }
        
        catalog[key]["versions"].append({
            "timestamp": timestamp,
            "row_count": metadata.get("row_count"),
            "created_at": datetime.now().isoformat(),
        })
        catalog[key]["latest_version"] = timestamp
        catalog[key]["latest_update"] = datetime.now().isoformat()
        
        # Keep only last 100 versions in catalog
        catalog[key]["versions"] = catalog[key]["versions"][-100:]
        
        self._save_catalog(catalog)
    
    def cleanup_old_versions(
        self,
        source: str,
        dataset: str,
        keep_versions: int = 10,
    ) -> int:
        """
        Remove old versions, keeping the N most recent.
        
        Args:
            source: Data source name
            dataset: Dataset name
            keep_versions: Number of versions to keep
            
        Returns:
            Number of versions removed
        """
        versions = self.list_versions(source, dataset)
        
        if len(versions) <= keep_versions:
            return 0
        
        to_remove = versions[keep_versions:]
        removed = 0
        
        for version in to_remove:
            path = Path(version["path"])
            metadata_path = path.with_suffix(f"{path.suffix}.metadata.json")
            
            try:
                path.unlink()
                if metadata_path.exists():
                    metadata_path.unlink()
                removed += 1
                logger.info(f"Removed old version: {path}")
            except Exception as e:
                logger.error(f"Failed to remove {path}: {e}")
        
        return removed
    
    def cleanup_by_age(
        self,
        max_age_days: int = 90,
    ) -> int:
        """
        Remove versions older than specified days.
        
        Args:
            max_age_days: Maximum age in days
            
        Returns:
            Number of versions removed
        """
        cutoff = datetime.now() - timedelta(days=max_age_days)
        cutoff_str = cutoff.strftime("%Y%m%d_%H%M%S")
        
        removed = 0
        
        for source in self.list_sources():
            source_path = self.raw_path / source
            for file in source_path.glob("*.parquet"):
                if ".metadata." in str(file):
                    continue
                
                # Extract timestamp from filename
                parts = file.stem.rsplit("_", 2)
                if len(parts) >= 3:
                    timestamp = f"{parts[-2]}_{parts[-1]}"
                    if timestamp < cutoff_str:
                        metadata_path = file.with_suffix(".parquet.metadata.json")
                        try:
                            file.unlink()
                            if metadata_path.exists():
                                metadata_path.unlink()
                            removed += 1
                        except Exception as e:
                            logger.error(f"Failed to remove {file}: {e}")
        
        logger.info(f"Cleanup complete: removed {removed} old versions")
        return removed
    
    def get_catalog_summary(self) -> dict[str, Any]:
        """Get a summary of the data lake contents."""
        catalog = self._load_catalog()
        
        total_datasets = len(catalog)
        total_versions = sum(
            len(entry.get("versions", []))
            for entry in catalog.values()
        )
        
        sources = set()
        for key in catalog:
            sources.add(key.split("/")[0])
        
        return {
            "total_sources": len(sources),
            "total_datasets": total_datasets,
            "total_versions": total_versions,
            "sources": list(sources),
            "latest_update": max(
                (entry.get("latest_update") for entry in catalog.values()),
                default=None
            ),
        }
