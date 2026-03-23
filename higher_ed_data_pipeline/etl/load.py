"""
Loader Module
=============

Handles data loading to various destinations including:
- Local files (CSV, JSON, Parquet)
- Databases (PostgreSQL, MySQL, SQLite)
- Cloud storage (S3, GCS, Azure Blob)
- Data warehouses

Design Patterns:
- Strategy pattern for different loading methods
- Factory pattern for destination-specific loaders

Usage:
    from higher_ed_data_pipeline.etl.load import Loader
    
    loader = Loader(settings)
    loader.to_csv(df, "output.csv")
    loader.to_database(df, "table_name")
"""

from abc import ABC, abstractmethod
from datetime import datetime
from pathlib import Path
from typing import Any, Optional, Union

import pandas as pd
from loguru import logger

from higher_ed_data_pipeline.config.settings import Settings, get_settings


class BaseLoader(ABC):
    """Abstract base class for all loaders."""
    
    @abstractmethod
    def load(self, df: pd.DataFrame, destination: str, **kwargs: Any) -> None:
        """
        Load data to the destination.
        
        Args:
            df: DataFrame to load
            destination: Target location identifier
            **kwargs: Additional loading parameters
        """
        pass


class FileLoader(BaseLoader):
    """Loader for local file destinations."""
    
    def __init__(self, settings: Settings) -> None:
        """
        Initialize the file loader.
        
        Args:
            settings: Application settings
        """
        self.settings = settings
    
    def load(
        self,
        df: pd.DataFrame,
        destination: str,
        file_format: Optional[str] = None,
        **kwargs: Any
    ) -> None:
        """
        Load data to a local file.
        
        Args:
            df: DataFrame to load
            destination: File path
            file_format: Override file format detection
            **kwargs: Additional parameters passed to pandas write functions
        """
        path = Path(destination)
        path.parent.mkdir(parents=True, exist_ok=True)
        
        # Determine format from extension or parameter
        suffix = file_format or path.suffix.lower().lstrip(".")
        
        logger.info(f"Loading {len(df):,} rows to {destination}")
        
        if suffix == "csv":
            df.to_csv(path, index=kwargs.pop("index", False), **kwargs)
        elif suffix == "json":
            df.to_json(
                path, 
                orient=kwargs.pop("orient", "records"),
                indent=kwargs.pop("indent", 2),
                **kwargs
            )
        elif suffix == "parquet":
            df.to_parquet(path, index=kwargs.pop("index", False), **kwargs)
        elif suffix in ("xlsx", "xls"):
            df.to_excel(path, index=kwargs.pop("index", False), **kwargs)
        else:
            raise ValueError(f"Unsupported file format: {suffix}")
        
        logger.info(f"Successfully saved to {destination}")


class DatabaseLoader(BaseLoader):
    """Loader for database destinations."""
    
    def __init__(self, settings: Settings) -> None:
        """
        Initialize the database loader.
        
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
    
    def load(
        self,
        df: pd.DataFrame,
        destination: str,
        if_exists: str = "append",
        chunk_size: Optional[int] = None,
        **kwargs: Any
    ) -> None:
        """
        Load data to a database table.
        
        Args:
            df: DataFrame to load
            destination: Table name
            if_exists: How to handle existing table ('fail', 'replace', 'append')
            chunk_size: Number of rows per insert batch
            **kwargs: Additional parameters passed to pandas to_sql
        """
        chunk_size = chunk_size or self.settings.batch_size
        
        logger.info(
            f"Loading {len(df):,} rows to table '{destination}' "
            f"(if_exists={if_exists})"
        )
        
        df.to_sql(
            destination,
            self.engine,
            if_exists=if_exists,
            index=kwargs.pop("index", False),
            chunksize=chunk_size,
            method=kwargs.pop("method", "multi"),
            **kwargs
        )
        
        logger.info(f"Successfully loaded to table '{destination}'")
    
    def upsert(
        self,
        df: pd.DataFrame,
        table_name: str,
        primary_key: Union[str, list[str]],
        **kwargs: Any
    ) -> None:
        """
        Upsert data to a database table (insert or update).
        
        Args:
            df: DataFrame to load
            table_name: Table name
            primary_key: Primary key column(s)
            **kwargs: Additional parameters
        """
        from sqlalchemy import text
        from sqlalchemy.dialects.postgresql import insert
        
        if isinstance(primary_key, str):
            primary_key = [primary_key]
        
        logger.info(f"Upserting {len(df):,} rows to table '{table_name}'")
        
        # For PostgreSQL, use ON CONFLICT
        records = df.to_dict(orient="records")
        
        with self.engine.begin() as conn:
            for record in records:
                # Build insert statement with ON CONFLICT
                columns = list(record.keys())
                values = list(record.values())
                
                placeholders = ", ".join([f":{col}" for col in columns])
                column_list = ", ".join(columns)
                update_cols = ", ".join([
                    f"{col} = EXCLUDED.{col}" 
                    for col in columns 
                    if col not in primary_key
                ])
                pk_list = ", ".join(primary_key)
                
                sql = f"""
                    INSERT INTO {table_name} ({column_list})
                    VALUES ({placeholders})
                    ON CONFLICT ({pk_list})
                    DO UPDATE SET {update_cols}
                """
                
                conn.execute(text(sql), record)
        
        logger.info(f"Successfully upserted to table '{table_name}'")


class CloudLoader(BaseLoader):
    """Loader for cloud storage destinations."""
    
    def __init__(self, settings: Settings) -> None:
        """
        Initialize the cloud loader.
        
        Args:
            settings: Application settings
        """
        self.settings = settings
    
    def load(
        self,
        df: pd.DataFrame,
        destination: str,
        **kwargs: Any
    ) -> None:
        """
        Load data to cloud storage.
        
        Args:
            df: DataFrame to load
            destination: Cloud storage path (s3://bucket/key, gs://bucket/key)
            **kwargs: Additional parameters
        """
        if destination.startswith("s3://"):
            self._load_to_s3(df, destination, **kwargs)
        elif destination.startswith("gs://"):
            self._load_to_gcs(df, destination, **kwargs)
        else:
            raise ValueError(f"Unsupported cloud destination: {destination}")
    
    def _load_to_s3(
        self,
        df: pd.DataFrame,
        destination: str,
        file_format: str = "parquet",
        **kwargs: Any
    ) -> None:
        """
        Load data to AWS S3.
        
        Args:
            df: DataFrame to load
            destination: S3 path (s3://bucket/key)
            file_format: Output format (csv, json, parquet)
            **kwargs: Additional parameters
        """
        import boto3
        from io import BytesIO
        
        # Parse S3 path
        path = destination.replace("s3://", "")
        bucket = path.split("/")[0]
        key = "/".join(path.split("/")[1:])
        
        logger.info(f"Loading {len(df):,} rows to {destination}")
        
        buffer = BytesIO()
        
        if file_format == "csv":
            df.to_csv(buffer, index=False)
            content_type = "text/csv"
        elif file_format == "json":
            df.to_json(buffer, orient="records")
            content_type = "application/json"
        elif file_format == "parquet":
            df.to_parquet(buffer, index=False)
            content_type = "application/octet-stream"
        else:
            raise ValueError(f"Unsupported format: {file_format}")
        
        buffer.seek(0)
        
        s3 = boto3.client(
            "s3",
            aws_access_key_id=self.settings.aws_access_key_id,
            aws_secret_access_key=self.settings.aws_secret_access_key,
            region_name=self.settings.aws_region,
        )
        
        s3.upload_fileobj(
            buffer,
            bucket,
            key,
            ExtraArgs={"ContentType": content_type}
        )
        
        logger.info(f"Successfully uploaded to {destination}")
    
    def _load_to_gcs(
        self,
        df: pd.DataFrame,
        destination: str,
        file_format: str = "parquet",
        **kwargs: Any
    ) -> None:
        """
        Load data to Google Cloud Storage.
        
        Args:
            df: DataFrame to load
            destination: GCS path (gs://bucket/key)
            file_format: Output format (csv, json, parquet)
            **kwargs: Additional parameters
        """
        from google.cloud import storage
        from io import BytesIO
        
        # Parse GCS path
        path = destination.replace("gs://", "")
        bucket_name = path.split("/")[0]
        blob_name = "/".join(path.split("/")[1:])
        
        logger.info(f"Loading {len(df):,} rows to {destination}")
        
        buffer = BytesIO()
        
        if file_format == "csv":
            df.to_csv(buffer, index=False)
            content_type = "text/csv"
        elif file_format == "json":
            df.to_json(buffer, orient="records")
            content_type = "application/json"
        elif file_format == "parquet":
            df.to_parquet(buffer, index=False)
            content_type = "application/octet-stream"
        else:
            raise ValueError(f"Unsupported format: {file_format}")
        
        buffer.seek(0)
        
        client = storage.Client(project=self.settings.gcp_project_id)
        bucket = client.bucket(bucket_name)
        blob = bucket.blob(blob_name)
        
        blob.upload_from_file(buffer, content_type=content_type)
        
        logger.info(f"Successfully uploaded to {destination}")


class Loader:
    """
    Main loader class that delegates to specialized loaders.
    
    Provides a unified interface for loading data to various
    destinations, abstracting away the complexity of different
    storage systems.
    
    Usage:
        loader = Loader(settings)
        
        # To CSV file
        loader.to_csv(df, "data/processed/output.csv")
        
        # To database
        loader.to_database(df, "students", if_exists="append")
        
        # To S3
        loader.to_s3(df, "my-bucket", "data/output.parquet")
    """
    
    def __init__(self, settings: Optional[Settings] = None) -> None:
        """
        Initialize the loader with settings.
        
        Args:
            settings: Application settings (uses global settings if None)
        """
        self.settings = settings or get_settings()
        self._file_loader = FileLoader(self.settings)
        self._db_loader = DatabaseLoader(self.settings)
        self._cloud_loader = CloudLoader(self.settings)
    
    def to_csv(self, df: pd.DataFrame, path: Union[str, Path], **kwargs: Any) -> None:
        """Save DataFrame to CSV file."""
        self._file_loader.load(df, str(path), file_format="csv", **kwargs)
    
    def to_json(self, df: pd.DataFrame, path: Union[str, Path], **kwargs: Any) -> None:
        """Save DataFrame to JSON file."""
        self._file_loader.load(df, str(path), file_format="json", **kwargs)
    
    def to_parquet(self, df: pd.DataFrame, path: Union[str, Path], **kwargs: Any) -> None:
        """Save DataFrame to Parquet file."""
        self._file_loader.load(df, str(path), file_format="parquet", **kwargs)
    
    def to_excel(self, df: pd.DataFrame, path: Union[str, Path], **kwargs: Any) -> None:
        """Save DataFrame to Excel file."""
        self._file_loader.load(df, str(path), file_format="xlsx", **kwargs)
    
    def to_file(self, df: pd.DataFrame, path: Union[str, Path], **kwargs: Any) -> None:
        """Save DataFrame to file (format detected from extension)."""
        self._file_loader.load(df, str(path), **kwargs)
    
    def to_database(
        self,
        df: pd.DataFrame,
        table_name: str,
        if_exists: str = "append",
        **kwargs: Any
    ) -> None:
        """Save DataFrame to database table."""
        self._db_loader.load(df, table_name, if_exists=if_exists, **kwargs)
    
    def to_database_upsert(
        self,
        df: pd.DataFrame,
        table_name: str,
        primary_key: Union[str, list[str]],
        **kwargs: Any
    ) -> None:
        """Upsert DataFrame to database table."""
        self._db_loader.upsert(df, table_name, primary_key, **kwargs)
    
    def to_s3(
        self,
        df: pd.DataFrame,
        bucket: str,
        key: str,
        file_format: str = "parquet",
        **kwargs: Any
    ) -> None:
        """
        Save DataFrame to S3.
        
        Args:
            df: DataFrame to save
            bucket: S3 bucket name
            key: Object key (path within bucket)
            file_format: Output format (csv, json, parquet)
            **kwargs: Additional parameters
        """
        destination = f"s3://{bucket}/{key}"
        self._cloud_loader.load(df, destination, file_format=file_format, **kwargs)
    
    def to_gcs(
        self,
        df: pd.DataFrame,
        bucket: str,
        blob_name: str,
        file_format: str = "parquet",
        **kwargs: Any
    ) -> None:
        """
        Save DataFrame to Google Cloud Storage.
        
        Args:
            df: DataFrame to save
            bucket: GCS bucket name
            blob_name: Object name (path within bucket)
            file_format: Output format (csv, json, parquet)
            **kwargs: Additional parameters
        """
        destination = f"gs://{bucket}/{blob_name}"
        self._cloud_loader.load(df, destination, file_format=file_format, **kwargs)
    
    def to_staging(
        self,
        df: pd.DataFrame,
        name: str,
        file_format: str = "parquet",
        **kwargs: Any
    ) -> Path:
        """
        Save DataFrame to staging directory.
        
        Args:
            df: DataFrame to save
            name: File name (without extension)
            file_format: Output format
            **kwargs: Additional parameters
            
        Returns:
            Path: Path to saved file
        """
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"{name}_{timestamp}.{file_format}"
        path = self.settings.data_staging_path / filename
        
        self._file_loader.load(df, str(path), file_format=file_format, **kwargs)
        return path
    
    def to_processed(
        self,
        df: pd.DataFrame,
        name: str,
        file_format: str = "parquet",
        **kwargs: Any
    ) -> Path:
        """
        Save DataFrame to processed directory.
        
        Args:
            df: DataFrame to save
            name: File name (without extension)
            file_format: Output format
            **kwargs: Additional parameters
            
        Returns:
            Path: Path to saved file
        """
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"{name}_{timestamp}.{file_format}"
        path = self.settings.data_processed_path / filename
        
        self._file_loader.load(df, str(path), file_format=file_format, **kwargs)
        return path
