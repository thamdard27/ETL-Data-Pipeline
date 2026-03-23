"""
Transformer Module
==================

Handles data transformation operations including:
- Data cleaning and standardization
- Type conversions
- Aggregations and calculations
- Feature engineering
- Data validation

Design Patterns:
- Pipeline pattern for chaining transformations
- Strategy pattern for different transformation types

Usage:
    from higher_ed_data_pipeline.etl.transform import Transformer
    
    transformer = Transformer(settings)
    df = transformer.clean(df)
    df = transformer.standardize_columns(df)
"""

from abc import ABC, abstractmethod
from datetime import datetime
from typing import Any, Callable, Optional, Union

import numpy as np
import pandas as pd
from loguru import logger

from higher_ed_data_pipeline.config.settings import Settings, get_settings


class BaseTransformation(ABC):
    """Abstract base class for transformations."""
    
    @abstractmethod
    def apply(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Apply the transformation to the DataFrame.
        
        Args:
            df: Input DataFrame
            
        Returns:
            pd.DataFrame: Transformed DataFrame
        """
        pass


class ColumnStandardizer(BaseTransformation):
    """Standardizes column names to snake_case."""
    
    def apply(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Standardize column names to snake_case.
        
        Args:
            df: Input DataFrame
            
        Returns:
            pd.DataFrame: DataFrame with standardized column names
        """
        import re
        
        def to_snake_case(name: str) -> str:
            # Replace spaces and hyphens with underscores
            name = re.sub(r"[\s\-]+", "_", str(name))
            # Insert underscore before uppercase letters
            name = re.sub(r"([a-z])([A-Z])", r"\1_\2", name)
            # Convert to lowercase and remove special characters
            name = re.sub(r"[^a-zA-Z0-9_]", "", name.lower())
            # Remove consecutive underscores
            name = re.sub(r"_+", "_", name)
            # Strip leading/trailing underscores
            return name.strip("_")
        
        df.columns = [to_snake_case(col) for col in df.columns]
        return df


class DataCleaner(BaseTransformation):
    """Performs common data cleaning operations."""
    
    def __init__(
        self,
        drop_duplicates: bool = True,
        handle_missing: str = "keep",
        missing_threshold: float = 0.5,
    ) -> None:
        """
        Initialize the data cleaner.
        
        Args:
            drop_duplicates: Whether to drop duplicate rows
            handle_missing: How to handle missing values ('keep', 'drop', 'fill')
            missing_threshold: Drop columns with more than this fraction missing
        """
        self.drop_duplicates = drop_duplicates
        self.handle_missing = handle_missing
        self.missing_threshold = missing_threshold
    
    def apply(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Apply cleaning transformations.
        
        Args:
            df: Input DataFrame
            
        Returns:
            pd.DataFrame: Cleaned DataFrame
        """
        original_rows = len(df)
        original_cols = len(df.columns)
        
        # Drop columns with too many missing values
        missing_ratio = df.isnull().sum() / len(df)
        cols_to_drop = missing_ratio[missing_ratio > self.missing_threshold].index
        if len(cols_to_drop) > 0:
            logger.info(f"Dropping {len(cols_to_drop)} columns with >{self.missing_threshold*100}% missing")
            df = df.drop(columns=cols_to_drop)
        
        # Handle duplicates
        if self.drop_duplicates:
            before = len(df)
            df = df.drop_duplicates()
            dropped = before - len(df)
            if dropped > 0:
                logger.info(f"Dropped {dropped:,} duplicate rows")
        
        # Handle missing values
        if self.handle_missing == "drop":
            df = df.dropna()
        elif self.handle_missing == "fill":
            # Fill numeric columns with median, categorical with mode
            for col in df.columns:
                if df[col].dtype in ["int64", "float64"]:
                    df[col] = df[col].fillna(df[col].median())
                else:
                    mode = df[col].mode()
                    if len(mode) > 0:
                        df[col] = df[col].fillna(mode[0])
        
        logger.info(
            f"Cleaning complete: {original_rows:,} → {len(df):,} rows, "
            f"{original_cols} → {len(df.columns)} columns"
        )
        
        return df


class TypeConverter(BaseTransformation):
    """Converts column types based on specifications."""
    
    def __init__(self, type_mapping: dict[str, str]) -> None:
        """
        Initialize the type converter.
        
        Args:
            type_mapping: Dictionary mapping column names to target types
        """
        self.type_mapping = type_mapping
    
    def apply(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Apply type conversions.
        
        Args:
            df: Input DataFrame
            
        Returns:
            pd.DataFrame: DataFrame with converted types
        """
        for col, dtype in self.type_mapping.items():
            if col not in df.columns:
                logger.warning(f"Column '{col}' not found, skipping type conversion")
                continue
            
            try:
                if dtype == "datetime":
                    df[col] = pd.to_datetime(df[col], errors="coerce")
                elif dtype == "int":
                    df[col] = pd.to_numeric(df[col], errors="coerce").astype("Int64")
                elif dtype == "float":
                    df[col] = pd.to_numeric(df[col], errors="coerce")
                elif dtype == "bool":
                    df[col] = df[col].astype(bool)
                elif dtype == "str":
                    df[col] = df[col].astype(str)
                elif dtype == "category":
                    df[col] = df[col].astype("category")
                else:
                    df[col] = df[col].astype(dtype)
                    
                logger.debug(f"Converted '{col}' to {dtype}")
            except Exception as e:
                logger.error(f"Failed to convert '{col}' to {dtype}: {e}")
        
        return df


class Transformer:
    """
    Main transformer class for data transformation operations.
    
    Provides a fluent interface for chaining transformations and
    common data manipulation methods.
    
    Usage:
        transformer = Transformer(settings)
        
        # Chain transformations
        df = (transformer
              .standardize_columns(df)
              .clean(df)
              .convert_types(df, {"age": "int", "created_at": "datetime"})
              .add_derived_columns(df, {"full_name": lambda r: f"{r['first']} {r['last']}"}))
    """
    
    def __init__(self, settings: Optional[Settings] = None) -> None:
        """
        Initialize the transformer with settings.
        
        Args:
            settings: Application settings (uses global settings if None)
        """
        self.settings = settings or get_settings()
        self._transformations: list[BaseTransformation] = []
    
    def standardize_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Standardize column names to snake_case.
        
        Args:
            df: Input DataFrame
            
        Returns:
            pd.DataFrame: DataFrame with standardized column names
        """
        return ColumnStandardizer().apply(df)
    
    def clean(
        self,
        df: pd.DataFrame,
        drop_duplicates: bool = True,
        handle_missing: str = "keep",
        missing_threshold: float = 0.5,
    ) -> pd.DataFrame:
        """
        Clean the DataFrame by handling duplicates and missing values.
        
        Args:
            df: Input DataFrame
            drop_duplicates: Whether to drop duplicate rows
            handle_missing: How to handle missing values ('keep', 'drop', 'fill')
            missing_threshold: Drop columns with more than this fraction missing
            
        Returns:
            pd.DataFrame: Cleaned DataFrame
        """
        cleaner = DataCleaner(
            drop_duplicates=drop_duplicates,
            handle_missing=handle_missing,
            missing_threshold=missing_threshold,
        )
        return cleaner.apply(df)
    
    def convert_types(
        self,
        df: pd.DataFrame,
        type_mapping: dict[str, str],
    ) -> pd.DataFrame:
        """
        Convert column types according to mapping.
        
        Args:
            df: Input DataFrame
            type_mapping: Dictionary mapping column names to types
            
        Returns:
            pd.DataFrame: DataFrame with converted types
        """
        return TypeConverter(type_mapping).apply(df)
    
    def filter_rows(
        self,
        df: pd.DataFrame,
        condition: Union[str, Callable[[pd.DataFrame], pd.Series]],
    ) -> pd.DataFrame:
        """
        Filter rows based on a condition.
        
        Args:
            df: Input DataFrame
            condition: Either a query string or a function returning boolean Series
            
        Returns:
            pd.DataFrame: Filtered DataFrame
        """
        original_rows = len(df)
        
        if isinstance(condition, str):
            df = df.query(condition)
        else:
            mask = condition(df)
            df = df[mask]
        
        logger.info(f"Filtered: {original_rows:,} → {len(df):,} rows")
        return df
    
    def add_derived_columns(
        self,
        df: pd.DataFrame,
        columns: dict[str, Callable[[pd.Series], Any]],
    ) -> pd.DataFrame:
        """
        Add calculated columns to the DataFrame.
        
        Args:
            df: Input DataFrame
            columns: Dictionary mapping new column names to calculation functions
            
        Returns:
            pd.DataFrame: DataFrame with new columns
        """
        for col_name, func in columns.items():
            df[col_name] = df.apply(func, axis=1)
            logger.debug(f"Added derived column: {col_name}")
        
        return df
    
    def rename_columns(
        self,
        df: pd.DataFrame,
        mapping: dict[str, str],
    ) -> pd.DataFrame:
        """
        Rename columns according to mapping.
        
        Args:
            df: Input DataFrame
            mapping: Dictionary mapping old names to new names
            
        Returns:
            pd.DataFrame: DataFrame with renamed columns
        """
        return df.rename(columns=mapping)
    
    def select_columns(
        self,
        df: pd.DataFrame,
        columns: list[str],
    ) -> pd.DataFrame:
        """
        Select specific columns from DataFrame.
        
        Args:
            df: Input DataFrame
            columns: List of column names to keep
            
        Returns:
            pd.DataFrame: DataFrame with selected columns
        """
        missing = set(columns) - set(df.columns)
        if missing:
            logger.warning(f"Columns not found: {missing}")
            columns = [c for c in columns if c in df.columns]
        
        return df[columns]
    
    def aggregate(
        self,
        df: pd.DataFrame,
        group_by: Union[str, list[str]],
        aggregations: dict[str, Union[str, list[str]]],
    ) -> pd.DataFrame:
        """
        Aggregate data with grouping.
        
        Args:
            df: Input DataFrame
            group_by: Column(s) to group by
            aggregations: Dictionary mapping columns to aggregation functions
            
        Returns:
            pd.DataFrame: Aggregated DataFrame
        """
        result = df.groupby(group_by).agg(aggregations)
        
        # Flatten multi-level column names if present
        if isinstance(result.columns, pd.MultiIndex):
            result.columns = ["_".join(col).strip("_") for col in result.columns]
        
        return result.reset_index()
    
    def join(
        self,
        df: pd.DataFrame,
        other: pd.DataFrame,
        on: Union[str, list[str]],
        how: str = "left",
        suffixes: tuple[str, str] = ("", "_right"),
    ) -> pd.DataFrame:
        """
        Join two DataFrames.
        
        Args:
            df: Left DataFrame
            other: Right DataFrame
            on: Column(s) to join on
            how: Join type ('left', 'right', 'inner', 'outer')
            suffixes: Suffixes for overlapping column names
            
        Returns:
            pd.DataFrame: Joined DataFrame
        """
        return pd.merge(df, other, on=on, how=how, suffixes=suffixes)
    
    def validate(
        self,
        df: pd.DataFrame,
        schema: dict[str, Any],
    ) -> tuple[pd.DataFrame, pd.DataFrame]:
        """
        Validate DataFrame against a schema.
        
        Args:
            df: Input DataFrame
            schema: Validation schema with rules
            
        Returns:
            tuple: (valid_df, invalid_df)
        """
        valid_mask = pd.Series([True] * len(df), index=df.index)
        errors = []
        
        for col, rules in schema.items():
            if col not in df.columns:
                logger.warning(f"Validation column '{col}' not found")
                continue
            
            if "not_null" in rules and rules["not_null"]:
                null_mask = df[col].isnull()
                if null_mask.any():
                    valid_mask &= ~null_mask
                    errors.append(f"{col}: {null_mask.sum()} null values")
            
            if "min" in rules:
                min_mask = df[col] < rules["min"]
                if min_mask.any():
                    valid_mask &= ~min_mask
                    errors.append(f"{col}: {min_mask.sum()} below minimum")
            
            if "max" in rules:
                max_mask = df[col] > rules["max"]
                if max_mask.any():
                    valid_mask &= ~max_mask
                    errors.append(f"{col}: {max_mask.sum()} above maximum")
            
            if "allowed_values" in rules:
                allowed_mask = df[col].isin(rules["allowed_values"])
                if not allowed_mask.all():
                    valid_mask &= allowed_mask
                    errors.append(f"{col}: {(~allowed_mask).sum()} invalid values")
        
        if errors:
            logger.warning(f"Validation errors: {'; '.join(errors)}")
        
        valid_df = df[valid_mask]
        invalid_df = df[~valid_mask]
        
        logger.info(f"Validation: {len(valid_df):,} valid, {len(invalid_df):,} invalid")
        
        return valid_df, invalid_df
    
    def add_metadata(
        self,
        df: pd.DataFrame,
        source: Optional[str] = None,
        batch_id: Optional[str] = None,
    ) -> pd.DataFrame:
        """
        Add ETL metadata columns to DataFrame.
        
        Args:
            df: Input DataFrame
            source: Data source identifier
            batch_id: Processing batch identifier
            
        Returns:
            pd.DataFrame: DataFrame with metadata columns
        """
        df = df.copy()
        df["_etl_loaded_at"] = datetime.utcnow()
        
        if source:
            df["_etl_source"] = source
        if batch_id:
            df["_etl_batch_id"] = batch_id
        
        return df
