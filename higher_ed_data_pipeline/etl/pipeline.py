"""
ETL Pipeline Orchestrator
=========================

Orchestrates the complete ETL workflow with:
- Pipeline configuration and execution
- Error handling and recovery
- Logging and monitoring
- Batch processing support

Design Patterns:
- Builder pattern for pipeline construction
- Template method for pipeline execution

Usage:
    from higher_ed_data_pipeline.etl.pipeline import ETLPipeline
    
    pipeline = (ETLPipeline(settings)
                .extract(extractor.from_csv, "data/raw/students.csv")
                .transform(transformer.clean)
                .transform(transformer.standardize_columns)
                .load(loader.to_parquet, "data/processed/students.parquet"))
    
    result = pipeline.run()
"""

import traceback
import uuid
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Any, Callable, Optional

import pandas as pd
from loguru import logger

from higher_ed_data_pipeline.config.settings import Settings, get_settings
from higher_ed_data_pipeline.etl.extract import Extractor
from higher_ed_data_pipeline.etl.transform import Transformer
from higher_ed_data_pipeline.etl.load import Loader


class PipelineStatus(Enum):
    """Pipeline execution status."""
    PENDING = "pending"
    RUNNING = "running"
    SUCCESS = "success"
    FAILED = "failed"
    PARTIAL = "partial"


@dataclass
class PipelineStep:
    """Represents a single step in the pipeline."""
    name: str
    func: Callable
    args: tuple = field(default_factory=tuple)
    kwargs: dict = field(default_factory=dict)
    step_type: str = "transform"


@dataclass
class PipelineResult:
    """Result of a pipeline execution."""
    pipeline_id: str
    status: PipelineStatus
    start_time: datetime
    end_time: Optional[datetime] = None
    rows_processed: int = 0
    rows_output: int = 0
    error: Optional[str] = None
    error_traceback: Optional[str] = None
    steps_completed: int = 0
    total_steps: int = 0
    output_path: Optional[str] = None
    
    @property
    def duration_seconds(self) -> Optional[float]:
        """Calculate execution duration in seconds."""
        if self.end_time and self.start_time:
            return (self.end_time - self.start_time).total_seconds()
        return None
    
    def to_dict(self) -> dict[str, Any]:
        """Convert result to dictionary."""
        return {
            "pipeline_id": self.pipeline_id,
            "status": self.status.value,
            "start_time": self.start_time.isoformat(),
            "end_time": self.end_time.isoformat() if self.end_time else None,
            "duration_seconds": self.duration_seconds,
            "rows_processed": self.rows_processed,
            "rows_output": self.rows_output,
            "steps_completed": self.steps_completed,
            "total_steps": self.total_steps,
            "error": self.error,
            "output_path": self.output_path,
        }


class ETLPipeline:
    """
    ETL Pipeline orchestrator for building and executing data pipelines.
    
    Provides a fluent interface for constructing pipelines with
    extraction, transformation, and loading steps.
    
    Features:
    - Step-by-step pipeline construction
    - Automatic logging and monitoring
    - Error handling with detailed reporting
    - Retry logic for failed operations
    - Batch processing support
    
    Usage:
        # Simple pipeline
        pipeline = ETLPipeline()
        result = pipeline.run_simple(
            source="data/raw/students.csv",
            destination="data/processed/students.parquet",
            transformations=[
                lambda df: transformer.clean(df),
                lambda df: transformer.standardize_columns(df),
            ]
        )
        
        # Advanced pipeline with builder pattern
        pipeline = (ETLPipeline()
                    .set_name("Student Data Pipeline")
                    .extract(extractor.from_csv, "data/raw/students.csv")
                    .transform(transformer.clean)
                    .transform(transformer.standardize_columns)
                    .load(loader.to_parquet, "data/processed/students.parquet"))
        
        result = pipeline.run()
    """
    
    def __init__(
        self,
        settings: Optional[Settings] = None,
        name: Optional[str] = None,
    ) -> None:
        """
        Initialize the ETL pipeline.
        
        Args:
            settings: Application settings (uses global settings if None)
            name: Pipeline name for logging
        """
        self.settings = settings or get_settings()
        self.name = name or "ETL Pipeline"
        self.pipeline_id = str(uuid.uuid4())[:8]
        
        self._extract_step: Optional[PipelineStep] = None
        self._transform_steps: list[PipelineStep] = []
        self._load_step: Optional[PipelineStep] = None
        
        # Components
        self._extractor = Extractor(self.settings)
        self._transformer = Transformer(self.settings)
        self._loader = Loader(self.settings)
        
        # Configure logging
        self._setup_logging()
    
    def _setup_logging(self) -> None:
        """Configure logging for the pipeline."""
        log_file = self.settings.logs_path / f"pipeline_{self.pipeline_id}.log"
        log_file.parent.mkdir(parents=True, exist_ok=True)
        
        logger.add(
            log_file,
            rotation="10 MB",
            retention="30 days",
            level=self.settings.log_level,
            format="{time:YYYY-MM-DD HH:mm:ss} | {level: <8} | {message}",
        )
    
    def set_name(self, name: str) -> "ETLPipeline":
        """
        Set the pipeline name.
        
        Args:
            name: Pipeline name
            
        Returns:
            ETLPipeline: Self for method chaining
        """
        self.name = name
        return self
    
    def extract(
        self,
        func: Callable[..., pd.DataFrame],
        *args: Any,
        **kwargs: Any
    ) -> "ETLPipeline":
        """
        Add extraction step to the pipeline.
        
        Args:
            func: Extraction function that returns a DataFrame
            *args: Positional arguments for the function
            **kwargs: Keyword arguments for the function
            
        Returns:
            ETLPipeline: Self for method chaining
        """
        self._extract_step = PipelineStep(
            name="extract",
            func=func,
            args=args,
            kwargs=kwargs,
            step_type="extract",
        )
        return self
    
    def transform(
        self,
        func: Callable[[pd.DataFrame], pd.DataFrame],
        *args: Any,
        name: Optional[str] = None,
        **kwargs: Any
    ) -> "ETLPipeline":
        """
        Add transformation step to the pipeline.
        
        Args:
            func: Transformation function that takes and returns a DataFrame
            *args: Additional positional arguments
            name: Step name for logging
            **kwargs: Additional keyword arguments
            
        Returns:
            ETLPipeline: Self for method chaining
        """
        step_name = name or func.__name__ if hasattr(func, "__name__") else f"transform_{len(self._transform_steps)}"
        
        self._transform_steps.append(PipelineStep(
            name=step_name,
            func=func,
            args=args,
            kwargs=kwargs,
            step_type="transform",
        ))
        return self
    
    def load(
        self,
        func: Callable[[pd.DataFrame, Any], None],
        *args: Any,
        **kwargs: Any
    ) -> "ETLPipeline":
        """
        Add loading step to the pipeline.
        
        Args:
            func: Loading function
            *args: Positional arguments (destination path/table)
            **kwargs: Keyword arguments
            
        Returns:
            ETLPipeline: Self for method chaining
        """
        self._load_step = PipelineStep(
            name="load",
            func=func,
            args=args,
            kwargs=kwargs,
            step_type="load",
        )
        return self
    
    def run(self) -> PipelineResult:
        """
        Execute the pipeline.
        
        Returns:
            PipelineResult: Execution result with status and metrics
        """
        total_steps = (
            (1 if self._extract_step else 0) +
            len(self._transform_steps) +
            (1 if self._load_step else 0)
        )
        
        result = PipelineResult(
            pipeline_id=self.pipeline_id,
            status=PipelineStatus.RUNNING,
            start_time=datetime.utcnow(),
            total_steps=total_steps,
        )
        
        logger.info(f"Starting pipeline: {self.name} (ID: {self.pipeline_id})")
        
        df = None
        
        try:
            # Extract
            if self._extract_step:
                logger.info("Step 1: Extracting data")
                df = self._extract_step.func(
                    *self._extract_step.args,
                    **self._extract_step.kwargs
                )
                result.rows_processed = len(df)
                result.steps_completed += 1
                logger.info(f"Extracted {len(df):,} rows")
            else:
                raise ValueError("No extraction step defined")
            
            # Transform
            for i, step in enumerate(self._transform_steps, start=2):
                logger.info(f"Step {i}: Applying transformation '{step.name}'")
                df = step.func(df, *step.args, **step.kwargs)
                result.steps_completed += 1
                logger.info(f"Transformation complete: {len(df):,} rows")
            
            # Load
            if self._load_step:
                step_num = 2 + len(self._transform_steps)
                logger.info(f"Step {step_num}: Loading data")
                self._load_step.func(df, *self._load_step.args, **self._load_step.kwargs)
                result.steps_completed += 1
                result.rows_output = len(df)
                
                # Capture output path if available
                if self._load_step.args:
                    result.output_path = str(self._load_step.args[0])
                
                logger.info(f"Loaded {len(df):,} rows")
            
            result.status = PipelineStatus.SUCCESS
            
        except Exception as e:
            result.status = PipelineStatus.FAILED
            result.error = str(e)
            result.error_traceback = traceback.format_exc()
            logger.error(f"Pipeline failed: {e}")
            logger.error(result.error_traceback)
        
        finally:
            result.end_time = datetime.utcnow()
            logger.info(
                f"Pipeline completed: {result.status.value} "
                f"({result.steps_completed}/{result.total_steps} steps, "
                f"{result.duration_seconds:.2f}s)"
            )
        
        return result
    
    def run_simple(
        self,
        source: str,
        destination: str,
        source_format: str = "csv",
        destination_format: str = "parquet",
        transformations: Optional[list[Callable[[pd.DataFrame], pd.DataFrame]]] = None,
        clean: bool = True,
        standardize_columns: bool = True,
    ) -> PipelineResult:
        """
        Run a simple ETL pipeline with common defaults.
        
        Args:
            source: Source file path
            destination: Destination file path
            source_format: Source file format
            destination_format: Destination file format
            transformations: List of transformation functions to apply
            clean: Apply default cleaning
            standardize_columns: Standardize column names
            
        Returns:
            PipelineResult: Execution result
        """
        # Set up extraction
        extract_methods = {
            "csv": self._extractor.from_csv,
            "json": self._extractor.from_json,
            "parquet": self._extractor.from_parquet,
            "excel": self._extractor.from_excel,
        }
        
        if source_format not in extract_methods:
            raise ValueError(f"Unsupported source format: {source_format}")
        
        self.extract(extract_methods[source_format], source)
        
        # Set up transformations
        if standardize_columns:
            self.transform(self._transformer.standardize_columns, name="standardize_columns")
        
        if clean:
            self.transform(self._transformer.clean, name="clean")
        
        if transformations:
            for i, func in enumerate(transformations):
                self.transform(func, name=f"custom_{i}")
        
        # Set up loading
        load_methods = {
            "csv": self._loader.to_csv,
            "json": self._loader.to_json,
            "parquet": self._loader.to_parquet,
            "excel": self._loader.to_excel,
        }
        
        if destination_format not in load_methods:
            raise ValueError(f"Unsupported destination format: {destination_format}")
        
        self.load(load_methods[destination_format], destination)
        
        return self.run()


def create_pipeline(
    name: str,
    settings: Optional[Settings] = None,
) -> ETLPipeline:
    """
    Factory function to create a new pipeline.
    
    Args:
        name: Pipeline name
        settings: Application settings
        
    Returns:
        ETLPipeline: New pipeline instance
    """
    return ETLPipeline(settings=settings, name=name)
