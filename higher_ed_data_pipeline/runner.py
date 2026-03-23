"""
Automated ETL Runner
====================

Automated pipeline execution for live data sources.
Provides scheduled and on-demand ETL processing without manual intervention.

Features:
- Scheduled pipeline execution
- Multiple data source orchestration
- Automatic raw data lake storage
- Error handling and notifications
- Logging and monitoring

Usage:
    # Run all configured pipelines
    python -m higher_ed_data_pipeline.runner run-all
    
    # Run specific pipeline
    python -m higher_ed_data_pipeline.runner run --pipeline college_scorecard
    
    # Schedule recurring runs
    python -m higher_ed_data_pipeline.runner schedule --interval daily
"""

import json
import sys
from datetime import datetime
from pathlib import Path
from typing import Any, Callable, Optional

import click
from loguru import logger

from higher_ed_data_pipeline.config.settings import Settings, get_settings
from higher_ed_data_pipeline.etl.extract import Extractor
from higher_ed_data_pipeline.etl.transform import Transformer
from higher_ed_data_pipeline.etl.load import Loader
from higher_ed_data_pipeline.etl.pipeline import ETLPipeline, PipelineResult, PipelineStatus
from higher_ed_data_pipeline.storage import DataLake
from higher_ed_data_pipeline.utils.logging import setup_logging


class PipelineConfig:
    """Configuration for an automated pipeline."""
    
    def __init__(
        self,
        name: str,
        source: str,
        source_params: dict[str, Any],
        transformations: list[dict[str, Any]],
        output_dataset: str,
        enabled: bool = True,
        schedule: Optional[str] = None,
    ) -> None:
        """
        Initialize pipeline configuration.
        
        Args:
            name: Pipeline name
            source: Data source name
            source_params: Parameters for data source
            transformations: List of transformation configurations
            output_dataset: Name for processed output
            enabled: Whether pipeline is enabled
            schedule: Cron-like schedule string
        """
        self.name = name
        self.source = source
        self.source_params = source_params
        self.transformations = transformations
        self.output_dataset = output_dataset
        self.enabled = enabled
        self.schedule = schedule


class AutomatedRunner:
    """
    Automated ETL pipeline runner.
    
    Executes configured pipelines against live data sources,
    storing raw data in the data lake and processed data
    in the processed directory.
    """
    
    # Default pipeline configurations
    DEFAULT_PIPELINES = [
        {
            "name": "college_scorecard_full",
            "description": "Fetch all institutions from College Scorecard",
            "source": "college_scorecard",
            "source_params": {
                "field_groups": ["basic", "enrollment", "cost", "outcomes"],
            },
            "transformations": [
                {"type": "standardize_columns"},
                {"type": "clean", "params": {"handle_missing": "keep"}},
            ],
            "output_dataset": "institutions_processed",
            "enabled": True,
        },
        {
            "name": "ipeds_directory",
            "description": "Fetch IPEDS institutional directory",
            "source": "ipeds",
            "source_params": {
                "survey": "HD",
            },
            "transformations": [
                {"type": "standardize_columns"},
                {"type": "clean"},
            ],
            "output_dataset": "ipeds_directory_processed",
            "enabled": True,
        },
        {
            "name": "ipeds_enrollment",
            "description": "Fetch IPEDS enrollment data",
            "source": "ipeds",
            "source_params": {
                "survey": "EFFY",
            },
            "transformations": [
                {"type": "standardize_columns"},
                {"type": "clean"},
            ],
            "output_dataset": "ipeds_enrollment_processed",
            "enabled": True,
        },
        {
            "name": "ipeds_financial_aid",
            "description": "Fetch IPEDS student financial aid data",
            "source": "ipeds",
            "source_params": {
                "survey": "SFA",
            },
            "transformations": [
                {"type": "standardize_columns"},
                {"type": "clean"},
            ],
            "output_dataset": "ipeds_financial_aid_processed",
            "enabled": True,
        },
        {
            "name": "ipeds_graduation_rates",
            "description": "Fetch IPEDS graduation rates",
            "source": "ipeds",
            "source_params": {
                "survey": "GR",
            },
            "transformations": [
                {"type": "standardize_columns"},
                {"type": "clean"},
            ],
            "output_dataset": "ipeds_graduation_processed",
            "enabled": True,
        },
    ]
    
    def __init__(self, settings: Optional[Settings] = None) -> None:
        """
        Initialize the automated runner.
        
        Args:
            settings: Application settings
        """
        self.settings = settings or get_settings()
        self.extractor = Extractor(self.settings)
        self.transformer = Transformer(self.settings)
        self.loader = Loader(self.settings)
        self.lake = DataLake(self.settings)
        
        self._results: list[PipelineResult] = []
    
    def run_pipeline(
        self,
        config: dict[str, Any],
    ) -> PipelineResult:
        """
        Run a single pipeline from configuration.
        
        Args:
            config: Pipeline configuration dictionary
            
        Returns:
            PipelineResult: Execution result
        """
        name = config["name"]
        source = config["source"]
        source_params = config.get("source_params", {})
        transformations = config.get("transformations", [])
        output_dataset = config.get("output_dataset", name)
        
        logger.info(f"Starting pipeline: {name}")
        logger.info(f"  Source: {source}")
        logger.info(f"  Output: {output_dataset}")
        
        start_time = datetime.utcnow()
        result = PipelineResult(
            pipeline_id=name,
            status=PipelineStatus.RUNNING,
            start_time=start_time,
            total_steps=2 + len(transformations),
        )
        
        try:
            # Step 1: Extract from live source
            logger.info("Step 1: Extracting data from live source")
            df = self._extract_from_source(source, source_params)
            result.rows_processed = len(df)
            result.steps_completed = 1
            logger.info(f"Extracted {len(df):,} rows")
            
            # Step 2+: Apply transformations
            for i, transform in enumerate(transformations, start=2):
                transform_type = transform["type"]
                transform_params = transform.get("params", {})
                
                logger.info(f"Step {i}: Applying transformation '{transform_type}'")
                df = self._apply_transformation(df, transform_type, transform_params)
                result.steps_completed += 1
            
            # Final step: Load to processed
            logger.info(f"Step {result.total_steps}: Loading to processed storage")
            output_path = self.loader.to_processed(df, output_dataset)
            result.steps_completed += 1
            result.rows_output = len(df)
            result.output_path = str(output_path)
            
            result.status = PipelineStatus.SUCCESS
            logger.info(f"Pipeline '{name}' completed successfully")
            
        except Exception as e:
            import traceback
            result.status = PipelineStatus.FAILED
            result.error = str(e)
            result.error_traceback = traceback.format_exc()
            logger.error(f"Pipeline '{name}' failed: {e}")
        
        finally:
            result.end_time = datetime.utcnow()
        
        self._results.append(result)
        return result
    
    def _extract_from_source(
        self,
        source: str,
        params: dict[str, Any],
    ) -> "pd.DataFrame":
        """Extract data from a configured source."""
        import pandas as pd
        
        if source == "college_scorecard":
            return self.extractor.from_college_scorecard(
                store_raw=True,
                **params
            )
        elif source == "ipeds":
            return self.extractor.from_ipeds(
                store_raw=True,
                **params
            )
        else:
            return self.extractor.from_live_source(
                source,
                store_raw=True,
                **params
            )
    
    def _apply_transformation(
        self,
        df: "pd.DataFrame",
        transform_type: str,
        params: dict[str, Any],
    ) -> "pd.DataFrame":
        """Apply a transformation by type."""
        transformers = {
            "standardize_columns": self.transformer.standardize_columns,
            "clean": self.transformer.clean,
            "convert_types": self.transformer.convert_types,
            "filter_rows": self.transformer.filter_rows,
            "add_metadata": self.transformer.add_metadata,
        }
        
        if transform_type not in transformers:
            logger.warning(f"Unknown transformation type: {transform_type}")
            return df
        
        return transformers[transform_type](df, **params)
    
    def run_all(
        self,
        pipelines: Optional[list[dict[str, Any]]] = None,
        continue_on_error: bool = True,
    ) -> list[PipelineResult]:
        """
        Run all enabled pipelines.
        
        Args:
            pipelines: Pipeline configurations (defaults to DEFAULT_PIPELINES)
            continue_on_error: Continue running if a pipeline fails
            
        Returns:
            List of pipeline results
        """
        pipelines = pipelines or self.DEFAULT_PIPELINES
        enabled_pipelines = [p for p in pipelines if p.get("enabled", True)]
        
        logger.info(f"Running {len(enabled_pipelines)} pipelines")
        
        results = []
        for config in enabled_pipelines:
            try:
                result = self.run_pipeline(config)
                results.append(result)
            except Exception as e:
                logger.error(f"Pipeline {config['name']} failed: {e}")
                if not continue_on_error:
                    raise
        
        # Summary
        successful = sum(1 for r in results if r.status == PipelineStatus.SUCCESS)
        failed = sum(1 for r in results if r.status == PipelineStatus.FAILED)
        
        logger.info(f"Pipeline run complete: {successful} succeeded, {failed} failed")
        
        return results
    
    def run_by_name(self, name: str) -> Optional[PipelineResult]:
        """
        Run a specific pipeline by name.
        
        Args:
            name: Pipeline name
            
        Returns:
            Pipeline result or None if not found
        """
        for config in self.DEFAULT_PIPELINES:
            if config["name"] == name:
                return self.run_pipeline(config)
        
        logger.error(f"Pipeline not found: {name}")
        return None
    
    def get_results_summary(self) -> dict[str, Any]:
        """Get summary of all pipeline runs."""
        return {
            "total_runs": len(self._results),
            "successful": sum(1 for r in self._results if r.status == PipelineStatus.SUCCESS),
            "failed": sum(1 for r in self._results if r.status == PipelineStatus.FAILED),
            "total_rows_processed": sum(r.rows_processed for r in self._results),
            "total_rows_output": sum(r.rows_output for r in self._results),
            "results": [r.to_dict() for r in self._results],
        }
    
    def save_results(self, path: Optional[Path] = None) -> Path:
        """Save run results to file."""
        if path is None:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            path = self.settings.logs_path / f"pipeline_results_{timestamp}.json"
        
        with open(path, "w") as f:
            json.dump(self.get_results_summary(), f, indent=2, default=str)
        
        return path


# CLI for the runner
@click.group()
@click.option("--debug/--no-debug", default=False)
@click.pass_context
def cli(ctx: click.Context, debug: bool) -> None:
    """Automated ETL Pipeline Runner"""
    ctx.ensure_object(dict)
    
    settings = get_settings()
    if debug:
        settings.debug = True
        settings.log_level = "DEBUG"
    
    setup_logging(log_level=settings.log_level)
    settings.ensure_directories()
    
    ctx.obj["settings"] = settings
    ctx.obj["runner"] = AutomatedRunner(settings)


@cli.command()
@click.option("--continue-on-error/--stop-on-error", default=True)
@click.option("--save-results/--no-save-results", default=True)
@click.pass_context
def run_all(ctx: click.Context, continue_on_error: bool, save_results: bool) -> None:
    """Run all enabled pipelines."""
    runner: AutomatedRunner = ctx.obj["runner"]
    
    click.echo("Starting automated pipeline run...")
    click.echo(f"Running {len(runner.DEFAULT_PIPELINES)} configured pipelines\n")
    
    results = runner.run_all(continue_on_error=continue_on_error)
    
    # Display results
    click.echo("\n" + "=" * 60)
    click.echo("PIPELINE RUN SUMMARY")
    click.echo("=" * 60)
    
    for result in results:
        status_color = "green" if result.status == PipelineStatus.SUCCESS else "red"
        status_icon = "✓" if result.status == PipelineStatus.SUCCESS else "✗"
        
        click.echo(f"\n{click.style(status_icon, fg=status_color)} {result.pipeline_id}")
        click.echo(f"  Status: {result.status.value}")
        click.echo(f"  Rows: {result.rows_processed:,} → {result.rows_output:,}")
        click.echo(f"  Duration: {result.duration_seconds:.2f}s")
        
        if result.error:
            click.secho(f"  Error: {result.error}", fg="red")
    
    # Summary
    summary = runner.get_results_summary()
    click.echo("\n" + "-" * 60)
    click.echo(f"Total: {summary['successful']} succeeded, {summary['failed']} failed")
    click.echo(f"Rows processed: {summary['total_rows_processed']:,}")
    
    if save_results:
        results_path = runner.save_results()
        click.echo(f"\nResults saved to: {results_path}")


@cli.command()
@click.option("--pipeline", "-p", required=True, help="Pipeline name to run")
@click.pass_context
def run(ctx: click.Context, pipeline: str) -> None:
    """Run a specific pipeline by name."""
    runner: AutomatedRunner = ctx.obj["runner"]
    
    click.echo(f"Running pipeline: {pipeline}")
    
    result = runner.run_by_name(pipeline)
    
    if result is None:
        click.secho(f"Pipeline not found: {pipeline}", fg="red")
        click.echo("\nAvailable pipelines:")
        for p in runner.DEFAULT_PIPELINES:
            click.echo(f"  - {p['name']}")
        sys.exit(1)
    
    if result.status == PipelineStatus.SUCCESS:
        click.secho(f"\n✓ Pipeline completed successfully!", fg="green")
        click.echo(f"  Rows: {result.rows_processed:,} → {result.rows_output:,}")
        click.echo(f"  Duration: {result.duration_seconds:.2f}s")
        click.echo(f"  Output: {result.output_path}")
    else:
        click.secho(f"\n✗ Pipeline failed!", fg="red")
        click.echo(f"  Error: {result.error}")
        sys.exit(1)


@cli.command()
@click.pass_context
def list_pipelines(ctx: click.Context) -> None:
    """List all available pipelines."""
    runner: AutomatedRunner = ctx.obj["runner"]
    
    click.echo("Available Pipelines:")
    click.echo("=" * 60)
    
    for p in runner.DEFAULT_PIPELINES:
        status = click.style("enabled", fg="green") if p.get("enabled", True) else click.style("disabled", fg="yellow")
        click.echo(f"\n{p['name']} [{status}]")
        click.echo(f"  Description: {p.get('description', 'N/A')}")
        click.echo(f"  Source: {p['source']}")
        click.echo(f"  Output: {p.get('output_dataset', p['name'])}")


@cli.command()
@click.pass_context
def list_sources(ctx: click.Context) -> None:
    """List available data sources."""
    runner: AutomatedRunner = ctx.obj["runner"]
    
    sources = runner.extractor.list_available_sources()
    
    click.echo("Available Data Sources:")
    click.echo("=" * 60)
    
    for source in sources:
        api_key = click.style("[API Key Required]", fg="yellow") if source.get("requires_api_key") else ""
        click.echo(f"\n{source['name']} {api_key}")
        click.echo(f"  {source['description']}")
        click.echo(f"  URL: {source['url']}")


@cli.command()
@click.pass_context
def status(ctx: click.Context) -> None:
    """Show data lake status."""
    runner: AutomatedRunner = ctx.obj["runner"]
    
    summary = runner.lake.get_catalog_summary()
    datasets = runner.lake.list_datasets()
    
    click.echo("Data Lake Status:")
    click.echo("=" * 60)
    click.echo(f"\nSources: {summary['total_sources']}")
    click.echo(f"Datasets: {summary['total_datasets']}")
    click.echo(f"Total versions: {summary['total_versions']}")
    click.echo(f"Last update: {summary.get('latest_update', 'N/A')}")
    
    if datasets:
        click.echo("\nDatasets:")
        for ds in datasets:
            click.echo(f"  {ds['source']}/{ds['dataset']}: {ds['versions']} versions")


def main() -> None:
    """Main entry point."""
    cli(obj={})


if __name__ == "__main__":
    main()
