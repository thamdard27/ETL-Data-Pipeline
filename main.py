#!/usr/bin/env python3
"""
Higher Ed Data Pipeline - Main Entry Point
==========================================

Command-line interface for running ETL pipelines.

Usage:
    python main.py --help
    python main.py run --source data/raw/students.csv --dest data/processed/students.parquet
    python main.py run-config --config config/pipeline.yaml
"""

import sys
from pathlib import Path

import click
from loguru import logger

from higher_ed_data_pipeline import __version__
from higher_ed_data_pipeline.config.settings import Settings, get_settings
from higher_ed_data_pipeline.etl.pipeline import ETLPipeline
from higher_ed_data_pipeline.utils.logging import setup_logging


@click.group()
@click.version_option(version=__version__, prog_name="higher_ed_data_pipeline")
@click.option(
    "--debug/--no-debug",
    default=False,
    help="Enable debug mode with verbose logging"
)
@click.option(
    "--config",
    "-c",
    type=click.Path(exists=True),
    help="Path to configuration file"
)
@click.pass_context
def cli(ctx: click.Context, debug: bool, config: str) -> None:
    """
    Higher Ed Data Pipeline CLI
    
    A production-grade ETL pipeline for higher education data processing.
    """
    ctx.ensure_object(dict)
    
    # Initialize settings
    settings = get_settings()
    if debug:
        settings.debug = True
        settings.log_level = "DEBUG"
    
    # Setup logging
    setup_logging(log_level=settings.log_level)
    
    # Ensure directories exist
    settings.ensure_directories()
    
    ctx.obj["settings"] = settings
    ctx.obj["debug"] = debug


@cli.command()
@click.option(
    "--source", "-s",
    required=True,
    type=click.Path(exists=True),
    help="Source file path"
)
@click.option(
    "--dest", "-d",
    required=True,
    type=click.Path(),
    help="Destination file path"
)
@click.option(
    "--source-format",
    type=click.Choice(["csv", "json", "parquet", "excel"]),
    default="csv",
    help="Source file format"
)
@click.option(
    "--dest-format",
    type=click.Choice(["csv", "json", "parquet"]),
    default="parquet",
    help="Destination file format"
)
@click.option(
    "--clean/--no-clean",
    default=True,
    help="Apply data cleaning"
)
@click.option(
    "--standardize/--no-standardize",
    default=True,
    help="Standardize column names"
)
@click.pass_context
def run(
    ctx: click.Context,
    source: str,
    dest: str,
    source_format: str,
    dest_format: str,
    clean: bool,
    standardize: bool,
) -> None:
    """
    Run a simple ETL pipeline.
    
    Examples:
        python main.py run -s data/raw/students.csv -d data/processed/students.parquet
        python main.py run -s data.json --source-format json -d output.csv --dest-format csv
    """
    settings = ctx.obj["settings"]
    
    click.echo(f"Starting ETL pipeline...")
    click.echo(f"  Source: {source}")
    click.echo(f"  Destination: {dest}")
    
    pipeline = ETLPipeline(settings=settings, name="CLI Pipeline")
    
    result = pipeline.run_simple(
        source=source,
        destination=dest,
        source_format=source_format,
        destination_format=dest_format,
        clean=clean,
        standardize_columns=standardize,
    )
    
    if result.status.value == "success":
        click.secho(f"\n✓ Pipeline completed successfully!", fg="green")
        click.echo(f"  Rows processed: {result.rows_processed:,}")
        click.echo(f"  Rows output: {result.rows_output:,}")
        click.echo(f"  Duration: {result.duration_seconds:.2f}s")
        click.echo(f"  Output: {result.output_path}")
    else:
        click.secho(f"\n✗ Pipeline failed!", fg="red")
        click.echo(f"  Error: {result.error}")
        if ctx.obj["debug"]:
            click.echo(f"  Traceback:\n{result.error_traceback}")
        sys.exit(1)


@cli.command()
@click.pass_context
def info(ctx: click.Context) -> None:
    """
    Display pipeline information and settings.
    """
    settings = ctx.obj["settings"]
    
    click.echo(f"\nHigher Ed Data Pipeline v{__version__}")
    click.echo("=" * 40)
    click.echo(f"\nEnvironment: {settings.environment}")
    click.echo(f"Debug: {settings.debug}")
    click.echo(f"Log Level: {settings.log_level}")
    click.echo(f"\nPaths:")
    click.echo(f"  Raw Data: {settings.data_raw_path}")
    click.echo(f"  Staging: {settings.data_staging_path}")
    click.echo(f"  Processed: {settings.data_processed_path}")
    click.echo(f"  Logs: {settings.logs_path}")
    click.echo(f"\nProcessing:")
    click.echo(f"  Batch Size: {settings.batch_size:,}")
    click.echo(f"  Max Workers: {settings.max_workers}")


@cli.command()
@click.pass_context
def validate(ctx: click.Context) -> None:
    """
    Validate the pipeline configuration and dependencies.
    """
    settings = ctx.obj["settings"]
    
    click.echo("Validating pipeline configuration...\n")
    
    checks = []
    
    # Check directories
    for name, path in [
        ("Raw data", settings.data_raw_path),
        ("Staging", settings.data_staging_path),
        ("Processed", settings.data_processed_path),
        ("Logs", settings.logs_path),
    ]:
        exists = path.exists()
        status = click.style("✓", fg="green") if exists else click.style("✗", fg="red")
        click.echo(f"  {status} {name}: {path}")
        checks.append(exists)
    
    # Check optional configurations
    click.echo("\nOptional Configurations:")
    
    db_configured = bool(settings.database_url)
    status = click.style("✓", fg="green") if db_configured else click.style("-", fg="yellow")
    click.echo(f"  {status} Database: {'Configured' if db_configured else 'Not configured'}")
    
    aws_configured = bool(settings.aws_access_key_id)
    status = click.style("✓", fg="green") if aws_configured else click.style("-", fg="yellow")
    click.echo(f"  {status} AWS: {'Configured' if aws_configured else 'Not configured'}")
    
    gcp_configured = bool(settings.gcp_project_id)
    status = click.style("✓", fg="green") if gcp_configured else click.style("-", fg="yellow")
    click.echo(f"  {status} GCP: {'Configured' if gcp_configured else 'Not configured'}")
    
    # Summary
    if all(checks):
        click.secho("\n✓ All required configurations valid!", fg="green")
    else:
        click.secho("\n✗ Some configurations are missing!", fg="red")
        click.echo("Run with --debug for more information")
        sys.exit(1)


@cli.command()
@click.option(
    "--all", "-a", "init_all",
    is_flag=True,
    help="Initialize all directories and create sample files"
)
@click.pass_context
def init(ctx: click.Context, init_all: bool) -> None:
    """
    Initialize the pipeline directories and configuration.
    """
    settings = ctx.obj["settings"]
    
    click.echo("Initializing pipeline directories...")
    
    settings.ensure_directories()
    
    # Create .gitkeep files
    for path in [
        settings.data_raw_path,
        settings.data_staging_path,
        settings.data_processed_path,
        settings.logs_path,
    ]:
        gitkeep = path / ".gitkeep"
        if not gitkeep.exists():
            gitkeep.touch()
            click.echo(f"  Created: {gitkeep}")
    
    if init_all:
        # Create sample .env file
        env_file = settings.base_path / ".env.example"
        if not env_file.exists():
            env_content = """# Higher Ed Data Pipeline Configuration
# Copy this file to .env and update values

# Application
ENVIRONMENT=development
DEBUG=false
LOG_LEVEL=INFO

# Database (optional)
# DATABASE_URL=postgresql://user:password@localhost:5432/dbname

# AWS (optional)
# AWS_ACCESS_KEY_ID=your_access_key
# AWS_SECRET_ACCESS_KEY=your_secret_key
# AWS_REGION=us-east-1
# S3_BUCKET=your-bucket

# GCP (optional)
# GCP_PROJECT_ID=your-project-id
# GCS_BUCKET=your-bucket

# Processing
BATCH_SIZE=10000
MAX_WORKERS=4
"""
            env_file.write_text(env_content)
            click.echo(f"  Created: {env_file}")
    
    click.secho("\n✓ Initialization complete!", fg="green")


def main() -> None:
    """Main entry point."""
    cli(obj={})


if __name__ == "__main__":
    main()
