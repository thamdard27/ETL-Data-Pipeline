"""
Logging Utilities
=================

Centralized logging configuration using loguru.
Provides structured logging with file rotation and custom formatting.

Usage:
    from higher_ed_data_pipeline.utils.logging import setup_logging, get_logger
    
    setup_logging()
    logger = get_logger(__name__)
    logger.info("Processing started")
"""

import sys
from pathlib import Path
from typing import Optional

from loguru import logger

from higher_ed_data_pipeline.config.settings import get_settings


def setup_logging(
    log_level: Optional[str] = None,
    log_file: Optional[Path] = None,
    json_format: bool = False,
) -> None:
    """
    Configure application logging.
    
    Args:
        log_level: Override log level from settings
        log_file: Override log file path from settings
        json_format: Use JSON format for structured logging
    """
    settings = get_settings()
    
    # Remove default handler
    logger.remove()
    
    level = log_level or settings.log_level
    
    # Console handler with color
    if not json_format:
        logger.add(
            sys.stderr,
            level=level,
            format=(
                "<green>{time:YYYY-MM-DD HH:mm:ss}</green> | "
                "<level>{level: <8}</level> | "
                "<cyan>{name}</cyan>:<cyan>{function}</cyan>:<cyan>{line}</cyan> | "
                "<level>{message}</level>"
            ),
            colorize=True,
        )
    else:
        # JSON format for production
        logger.add(
            sys.stderr,
            level=level,
            serialize=True,
        )
    
    # File handler
    log_path = log_file or settings.logs_path / "app.log"
    log_path.parent.mkdir(parents=True, exist_ok=True)
    
    logger.add(
        log_path,
        level=level,
        rotation="10 MB",
        retention="30 days",
        compression="gz",
        format="{time:YYYY-MM-DD HH:mm:ss} | {level: <8} | {name}:{function}:{line} | {message}",
    )
    
    logger.info(f"Logging configured: level={level}, file={log_path}")


def get_logger(name: str):
    """
    Get a logger instance with context.
    
    Args:
        name: Logger name (typically __name__)
        
    Returns:
        Logger: Configured logger instance
    """
    return logger.bind(module=name)


class LoggerContextManager:
    """Context manager for adding context to log messages."""
    
    def __init__(self, **context):
        """
        Initialize with context variables.
        
        Args:
            **context: Key-value pairs to add to log context
        """
        self.context = context
        self._token = None
    
    def __enter__(self):
        """Add context on entry."""
        self._token = logger.configure(extra=self.context)
        return logger
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Remove context on exit."""
        pass


def log_context(**context):
    """
    Create a logging context manager.
    
    Args:
        **context: Context variables to add
        
    Returns:
        LoggerContextManager: Context manager
        
    Usage:
        with log_context(pipeline_id="abc123", batch=1):
            logger.info("Processing batch")
    """
    return LoggerContextManager(**context)
