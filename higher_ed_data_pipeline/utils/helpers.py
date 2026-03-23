"""
Helper Utilities
================

Common helper functions and decorators for the ETL pipeline.

Features:
- Retry decorator with exponential backoff
- Timing decorator for performance monitoring
- Data manipulation utilities
"""

import functools
import time
from typing import Any, Callable, Iterator, Optional, TypeVar

from loguru import logger

T = TypeVar("T")


def retry(
    max_attempts: int = 3,
    delay: float = 1.0,
    backoff: float = 2.0,
    exceptions: tuple = (Exception,),
) -> Callable:
    """
    Decorator for retrying a function with exponential backoff.
    
    Args:
        max_attempts: Maximum number of attempts
        delay: Initial delay between retries in seconds
        backoff: Multiplier for delay after each attempt
        exceptions: Tuple of exceptions to catch and retry
        
    Returns:
        Callable: Decorated function
        
    Usage:
        @retry(max_attempts=3, delay=1.0)
        def fetch_data():
            response = requests.get(url)
            response.raise_for_status()
            return response.json()
    """
    def decorator(func: Callable[..., T]) -> Callable[..., T]:
        @functools.wraps(func)
        def wrapper(*args, **kwargs) -> T:
            current_delay = delay
            last_exception = None
            
            for attempt in range(1, max_attempts + 1):
                try:
                    return func(*args, **kwargs)
                except exceptions as e:
                    last_exception = e
                    if attempt < max_attempts:
                        logger.warning(
                            f"Attempt {attempt}/{max_attempts} failed: {e}. "
                            f"Retrying in {current_delay:.1f}s..."
                        )
                        time.sleep(current_delay)
                        current_delay *= backoff
                    else:
                        logger.error(f"All {max_attempts} attempts failed")
            
            raise last_exception
        
        return wrapper
    return decorator


def timer(func: Callable[..., T]) -> Callable[..., T]:
    """
    Decorator for timing function execution.
    
    Args:
        func: Function to time
        
    Returns:
        Callable: Decorated function that logs execution time
        
    Usage:
        @timer
        def process_data(df):
            # ... processing logic
            return result
    """
    @functools.wraps(func)
    def wrapper(*args, **kwargs) -> T:
        start_time = time.perf_counter()
        
        try:
            result = func(*args, **kwargs)
            return result
        finally:
            elapsed = time.perf_counter() - start_time
            logger.debug(f"{func.__name__} executed in {elapsed:.3f}s")
    
    return wrapper


def chunked(iterable: list, chunk_size: int) -> Iterator[list]:
    """
    Split an iterable into chunks of specified size.
    
    Args:
        iterable: List to split
        chunk_size: Size of each chunk
        
    Yields:
        list: Chunks of the original list
        
    Usage:
        for batch in chunked(large_list, 1000):
            process_batch(batch)
    """
    for i in range(0, len(iterable), chunk_size):
        yield iterable[i:i + chunk_size]


def flatten(nested_list: list[list[T]]) -> list[T]:
    """
    Flatten a nested list one level.
    
    Args:
        nested_list: List of lists
        
    Returns:
        list: Flattened list
        
    Usage:
        flat = flatten([[1, 2], [3, 4], [5]])
        # [1, 2, 3, 4, 5]
    """
    return [item for sublist in nested_list for item in sublist]


def safe_get(
    data: dict,
    path: str,
    default: Any = None,
    separator: str = ".",
) -> Any:
    """
    Safely get a nested value from a dictionary.
    
    Args:
        data: Dictionary to search
        path: Dot-separated path to the value
        default: Default value if path not found
        separator: Path separator (default: ".")
        
    Returns:
        Any: Value at path or default
        
    Usage:
        config = {"database": {"host": "localhost", "port": 5432}}
        host = safe_get(config, "database.host")
        # "localhost"
        
        missing = safe_get(config, "database.user", "postgres")
        # "postgres"
    """
    keys = path.split(separator)
    current = data
    
    for key in keys:
        if isinstance(current, dict) and key in current:
            current = current[key]
        else:
            return default
    
    return current


def format_bytes(size: int) -> str:
    """
    Format byte size to human-readable format.
    
    Args:
        size: Size in bytes
        
    Returns:
        str: Formatted string (e.g., "1.5 MB")
    """
    for unit in ("B", "KB", "MB", "GB", "TB"):
        if abs(size) < 1024:
            return f"{size:.1f} {unit}"
        size /= 1024
    return f"{size:.1f} PB"


def format_duration(seconds: float) -> str:
    """
    Format duration to human-readable format.
    
    Args:
        seconds: Duration in seconds
        
    Returns:
        str: Formatted string (e.g., "2h 30m 15s")
    """
    if seconds < 60:
        return f"{seconds:.1f}s"
    elif seconds < 3600:
        minutes = int(seconds // 60)
        secs = int(seconds % 60)
        return f"{minutes}m {secs}s"
    else:
        hours = int(seconds // 3600)
        minutes = int((seconds % 3600) // 60)
        secs = int(seconds % 60)
        return f"{hours}h {minutes}m {secs}s"


def validate_required_columns(
    df,  # pd.DataFrame - avoiding import for circular dependency
    required: list[str],
) -> list[str]:
    """
    Validate that required columns exist in DataFrame.
    
    Args:
        df: DataFrame to validate
        required: List of required column names
        
    Returns:
        list: List of missing column names (empty if all present)
        
    Raises:
        ValueError: If any required columns are missing and raise_error=True
    """
    missing = [col for col in required if col not in df.columns]
    return missing


def generate_batch_id() -> str:
    """
    Generate a unique batch identifier.
    
    Returns:
        str: Unique batch ID in format YYYYMMDD_HHMMSS_XXXX
    """
    import uuid
    from datetime import datetime
    
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    unique_suffix = uuid.uuid4().hex[:6]
    return f"{timestamp}_{unique_suffix}"
