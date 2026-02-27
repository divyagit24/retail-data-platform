"""
utils.py
--------
Shared utility functions used across ingestion and validation scripts.
"""

import os
import logging
from datetime import datetime
from dotenv import load_dotenv

load_dotenv()

logger = logging.getLogger(__name__)


def get_snowflake_config() -> dict:
    """Return Snowflake connection config from environment variables."""
    config = {
        "account":   os.getenv("SNOWFLAKE_ACCOUNT"),
        "user":      os.getenv("SNOWFLAKE_USER"),
        "password":  os.getenv("SNOWFLAKE_PASSWORD"),
        "warehouse": os.getenv("SNOWFLAKE_WAREHOUSE", "RETAIL_WH"),
        "database":  os.getenv("SNOWFLAKE_DATABASE",  "RETAIL_DB"),
        "schema":    os.getenv("SNOWFLAKE_SCHEMA",    "RAW"),
        "role":      os.getenv("SNOWFLAKE_ROLE",       "SYSADMIN"),
    }
    missing = [k for k, v in config.items() if not v]
    if missing:
        raise EnvironmentError(
            f"Missing required environment variables: {missing}\n"
            f"Copy .env.example to .env and fill in your credentials."
        )
    return config


def setup_logging(log_filename: str = "pipeline.log") -> logging.Logger:
    """Configure and return a logger that writes to file and console."""
    os.makedirs("logs", exist_ok=True)
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
        handlers=[
            logging.FileHandler(f"logs/{log_filename}"),
            logging.StreamHandler()
        ]
    )
    return logging.getLogger(__name__)


def timer(func):
    """Decorator to log execution time of any function."""
    import functools
    import time

    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        start = time.time()
        result = func(*args, **kwargs)
        elapsed = time.time() - start
        logger.info(f"{func.__name__} completed in {elapsed:.2f}s")
        return result
    return wrapper


def format_number(n: int) -> str:
    """Format a number with commas for readable logging."""
    return f"{n:,}"


def current_utc_timestamp() -> str:
    """Return current UTC timestamp as string."""
    return datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
