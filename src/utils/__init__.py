"""
Utilidades del proyecto TFM E-commerce Analytics.
"""

from .config import (
    PROJECT_ROOT,
    DATA_DIR,
    RAW_DATA_DIR,
    BRONZE_DIR,
    SILVER_DIR,
    GOLD_DIR,
    POSTGRES_CONFIG,
    KAFKA_CONFIG,
    SPARK_CONFIG,
    ensure_directories,
)
from .logger import setup_logger

__all__ = [
    "PROJECT_ROOT",
    "DATA_DIR",
    "RAW_DATA_DIR",
    "BRONZE_DIR",
    "SILVER_DIR",
    "GOLD_DIR",
    "POSTGRES_CONFIG",
    "KAFKA_CONFIG",
    "SPARK_CONFIG",
    "ensure_directories",
    "setup_logger",
]