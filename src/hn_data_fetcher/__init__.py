"""Hacker News -> Hive-partitioned Parquet, with optional Tigris/S3 sync."""

from .delete_from_tigris import main as delete_from_tigris
from .fetcher import main
from .upload_to_tigris import main as upload_to_tigris

__all__ = ["main", "upload_to_tigris", "delete_from_tigris"]
