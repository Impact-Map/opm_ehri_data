"""Parquet metadata extraction."""

import hashlib
from pathlib import Path
import pandas as pd


def get_parquet_metadata(parquet_path: Path) -> dict:
    """Extract metadata from a parquet file: columns, row count, hash."""
    df = pd.read_parquet(parquet_path)
    file_hash = hashlib.sha256(parquet_path.read_bytes()).hexdigest()[:16]
    return {
        "columns": list(df.columns),
        "row_count": len(df),
        "file_hash": file_hash,
    }
