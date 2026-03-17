"""CSV-to-parquet conversion with metadata extraction."""

import hashlib
from pathlib import Path
import pandas as pd


def convert_to_parquet(csv_path: Path, parquet_dir: Path) -> Path:
    """Convert pipe-delimited CSV to parquet with zstd compression."""
    df = pd.read_csv(csv_path, delimiter='|', low_memory=False, dtype=str)
    parquet_name = csv_path.stem + ".parquet"
    parquet_path = parquet_dir / parquet_name
    df.to_parquet(parquet_path, compression='zstd', index=False)
    return parquet_path


def get_parquet_metadata(parquet_path: Path) -> dict:
    """Extract metadata from a parquet file: columns, row count, hash."""
    df = pd.read_parquet(parquet_path)
    file_hash = hashlib.sha256(parquet_path.read_bytes()).hexdigest()[:16]
    return {
        "columns": list(df.columns),
        "row_count": len(df),
        "file_hash": file_hash,
    }
