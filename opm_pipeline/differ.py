"""Compare old vs new parquet files: schema, distributions, stats."""

from __future__ import annotations

from pathlib import Path
import pandas as pd


def diff_schemas(old_cols: list[str], new_cols: list[str]) -> dict:
    """Compare column sets between old and new files."""
    old_set = set(old_cols)
    new_set = set(new_cols)
    return {
        "added": sorted(new_set - old_set),
        "removed": sorted(old_set - new_set),
        "unchanged": sorted(old_set & new_set),
    }


def diff_row_counts(old_count: int, new_count: int) -> dict:
    """Compare row counts with percentage change."""
    diff = new_count - old_count
    pct = (diff / old_count * 100) if old_count > 0 else float('inf') if new_count > 0 else 0
    return {
        "old": old_count,
        "new": new_count,
        "diff": diff,
        "pct_change": round(pct, 2),
    }


def diff_value_counts(old_df: pd.DataFrame, new_df: pd.DataFrame,
                      max_unique: int = 200) -> dict:
    """For each categorical column, compute per-value count changes.

    Returns dict keyed by column name. Each entry has:
      - value_changes: list of {value, old_count, new_count, diff} sorted by |diff| desc
      - new_values: values that only appear in new
      - removed_values: values that only appear in old
      - total_values_changed: count of values where the count changed
    """
    results = {}
    common_cols = sorted(set(old_df.columns) & set(new_df.columns))

    for col in common_cols:
        if old_df[col].dtype != 'object' and new_df[col].dtype != 'object':
            continue

        old_vc = old_df[col].value_counts()
        new_vc = new_df[col].value_counts()

        all_values = set(old_vc.index) | set(new_vc.index)

        # Skip high-cardinality columns
        if len(all_values) > max_unique:
            continue

        value_changes = []
        new_values = []
        removed_values = []

        for val in all_values:
            old_count = int(old_vc.get(val, 0))
            new_count = int(new_vc.get(val, 0))
            diff = new_count - old_count

            if diff == 0:
                continue

            entry = {
                "value": str(val),
                "old_count": old_count,
                "new_count": new_count,
                "diff": diff,
            }
            value_changes.append(entry)

            if old_count == 0:
                new_values.append(str(val))
            elif new_count == 0:
                removed_values.append(str(val))

        if not value_changes:
            continue

        value_changes.sort(key=lambda x: abs(x["diff"]), reverse=True)

        results[col] = {
            "value_changes": value_changes,
            "new_values": sorted(new_values),
            "removed_values": sorted(removed_values),
            "total_values_changed": len(value_changes),
        }

    return results


def summarize_new_file(parquet_path: Path) -> dict:
    """Summarize a brand-new parquet file: columns, row count, top values."""
    df = pd.read_parquet(parquet_path)
    summary = {
        "columns": list(df.columns),
        "row_count": len(df),
        "column_summaries": {},
    }

    for col in df.columns:
        if df[col].dtype != 'object':
            continue
        n_unique = df[col].nunique()
        # Skip columns with only 1 unique value (not interesting)
        # and high-cardinality columns (>100 unique = codes/IDs/dates, not categoricals)
        if n_unique <= 1 or n_unique > 100:
            continue
        vc = df[col].value_counts()
        summary["column_summaries"][col] = {
            "unique_count": n_unique,
            "top_values": [(str(k), int(v)) for k, v in vc.head(10).items()],
            "total_non_null": int(df[col].notna().sum()),
        }

    return summary


def generate_diff_summary(old_path: Path, new_path: Path) -> dict:
    """Full diff between old and new parquet files."""
    old_df = pd.read_parquet(old_path)
    new_df = pd.read_parquet(new_path)

    schema = diff_schemas(list(old_df.columns), list(new_df.columns))

    # Top values for newly added columns
    new_col_summaries = {}
    for col in schema["added"]:
        if col not in new_df.columns or new_df[col].dtype != 'object':
            continue
        vc = new_df[col].value_counts()
        if len(vc) <= 200:
            new_col_summaries[col] = [(str(k), int(v)) for k, v in vc.head(10).items()]

    return {
        "schema": schema,
        "new_col_summaries": new_col_summaries,
        "row_counts": diff_row_counts(len(old_df), len(new_df)),
        "value_counts": diff_value_counts(old_df, new_df),
    }
