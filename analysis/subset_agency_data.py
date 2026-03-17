"""
Subset OPM accessions and separations data for specific agencies (USDA, DOI).
Pulls from HuggingFace, filters, combines both data types, and saves as CSV.
Validates that all expected months are present before saving.
"""

from __future__ import annotations

import re
import sys
from pathlib import Path
from huggingface_hub import list_repo_files, hf_hub_download
import pandas as pd
from dotenv import load_dotenv
from tqdm import tqdm

load_dotenv()

HF_REPO = "abigailhaddad/opm-federal-workforce"
OUTPUT_DIR = Path("data/agency_subsets")

# Expected date range (inclusive)
START_YEAR_MONTH = (2015, 1)
END_YEAR_MONTH = (2025, 11)

AGENCIES = {
    "AG": "usda",  # Department of Agriculture
    "IN": "doi",   # Department of Interior
}


def get_expected_months() -> set[str]:
    """Generate all expected YYYYMM strings in the date range."""
    expected = set()
    year, month = START_YEAR_MONTH
    end_year, end_month = END_YEAR_MONTH

    while (year, month) <= (end_year, end_month):
        expected.add(f"{year}{month:02d}")
        month += 1
        if month > 12:
            month = 1
            year += 1
    return expected


def get_available_files(data_type: str) -> list[str]:
    """Get all parquet filenames in the repo for a data type."""
    all_files = list_repo_files(HF_REPO, repo_type="dataset")
    return sorted([f for f in all_files if f.startswith(data_type) and f.endswith('.parquet')])


def extract_year_month(filename: str) -> str | None:
    """Extract YYYYMM from filename like 'accessions_202511.parquet'."""
    match = re.search(r'_(\d{6})', filename)
    return match.group(1) if match else None


def validate_months(filenames: list[str], data_type: str) -> bool:
    """Check that all expected months are present. Returns True if valid."""
    expected = get_expected_months()
    found = set()

    for f in filenames:
        ym = extract_year_month(f)
        if ym:
            found.add(ym)

    missing = expected - found

    if missing:
        missing_sorted = sorted(missing)
        print(f"\n  ERROR: Missing {len(missing)} months for {data_type}:")
        for ym in missing_sorted:
            print(f"    - {ym[:4]}-{ym[4:]}")
        return False

    return True


def download_and_filter(filename: str, agency_codes: list[str], data_type: str,
                        dropped_tracker: dict) -> pd.DataFrame:
    """Download a dataset and filter for specific agencies.

    Filters on personnel_action_effective_date_yyyymm to only include data
    within our date range. Delayed reporting from prior years is kept if it
    falls within our window (e.g., 2023 data in a 2025 file is kept if our
    range includes 2023).

    Tracks dropped records in dropped_tracker dict for summary at end.
    """
    path = hf_hub_download(repo_id=HF_REPO, filename=filename, repo_type="dataset")
    df = pd.read_parquet(path)

    # Filter by agency FIRST
    df = df[df["agency_code"].isin(agency_codes)]

    if len(df) == 0:
        df["data_type"] = data_type
        return df

    # Check what would be dropped and track it
    date_col = "personnel_action_effective_date_yyyymm"
    if date_col in df.columns:
        start_yyyymm = f"{START_YEAR_MONTH[0]}{START_YEAR_MONTH[1]:02d}"
        end_yyyymm = f"{END_YEAR_MONTH[0]}{END_YEAR_MONTH[1]:02d}"

        in_range = (df[date_col] >= start_yyyymm) & (df[date_col] <= end_yyyymm)
        dropped = df[~in_range]

        if len(dropped) > 0:
            dropped = dropped.copy()
            dropped["count"] = pd.to_numeric(dropped["count"], errors="coerce").fillna(0)
            for _, row in dropped.iterrows():
                key = (row["agency_code"], data_type)
                if key not in dropped_tracker:
                    dropped_tracker[key] = {"count": 0, "months": set()}
                dropped_tracker[key]["count"] += int(row["count"])
                dropped_tracker[key]["months"].add(row[date_col])

        df = df[in_range]

    df["data_type"] = data_type  # Add column to distinguish accessions vs separations
    return df


def main():
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    agency_codes = list(AGENCIES.keys())
    expected_count = len(get_expected_months())

    print(f"Expected date range: {START_YEAR_MONTH[0]}-{START_YEAR_MONTH[1]:02d} to {END_YEAR_MONTH[0]}-{END_YEAR_MONTH[1]:02d}")
    print(f"Expected months per data type: {expected_count}")

    # First, validate all data types have complete months
    print(f"\n{'='*60}")
    print("VALIDATING DATA AVAILABILITY")
    print(f"{'='*60}")

    all_files = {}
    for data_type in ["accessions", "separations"]:
        files = get_available_files(data_type)
        all_files[data_type] = files
        print(f"\n  {data_type}: found {len(files)} files")

        if not validate_months(files, data_type):
            print(f"\n  ABORTING: Missing months detected. Please run the download script first.")
            sys.exit(1)

        print(f"  {data_type}: all {expected_count} months present")

    # Collect data per agency across both data types
    agency_dfs = {code: [] for code in agency_codes}
    dropped_tracker = {}  # Track what's dropped for summary

    for data_type in ["accessions", "separations"]:
        print(f"\n{'='*60}")
        print(f"Processing {data_type.upper()}")
        print(f"{'='*60}")

        files = all_files[data_type]

        for filename in tqdm(files, desc=f"Downloading {data_type}"):
            try:
                df = download_and_filter(filename, agency_codes, data_type, dropped_tracker)
                for code in agency_codes:
                    agency_df = df[df["agency_code"] == code]
                    if len(agency_df) > 0:
                        agency_dfs[code].append(agency_df)
            except Exception as e:
                print(f"  Error with {filename}: {e}")
                continue

    # Print dropped data summary
    if dropped_tracker:
        print(f"\n{'='*60}")
        print("DROPPED DATA SUMMARY (outside date range)")
        print(f"{'='*60}")
        start_yyyymm = f"{START_YEAR_MONTH[0]}{START_YEAR_MONTH[1]:02d}"
        end_yyyymm = f"{END_YEAR_MONTH[0]}{END_YEAR_MONTH[1]:02d}"
        print(f"  Date range: {start_yyyymm} - {end_yyyymm}\n")

        for (agency_code, data_type), info in sorted(dropped_tracker.items()):
            agency_name = next((n for c, n in AGENCIES.items() if c == agency_code), agency_code)
            months_sorted = sorted(info["months"])
            month_range = f"{months_sorted[0]} to {months_sorted[-1]}" if len(months_sorted) > 1 else months_sorted[0]
            print(f"  {agency_name.upper()} {data_type}: {info['count']:,} records dropped")
            print(f"    Months: {month_range} ({len(months_sorted)} unique months)")

    # Save combined data for each agency
    print(f"\n{'='*60}")
    print("SAVING COMBINED FILES")
    print(f"{'='*60}")

    for code, name in AGENCIES.items():
        if not agency_dfs[code]:
            print(f"  No data for {name.upper()}")
            continue

        combined = pd.concat(agency_dfs[code], ignore_index=True)
        filename = f"{name}_accessions_separations.csv"
        filepath = OUTPUT_DIR / filename
        combined.to_csv(filepath, index=False)

        size_mb = filepath.stat().st_size / (1024 * 1024)
        accessions_count = len(combined[combined["data_type"] == "accessions"])
        separations_count = len(combined[combined["data_type"] == "separations"])

        print(f"\n  {name.upper()} ({code}):")
        print(f"    File: {filepath}")
        print(f"    Accessions rows: {accessions_count:,}")
        print(f"    Separations rows: {separations_count:,}")
        print(f"    Total rows: {len(combined):,}")
        print(f"    Size: {size_mb:.2f} MB")


if __name__ == "__main__":
    main()
