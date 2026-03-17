"""
Smoke test: download one file from OPM, convert to parquet, verify the data looks right.
Exits 0 if everything works, 1 if something's broken.
"""

from __future__ import annotations

import asyncio
import os
import sys
from pathlib import Path

from dotenv import load_dotenv
from playwright.async_api import async_playwright

from opm_pipeline.scraper import setup_page, set_filters, get_card_filename, download_file_from_card
from opm_pipeline.converter import convert_to_parquet

load_dotenv()

DOWNLOAD_DIR = Path("data/downloads")
PARQUET_DIR = Path("data/parquet")

# Columns we always expect in accessions data
EXPECTED_COLUMNS = {"agency_code", "agency", "count", "occupational_series_code"}


async def smoke_test():
    DOWNLOAD_DIR.mkdir(parents=True, exist_ok=True)
    PARQUET_DIR.mkdir(parents=True, exist_ok=True)

    print("1. Connecting to OPM site...")
    async with async_playwright() as playwright:
        browser, context, page = await setup_page(playwright)

        try:
            total = await set_filters(page, "Accessions", "2025-11-01", "2025-11-30")
            assert total > 0, f"OPM returned 0 files (expected at least 1)"
            print(f"   Found {total} files")

            print("2. Downloading one file...")
            card_name = await get_card_filename(page, 0)
            assert card_name, "Could not read card filename"
            print(f"   Card: {card_name}")

            csv_path = await download_file_from_card(page, 0, DOWNLOAD_DIR)
            assert csv_path.exists(), f"Downloaded file not found at {csv_path}"
            assert csv_path.stat().st_size > 1000, f"Downloaded file too small ({csv_path.stat().st_size} bytes)"
            print(f"   Downloaded: {csv_path.name} ({csv_path.stat().st_size / 1024:.0f} KB)")

        finally:
            await browser.close()

    print("3. Converting to parquet...")
    import pandas as pd
    parquet_path = convert_to_parquet(csv_path, PARQUET_DIR)
    assert parquet_path.exists(), "Parquet file not created"

    df = pd.read_parquet(parquet_path)
    print(f"   {len(df)} rows, {len(df.columns)} columns")

    assert len(df) > 100, f"Too few rows ({len(df)}), data may be corrupted"
    assert len(df.columns) > 10, f"Too few columns ({len(df.columns)}), parsing may have failed"

    missing = EXPECTED_COLUMNS - set(df.columns)
    assert not missing, f"Missing expected columns: {missing}"

    print("4. Checking data quality...")
    assert df["count"].notna().all(), "count column has null values"
    assert df["agency_code"].nunique() > 5, f"Only {df['agency_code'].nunique()} agencies, expected many more"
    print(f"   {df['agency_code'].nunique()} agencies, all checks passed")

    # Cleanup
    csv_path.unlink()
    parquet_path.unlink()

    print("\nSMOKE TEST PASSED")


if __name__ == "__main__":
    try:
        asyncio.run(smoke_test())
    except Exception as e:
        print(f"\nSMOKE TEST FAILED: {e}")
        sys.exit(1)
