"""
Batch backfill: download files from OPM, convert to parquet, upload to HF.
Processes up to BATCH_SIZE files per run to stay under HF's rate limit (128 commits/hour).
Designed to run repeatedly (via GitHub Actions every 2 hours) until all files are uploaded.
"""

from __future__ import annotations

import asyncio
import os
import sys
import time
from pathlib import Path

from dotenv import load_dotenv
from playwright.async_api import async_playwright
from huggingface_hub import list_repo_files, create_repo

load_dotenv()

HF_TOKEN = os.environ.get("HF_TOKEN")
HF_REPO = "abigailhaddad/opm-federal-workforce"
BATCH_SIZE = 100  # Stay under 128 commits/hour limit
DOWNLOAD_DIR = Path("data/downloads")
PARQUET_DIR = Path("data/parquet")

# Only backfill data older than this. Recent months go through run_daily.py
# which creates diff reports and GitHub Issues.
BACKFILL_END_DATE = "2025-06-30"
BACKFILL_START_DATE = "2015-01-01"

# Import after dotenv so config picks up the token
from opm_pipeline.scraper import setup_page, set_filters, get_card_filename, download_file_from_card
from opm_pipeline.converter import convert_to_parquet
from opm_pipeline.uploader import upload_to_huggingface


def get_existing_files() -> set[str]:
    """Get set of parquet filenames already in HF repo."""
    try:
        create_repo(HF_REPO, repo_type="dataset", token=HF_TOKEN, exist_ok=True)
        files = list_repo_files(HF_REPO, repo_type="dataset", token=HF_TOKEN)
        return {f for f in files if f.endswith('.parquet')}
    except Exception:
        return set()


def card_name_to_parquet(card_name: str) -> str:
    """Convert card display name to expected parquet filename.

    'Accessions data from November 2025' -> 'Accessions data from November 2025.parquet'
    """
    return card_name.replace('.csv', '').replace('.txt', '') + ".parquet"


async def backfill_batch():
    if not HF_TOKEN:
        print("Error: HF_TOKEN required")
        sys.exit(1)

    DOWNLOAD_DIR.mkdir(parents=True, exist_ok=True)
    PARQUET_DIR.mkdir(parents=True, exist_ok=True)

    # Get what's already uploaded
    existing = get_existing_files()
    print(f"Already on HF: {len(existing)} parquet files")

    uploaded = 0
    failed = 0

    async with async_playwright() as playwright:
        browser, context, page = await setup_page(playwright)

        try:
            for data_type in ["Accessions", "Separations", "Employment"]:
                if uploaded >= BATCH_SIZE:
                    break

                total = await set_filters(page, data_type, BACKFILL_START_DATE, BACKFILL_END_DATE)
                if total == 0:
                    print(f"{data_type}: no files found")
                    continue

                print(f"\n{data_type}: {total} files on OPM")

                # Set to 100 items per page
                try:
                    rows_dropdown = page.get_by_label("rows per page")
                    await rows_dropdown.select_option('100')
                    await asyncio.sleep(2)
                except Exception:
                    pass

                page_num = 1
                while uploaded < BATCH_SIZE:
                    buttons = page.locator('button[aria-label^="Download options for"]')
                    count = await buttons.count()

                    for i in range(count):
                        if uploaded >= BATCH_SIZE:
                            break

                        card_name = await get_card_filename(page, i)
                        if not card_name:
                            continue

                        parquet_name = card_name_to_parquet(card_name)
                        if parquet_name in existing:
                            continue

                        try:
                            # Download
                            csv_path = await download_file_from_card(page, i, DOWNLOAD_DIR)
                            csv_size = csv_path.stat().st_size / (1024 * 1024)

                            # Convert
                            parquet_path = convert_to_parquet(csv_path, PARQUET_DIR)
                            parquet_size = parquet_path.stat().st_size / (1024 * 1024)

                            # Upload
                            upload_to_huggingface(parquet_path, parquet_name, HF_TOKEN)
                            existing.add(parquet_name)
                            uploaded += 1

                            print(f"  [{uploaded}/{BATCH_SIZE}] {parquet_name} ({csv_size:.0f}MB -> {parquet_size:.1f}MB)")

                            # Cleanup
                            csv_path.unlink()
                            parquet_path.unlink()

                            # Rate limit buffer
                            time.sleep(2)

                        except Exception as e:
                            error = str(e)[:100]
                            print(f"  FAILED: {parquet_name}: {error}")
                            failed += 1
                            try:
                                await page.keyboard.press('Escape')
                            except Exception:
                                pass
                            time.sleep(5)
                            continue

                    # Next page
                    next_button = page.locator('button[aria-label="Go to next page"]')
                    if await next_button.is_disabled():
                        break
                    await next_button.click()
                    await asyncio.sleep(2)
                    page_num += 1

        finally:
            await browser.close()

    print(f"\nDone: {uploaded} uploaded, {failed} failed, {len(existing)} total on HF")

    if uploaded == 0 and failed == 0:
        print("Nothing left to upload — backfill complete!")


if __name__ == "__main__":
    asyncio.run(backfill_batch())
