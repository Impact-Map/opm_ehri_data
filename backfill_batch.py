"""
Batch backfill: find gaps between OPM and HuggingFace, fill newest-first.

Scrapes OPM for all available files, checks what's already on HF,
and uploads missing files starting from the most recent.
Processes up to BATCH_SIZE files per run to stay under HF's rate limit.
Designed to run repeatedly (via GitHub Actions) until everything is uploaded.
"""

from __future__ import annotations

import asyncio
import os
import re
import sys
import time
from datetime import datetime
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

from huggingface_hub import HfApi, CommitOperationAdd
from opm_pipeline.scraper import setup_page, set_filters, get_card_filename, download_file_from_card
from opm_pipeline.converter import convert_to_parquet
from opm_pipeline.config import card_name_to_hf_path


def get_existing_files() -> set[str]:
    """Get set of parquet paths already in HF repo."""
    try:
        create_repo(HF_REPO, repo_type="dataset", token=HF_TOKEN, exist_ok=True)
        files = list_repo_files(HF_REPO, repo_type="dataset", token=HF_TOKEN)
        return {f for f in files if f.endswith('.parquet')}
    except Exception:
        return set()


MONTH_RE = re.compile(r'from (\w+) (\d{4})')

def _sort_key_newest_first(card_name: str) -> tuple:
    """Parse 'Month Year' from card name for sorting newest-first."""
    m = MONTH_RE.search(card_name)
    if not m:
        return (0, 0)
    try:
        dt = datetime.strptime(f"{m.group(1)} {m.group(2)}", "%B %Y")
        return (-dt.year, -dt.month)
    except ValueError:
        return (0, 0)


async def backfill_batch():
    if not HF_TOKEN:
        print("Error: HF_TOKEN required")
        sys.exit(1)

    DOWNLOAD_DIR.mkdir(parents=True, exist_ok=True)
    PARQUET_DIR.mkdir(parents=True, exist_ok=True)

    # Get what's already uploaded
    existing = get_existing_files()
    print(f"Already on HF: {len(existing)} parquet files")

    async with async_playwright() as playwright:
        browser, context, page = await setup_page(playwright)

        try:
            # Step 1: Scan OPM for all available files
            all_cards = []  # list of (card_name, data_type, page_num, card_index)

            for data_type in ["Accessions", "Separations", "Employment"]:
                total = await set_filters(page, data_type, "2015-01-01", "2030-12-31")
                if total == 0:
                    print(f"{data_type}: no files found")
                    continue

                print(f"{data_type}: {total} files on OPM")

                try:
                    rows_dropdown = page.get_by_label("rows per page")
                    await rows_dropdown.select_option('100')
                    await asyncio.sleep(2)
                except Exception:
                    pass

                while True:
                    buttons = page.locator('button[aria-label^="Download options for"]')
                    count = await buttons.count()

                    for i in range(count):
                        card_name = await get_card_filename(page, i)
                        if not card_name:
                            continue
                        parquet_name = card_name_to_hf_path(card_name)
                        if parquet_name not in existing:
                            all_cards.append((card_name, data_type))

                    next_button = page.locator('button[aria-label="Go to next page"]')
                    if await next_button.is_disabled():
                        break
                    await next_button.click()
                    await asyncio.sleep(2)

            # Step 2: Sort missing files newest-first
            all_cards.sort(key=lambda x: _sort_key_newest_first(x[0]))

            print(f"\nMissing from HF: {len(all_cards)} files")
            if not all_cards:
                print("Nothing to backfill!")
                await browser.close()
                return

            for name, dtype in all_cards[:10]:
                print(f"  {card_name_to_hf_path(name)}")
            if len(all_cards) > 10:
                print(f"  ... and {len(all_cards) - 10} more")

            # Step 3: Download all files in batch, then commit once
            downloaded = []  # list of (parquet_path, hf_path)
            failed = 0

            for card_name, data_type in all_cards:
                if len(downloaded) >= BATCH_SIZE:
                    break

                hf_path = card_name_to_hf_path(card_name)

                try:
                    total = await set_filters(page, data_type, "2015-01-01", "2030-12-31")
                    if total == 0:
                        failed += 1
                        continue

                    try:
                        rows_dropdown = page.get_by_label("rows per page")
                        await rows_dropdown.select_option('100')
                        await asyncio.sleep(2)
                    except Exception:
                        pass

                    found = False
                    while True:
                        buttons = page.locator('button[aria-label^="Download options for"]')
                        count = await buttons.count()

                        for i in range(count):
                            current_name = await get_card_filename(page, i)
                            if current_name != card_name:
                                continue

                            csv_path = await download_file_from_card(page, i, DOWNLOAD_DIR)
                            csv_size = csv_path.stat().st_size / (1024 * 1024)
                            parquet_path = convert_to_parquet(csv_path, PARQUET_DIR)
                            parquet_size = parquet_path.stat().st_size / (1024 * 1024)
                            csv_path.unlink()

                            downloaded.append((parquet_path, hf_path))
                            print(f"  [{len(downloaded)}/{BATCH_SIZE}] {hf_path} ({csv_size:.0f}MB -> {parquet_size:.1f}MB)")
                            found = True
                            break

                        if found:
                            break

                        next_button = page.locator('button[aria-label="Go to next page"]')
                        if await next_button.is_disabled():
                            break
                        await next_button.click()
                        await asyncio.sleep(2)

                    if not found:
                        print(f"  SKIPPED: {hf_path} (card not found)")
                        failed += 1

                except Exception as e:
                    print(f"  FAILED: {hf_path}: {str(e)[:100]}")
                    failed += 1
                    try:
                        await page.keyboard.press('Escape')
                    except Exception:
                        pass
                    time.sleep(5)

        finally:
            await browser.close()

    # Step 4: Single commit for all downloaded files
    if downloaded:
        print(f"\nUploading {len(downloaded)} files in one commit...")
        api = HfApi()
        ops = [CommitOperationAdd(path_in_repo=hf_path, path_or_fileobj=str(parquet_path))
               for parquet_path, hf_path in downloaded]
        api.create_commit(
            repo_id=HF_REPO, repo_type="dataset", token=HF_TOKEN,
            operations=ops,
            commit_message=f"Backfill: add {len(downloaded)} files",
        )
        for parquet_path, _ in downloaded:
            parquet_path.unlink(missing_ok=True)
        print(f"Committed {len(downloaded)} files.")

    total_on_hf = len(existing) + len(downloaded)
    print(f"\nDone: {len(downloaded)} uploaded, {failed} failed, {total_on_hf} total on HF")

    if len(downloaded) == 0 and failed == 0:
        print("Nothing left to upload — backfill complete!")


if __name__ == "__main__":
    asyncio.run(backfill_batch())
