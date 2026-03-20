"""One-time migration: rename existing HF files to include version numbers.

Scrapes OPM to get the current version of each file, then renames
'accessions/accessions_202511.parquet' -> 'accessions/accessions_202511_v3.parquet'
using server-side copy+delete (no re-uploading needed for LFS files).

After running this, rebuild the manifest:
    python run_daily.py --rebuild-manifest
"""

from __future__ import annotations

import asyncio
import os
import re
import sys
from pathlib import Path

from dotenv import load_dotenv
from huggingface_hub import HfApi, CommitOperationCopy, CommitOperationDelete, list_repo_files
from playwright.async_api import async_playwright

from opm_pipeline.config import card_name_to_hf_path
from opm_pipeline.scraper import setup_page, set_filters, get_card_filename, get_card_version

load_dotenv()

HF_TOKEN = os.environ.get("HF_TOKEN")
HF_REPO = "impactproject/opm-ehri-data"

# Matches old-style paths without version: accessions/accessions_202511.parquet
OLD_PATH_RE = re.compile(r'^(accessions|separations|employment)/\1_(\d{6})\.parquet$')


async def get_opm_versions() -> dict[str, int]:
    """Scrape OPM and return {card_name: version} for all current files."""
    versions = {}
    async with async_playwright() as playwright:
        browser, context, page = await setup_page(playwright)
        try:
            for data_type in ["Accessions", "Separations", "Employment"]:
                print(f"Scanning {data_type}...")
                total = await set_filters(page, data_type, "2015-01-01", "2030-12-31")
                if total == 0:
                    continue
                try:
                    await page.get_by_label("rows per page").select_option('100')
                    await asyncio.sleep(2)
                except Exception:
                    pass

                while True:
                    buttons = page.locator('button[aria-label^="Download options for"]')
                    count = await buttons.count()
                    for i in range(count):
                        card_name = await get_card_filename(page, i)
                        if card_name:
                            version = await get_card_version(page, i)
                            versions[card_name] = version
                    next_btn = page.locator('button[aria-label="Go to next page"]')
                    if await next_btn.is_disabled():
                        break
                    await next_btn.click()
                    await asyncio.sleep(2)
        finally:
            await browser.close()
    return versions


def migrate():
    if not HF_TOKEN:
        print("Error: HF_TOKEN required")
        sys.exit(1)

    # Get current files on HF
    all_files = list(list_repo_files(HF_REPO, repo_type="dataset", token=HF_TOKEN))
    old_files = [f for f in all_files if OLD_PATH_RE.match(f)]
    print(f"Found {len(old_files)} old-style (unversioned) files on HF")

    if not old_files:
        print("Nothing to migrate.")
        return

    # Scrape OPM for current versions
    print("Scraping OPM for version numbers...")
    opm_versions = asyncio.run(get_opm_versions())
    print(f"Got versions for {len(opm_versions)} OPM files")

    # Build rename map: old_path -> new_path
    renames = []
    skipped = []
    for old_path in old_files:
        # Find matching OPM card by reconstructing the card name
        from opm_pipeline.config import hf_path_to_card_stem
        card_stem = hf_path_to_card_stem(old_path)  # e.g. "Accessions data from November 2025"
        version = opm_versions.get(card_stem)
        if version is None:
            skipped.append(old_path)
            continue
        new_path = card_name_to_hf_path(card_stem, version)
        if new_path == old_path:
            skipped.append(old_path)  # already correct (shouldn't happen)
            continue
        renames.append((old_path, new_path))

    print(f"\nFiles to rename: {len(renames)}")
    for old, new in renames[:10]:
        print(f"  {old} -> {new}")
    if len(renames) > 10:
        print(f"  ... and {len(renames) - 10} more")
    if skipped:
        print(f"\nSkipped (not found on OPM): {len(skipped)}")
        for f in skipped:
            print(f"  {f}")

    if not renames:
        print("Nothing to rename.")
        return

    confirm = input("\nProceed with rename? [y/N] ").strip().lower()
    if confirm != 'y':
        print("Aborted.")
        return

    # Execute server-side copy+delete in one commit
    api = HfApi()
    ops = []
    for old_path, new_path in renames:
        ops.append(CommitOperationCopy(src_path_in_repo=old_path, path_in_repo=new_path))
        ops.append(CommitOperationDelete(path_in_repo=old_path))

    print(f"\nCommitting {len(renames)} renames to HF...")
    api.create_commit(
        repo_id=HF_REPO,
        repo_type="dataset",
        token=HF_TOKEN,
        operations=ops,
        commit_message=f"Migrate {len(renames)} files to versioned filenames",
    )
    print("Done! Now rebuild the manifest:")
    print("  python run_daily.py --rebuild-manifest")


if __name__ == "__main__":
    migrate()
