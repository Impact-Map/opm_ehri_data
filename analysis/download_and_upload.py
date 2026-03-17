"""
Download OPM workforce data, convert to parquet, upload to Hugging Face.
All files go into a single HF repo with their OPM-derived filenames.
"""

import asyncio
import argparse
import os
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from playwright.async_api import async_playwright
from tqdm import tqdm

from opm_pipeline.config import (
    HF_REPO, START_DATE, END_DATE, DATA_TYPES,
    DOWNLOAD_DIR, PARQUET_DIR, SIZE_ESTIMATES,
)
from opm_pipeline.scraper import (
    setup_page, set_filters, get_card_filename, download_file_from_card,
)
from opm_pipeline.converter import convert_to_parquet
from opm_pipeline.uploader import is_already_uploaded, upload_to_huggingface, get_repo_files


async def download_and_upload_all(page, data_type: str, download_dir, parquet_dir,
                                   start_date: str, end_date: str, token: str,
                                   repo_files: set[str]):
    """Download all files for a data type, uploading each immediately."""
    print(f"\n{'='*60}")
    print(f"  {data_type.upper()}")
    print(f"{'='*60}")

    total = await set_filters(page, data_type, start_date, end_date)
    if total == 0:
        print("  No files found, skipping...")
        return [], []

    print(f"  Found {total} files")

    # Set to 100 items per page
    try:
        rows_dropdown = page.get_by_label("rows per page")
        await rows_dropdown.select_option('100')
        await asyncio.sleep(2)
    except Exception:
        pass

    uploaded_files = []
    failed_files = []
    pbar = tqdm(total=total, desc=f"  {data_type}", unit="file")

    page_num = 1
    while True:
        buttons = page.locator('button[aria-label^="Download options for"]')
        count = await buttons.count()

        for i in range(count):
            card_filename = None
            try:
                card_filename = await get_card_filename(page, i)
                if card_filename:
                    parquet_name = card_filename.replace('.csv', '') + ".parquet"
                    # Remove .csv if present, ensure .parquet
                    if not parquet_name.endswith('.parquet'):
                        parquet_name = card_filename + ".parquet"

                    if is_already_uploaded(parquet_name, token, repo_files):
                        pbar.set_postfix({"status": "skipped (exists)"})
                        pbar.update(1)
                        uploaded_files.append(parquet_name)
                        continue

                # Download
                csv_path = await download_file_from_card(page, i, download_dir)
                csv_size = csv_path.stat().st_size / (1024 * 1024)

                # Convert
                parquet_path = convert_to_parquet(csv_path, parquet_dir)
                parquet_size = parquet_path.stat().st_size / (1024 * 1024)
                parquet_name = parquet_path.name

                pbar.set_postfix({
                    "file": parquet_name[-30:],
                    "size": f"{csv_size:.0f}->{parquet_size:.1f}MB"
                })

                # Upload
                upload_to_huggingface(parquet_path, parquet_name, token)
                uploaded_files.append(parquet_name)
                repo_files.add(parquet_name)

                # Cleanup
                csv_path.unlink()
                parquet_path.unlink()

                pbar.update(1)
                await asyncio.sleep(0.3)

            except Exception as e:
                error_msg = str(e)[:60]
                pbar.write(f"  Warning: {error_msg}")
                if card_filename:
                    failed_files.append({"filename": card_filename, "error": error_msg})
                await page.keyboard.press('Escape')
                await asyncio.sleep(0.5)
                pbar.update(1)
                continue

        next_button = page.locator('button[aria-label="Go to next page"]')
        if await next_button.is_disabled():
            break

        await next_button.click()
        await asyncio.sleep(2)
        page_num += 1

    pbar.close()
    print(f"  Uploaded {len(uploaded_files)} files")
    if failed_files:
        print(f"  Failed {len(failed_files)} files")
    return uploaded_files, failed_files


async def main():
    parser = argparse.ArgumentParser(description="Download OPM data and upload to HuggingFace")
    parser.add_argument("--token", default=os.environ.get("HF_TOKEN"), help="HuggingFace token")
    parser.add_argument("--start", default=START_DATE, help="Start date (YYYY-MM-DD)")
    parser.add_argument("--end", default=END_DATE, help="End date (YYYY-MM-DD)")
    parser.add_argument("--types", nargs="+", default=DATA_TYPES, help="Data types to download")
    args = parser.parse_args()

    if not args.token:
        print("Error: HF_TOKEN environment variable or --token required")
        return

    DOWNLOAD_DIR.mkdir(parents=True, exist_ok=True)
    PARQUET_DIR.mkdir(parents=True, exist_ok=True)

    print("\n" + "="*60)
    print(f"OPM Data -> HuggingFace ({HF_REPO})")
    print("="*60)
    print(f"Date range: {args.start} to {args.end}")
    print(f"Data types: {', '.join(args.types)}")

    print("\nEstimated totals:")
    for dtype in args.types:
        months = 59
        est_csv = SIZE_ESTIMATES.get(dtype, 10) * months
        est_parquet = est_csv * 0.04
        print(f"  {dtype}: {months} files, ~{est_parquet:.0f} MB total parquet")

    # Fetch existing files once upfront
    repo_files = get_repo_files(args.token)
    print(f"\nExisting files in HF repo: {len(repo_files)}")

    all_files = []
    all_failures = []

    async with async_playwright() as playwright:
        browser, context, page = await setup_page(playwright)

        try:
            for data_type in args.types:
                files, failures = await download_and_upload_all(
                    page, data_type, DOWNLOAD_DIR, PARQUET_DIR,
                    args.start, args.end, args.token, repo_files
                )
                all_files.extend(files)
                all_failures.extend(failures)

        finally:
            await browser.close()

    print("\n" + "="*60)
    print("DONE!")
    print("="*60)
    print(f"Uploaded {len(all_files)} files to {HF_REPO}")

    if all_failures:
        print(f"\n{len(all_failures)} files failed:")
        for f in all_failures:
            print(f"  - {f['filename']}: {f['error']}")
        print("\nRe-run the script to retry failed files.")


if __name__ == "__main__":
    asyncio.run(main())
