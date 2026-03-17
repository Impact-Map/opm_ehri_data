"""Playwright automation for the OPM data downloads site."""

import asyncio
import re
from pathlib import Path

from .config import OPM_URL


async def setup_page(playwright):
    """Launch browser and navigate to OPM data downloads page."""
    browser = await playwright.chromium.launch(headless=True)
    context = await browser.new_context(accept_downloads=True)
    page = await context.new_page()

    print("Navigating to OPM data downloads page...")
    await page.goto(OPM_URL)
    await page.wait_for_load_state("networkidle")
    await asyncio.sleep(2)

    return browser, context, page


async def set_filters(page, data_type: str, start_date: str, end_date: str) -> int:
    """Set the date range and data type filters. Returns total file count."""
    start_input = page.locator('input[aria-label="Select start date"]')
    await start_input.fill(start_date)
    await start_input.press('Enter')
    await asyncio.sleep(1)

    end_input = page.locator('input[aria-label="Select end date"]')
    await end_input.fill(end_date)
    await end_input.press('Enter')
    await asyncio.sleep(1)

    dropdown = page.locator('#data-sources')
    await dropdown.select_option(data_type)
    await asyncio.sleep(2)

    try:
        count_locator = page.locator('p').filter(has_text=re.compile(r'\d+-\d+ of \d+'))
        count_text = await count_locator.first.text_content()
        match = re.search(r'of (\d+)', count_text)
        total = int(match.group(1)) if match else 0
    except Exception:
        total = 0

    return total


async def get_card_filename(page, card_index: int) -> str:
    """Extract the filename from a card's download button label."""
    buttons = page.locator('button[aria-label^="Download options for"]')
    button = buttons.nth(card_index)
    label = await button.get_attribute('aria-label')
    if label and label.startswith("Download options for "):
        return label.replace("Download options for ", "").strip()
    return ""


async def download_file_from_card(page, card_index: int, download_dir: Path) -> Path:
    """Download a single file by clicking its download button."""
    buttons = page.locator('button[aria-label^="Download options for"]')
    button = buttons.nth(card_index)

    await button.click()
    await asyncio.sleep(1)

    # OPM now offers TXT and JSON (previously CSV). TXT files are pipe-delimited.
    txt_option = page.locator('[aria-label*="TXT"]').first
    await txt_option.wait_for(state="visible", timeout=10000)

    async with page.expect_download(timeout=600000) as download_info:
        await txt_option.click(force=True)

    download = await download_info.value
    dest_path = download_dir / download.suggested_filename
    await download.save_as(dest_path)

    await page.keyboard.press('Escape')
    await asyncio.sleep(0.3)

    return dest_path


def parse_version_from_filename(filename: str) -> int:
    """Extract version number from OPM filename.

    Example: accessions_202511_1_2026-01-09 -> 1
    """
    parts = filename.replace('.csv', '').replace('.parquet', '').split('_')
    if len(parts) >= 3:
        try:
            return int(parts[2])
        except ValueError:
            pass
    return 0


def parse_opm_date_from_filename(filename: str) -> str:
    """Extract the OPM publish date from filename.

    Example: accessions_202511_1_2026-01-09 -> 2026-01-09
    """
    parts = filename.replace('.csv', '').replace('.parquet', '').split('_')
    if len(parts) >= 4:
        return parts[3]
    return ""


async def get_site_manifest(page, data_types: list[str], start_date: str, end_date: str) -> dict:
    """Scan all cards on OPM site without downloading. Returns manifest of what's available.

    Returns dict keyed by filename stem with metadata about each file.
    """
    site_manifest = {}

    for data_type in data_types:
        total = await set_filters(page, data_type, start_date, end_date)
        if total == 0:
            continue

        # Set to 100 items per page
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
                card_filename = await get_card_filename(page, i)
                if not card_filename:
                    continue

                # Key by filename stem (without extension)
                stem = card_filename.replace('.csv', '').replace('.parquet', '')
                version = parse_version_from_filename(card_filename)
                opm_date = parse_opm_date_from_filename(card_filename)

                site_manifest[stem] = {
                    "filename": card_filename,
                    "version": version,
                    "opm_date": opm_date,
                    "data_type": data_type.lower(),
                }

            next_button = page.locator('button[aria-label="Go to next page"]')
            if await next_button.is_disabled():
                break

            await next_button.click()
            await asyncio.sleep(2)

    return site_manifest
