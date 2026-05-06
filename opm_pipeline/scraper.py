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


class LayoutChangedError(RuntimeError):
    """Raised when an OPM site selector no longer matches expected DOM.

    Surfaced as a hard pipeline failure so layout drifts cannot silently
    cause "0 new files" runs (as happened ~2026-04-30 when the date input
    aria-labels changed from "Select start date" -> "From date selector").
    """


async def set_filters(page, data_type: str, start_date: str, end_date: str) -> int:
    """Set the date range and data type filters. Returns total file count.

    Raises LayoutChangedError if any selector fails to match or the filter
    doesn't actually take effect — better to fail the whole run than silently
    report no changes.
    """
    # OPM has used multiple aria-label conventions; try each. If you see this
    # raise, inspect data.opm.gov/explore-data/data/data-downloads and add
    # the new label to the lists below.
    start_selector = (
        'input[aria-label="From date selector"], '
        'input[aria-label="Select start date"]'
    )
    end_selector = (
        'input[aria-label="To date selector"], '
        'input[aria-label="Select end date"]'
    )

    start_input = page.locator(start_selector).first
    end_input = page.locator(end_selector).first

    try:
        await start_input.wait_for(state="visible", timeout=15000)
        await end_input.wait_for(state="visible", timeout=15000)
    except Exception as e:
        raise LayoutChangedError(
            f"Could not find date input fields on OPM site. The aria-labels "
            f"may have changed again. Update the selectors in "
            f"opm_pipeline/scraper.py:set_filters. Underlying error: {e}"
        )

    await start_input.fill(start_date)
    await start_input.press('Enter')
    await asyncio.sleep(1)

    await end_input.fill(end_date)
    await end_input.press('Enter')
    await asyncio.sleep(1)

    # Verify the date filter was actually applied. If the inputs are still
    # empty, the fill silently dropped (e.g. wrong element type) and we'd
    # otherwise scrape unfiltered results that look identical to the manifest.
    actual_start = await start_input.input_value()
    actual_end = await end_input.input_value()
    if not actual_start or not actual_end:
        raise LayoutChangedError(
            f"Date filter did not stick. start={actual_start!r}, end={actual_end!r}. "
            f"OPM site layout has likely changed."
        )

    dropdown = page.locator('#data-sources')
    try:
        await dropdown.wait_for(state="visible", timeout=15000)
    except Exception as e:
        raise LayoutChangedError(
            f"Could not find #data-sources dropdown. OPM site layout has likely changed. "
            f"Underlying error: {e}"
        )
    await dropdown.select_option(data_type)
    await asyncio.sleep(2)

    actual_dropdown = await dropdown.input_value()
    if actual_dropdown != data_type:
        raise LayoutChangedError(
            f"Data source filter did not stick. Expected {data_type!r}, "
            f"got {actual_dropdown!r}."
        )

    count_locator = page.locator('p').filter(has_text=re.compile(r'\d+-\d+ of \d+'))
    try:
        await count_locator.first.wait_for(state="visible", timeout=15000)
        count_text = await count_locator.first.text_content()
    except Exception as e:
        raise LayoutChangedError(
            f"Could not find result count text (e.g. '1-100 of 254') on OPM page. "
            f"Layout has likely changed. Underlying error: {e}"
        )

    match = re.search(r'of (\d+)', count_text or "")
    if not match:
        raise LayoutChangedError(
            f"Could not parse result count from {count_text!r}."
        )

    return int(match.group(1))


async def get_card_filename(page, card_index: int) -> str:
    """Extract the filename from a card's download button label."""
    buttons = page.locator('button[aria-label^="Download options for"]')
    button = buttons.nth(card_index)
    label = await button.get_attribute('aria-label')
    if label and label.startswith("Download options for "):
        return label.replace("Download options for ", "").strip()
    return ""


async def get_card_version(page, card_index: int) -> int:
    """Read 'Version: X' from the card's visible text by walking up the DOM."""
    buttons = page.locator('button[aria-label^="Download options for"]')
    button = buttons.nth(card_index)
    try:
        version_text = await button.evaluate("""el => {
            let node = el;
            for (let i = 0; i < 10; i++) {
                node = node.parentElement;
                if (!node) break;
                const match = node.innerText.match(/Version:\\s*(\\d+)/);
                if (match) return match[1];
            }
            return null;
        }""")
        if version_text:
            return int(version_text)
    except Exception:
        pass
    return 1


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

    Returns dict keyed by versioned HF path (e.g. 'accessions/accessions_202511_v3.parquet').
    """
    from .config import card_name_to_hf_path

    site_manifest = {}

    for data_type in data_types:
        total = await set_filters(page, data_type, start_date, end_date)
        if total == 0:
            # set_filters succeeded — page genuinely shows zero results for this
            # data type in this date range. That's surprising enough to fail loudly
            # instead of silently skipping (which previously masked layout breakage).
            raise LayoutChangedError(
                f"OPM returned 0 files for data_type={data_type!r} in "
                f"{start_date}..{end_date}. Either the date range is wrong or "
                f"the site is broken."
            )

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

                version = await get_card_version(page, i)
                hf_path = card_name_to_hf_path(card_filename, version)

                site_manifest[hf_path] = {
                    "filename": card_filename,
                    "version": version,
                    "data_type": data_type.lower(),
                }

            next_button = page.locator('button[aria-label="Go to next page"]')
            if await next_button.is_disabled():
                break

            await next_button.click()
            await asyncio.sleep(2)

    return site_manifest
