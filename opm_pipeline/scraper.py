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
    """Set the data type filter. Returns total file count.

    Date filtering is intentionally NOT applied: OPM moved to a readonly
    MudBlazor date picker that can't be driven by .fill(), and the diff
    is done by comparing site_manifest → stored_manifest anyway. Iterating
    every card for a data_type is cheap (~250 cards, paginated 100 at a
    time) and avoids depending on a fragile picker UI.

    The start_date / end_date args are kept for backwards compatibility
    with the existing call sites (and smoke_test.py); they're ignored.

    Raises LayoutChangedError if the dropdown or count text can't be
    found / doesn't take effect — better to fail the whole run than
    silently report no changes.
    """
    del start_date, end_date  # see docstring

    dropdown = page.locator('#data-sources')
    try:
        await dropdown.wait_for(state="visible", timeout=15000)
    except Exception as e:
        raise LayoutChangedError(
            f"Could not find #data-sources dropdown on OPM site. Layout has "
            f"likely changed. Underlying error: {e}"
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
    """Download a single file by clicking its download button.

    We let Chromium initiate the download just long enough to capture the
    real URL, then cancel its in-progress save and fetch the file with httpx
    using the browser session's cookies. Chromium's download manager kept
    hitting "Download.save_as: canceled" on the large (~780 MB) Employment
    files, even with browser restarts; httpx streaming sidesteps that.
    """
    import httpx

    buttons = page.locator('button[aria-label^="Download options for"]')
    button = buttons.nth(card_index)

    await button.click()
    await asyncio.sleep(1)

    # OPM now offers TXT and JSON (previously CSV). TXT files are pipe-delimited.
    txt_option = page.locator('[aria-label*="TXT"]').first
    await txt_option.wait_for(state="visible", timeout=10000)

    async with page.expect_download(timeout=60000) as download_info:
        await txt_option.click(force=True)

    download = await download_info.value
    download_url = download.url
    suggested_filename = download.suggested_filename

    # We have the URL; cancel the Playwright download so it doesn't keep
    # streaming into a temp file we won't use.
    try:
        await download.cancel()
    except Exception:
        pass

    await page.keyboard.press('Escape')
    await asyncio.sleep(0.3)

    # Stream the file via httpx, using the same cookies the browser has so
    # that whatever signed/session-scoped URL OPM hands us still works.
    cookies = {c["name"]: c["value"] for c in await page.context.cookies()}
    headers = {
        "User-Agent": (await page.evaluate("navigator.userAgent")),
    }
    dest_path = download_dir / suggested_filename
    async with httpx.AsyncClient(cookies=cookies, headers=headers,
                                  follow_redirects=True, timeout=600.0) as client:
        async with client.stream("GET", download_url) as response:
            response.raise_for_status()
            with open(dest_path, "wb") as f:
                async for chunk in response.aiter_bytes(chunk_size=1024 * 64):
                    f.write(chunk)

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
