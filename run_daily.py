"""Daily OPM data pipeline entry point.

Checks OPM site for new/updated files, downloads and uploads them to HuggingFace,
and creates a GitHub Issue summarizing changes. On failure, creates a diagnostic
issue explaining what went wrong.
"""

from __future__ import annotations

import asyncio
import argparse
import os
import sys
import traceback
from datetime import date

from opm_pipeline.config import (
    HF_TOKEN, HF_REPO, DATA_TYPES, START_DATE, END_DATE,
    DOWNLOAD_DIR, PARQUET_DIR,
)
from opm_pipeline.reporter import create_github_issue


class PipelineError(Exception):
    """Error with a human-readable diagnosis and fix instructions."""
    def __init__(self, message: str, diagnosis: str, fix: str):
        super().__init__(message)
        self.diagnosis = diagnosis
        self.fix = fix


def preflight_checks(token: str):
    """Validate environment before doing any real work. Fails fast with clear messages."""

    # 1. HF token
    if not token:
        raise PipelineError(
            "No HuggingFace token provided",
            "The HF_TOKEN environment variable is empty or missing. "
            "This is required to upload data to HuggingFace.",
            "Go to Settings > Secrets and variables > Actions in the GitHub repo "
            "and make sure HF_TOKEN is set to a valid HuggingFace token with write access. "
            "You can create a token at https://huggingface.co/settings/tokens"
        )

    # 2. HF token is valid
    from huggingface_hub import whoami
    try:
        user_info = whoami(token=token)
        print(f"Authenticated as: {user_info.get('name', 'unknown')}")
    except Exception as e:
        raise PipelineError(
            f"HuggingFace authentication failed: {e}",
            "The HF_TOKEN exists but HuggingFace rejected it. "
            "It may be expired, revoked, or malformed.",
            "Go to https://huggingface.co/settings/tokens, create a new token with "
            "write access, and update the HF_TOKEN secret in GitHub repo settings."
        )

    # 3. Playwright is installed
    try:
        from playwright.async_api import async_playwright  # noqa: F401
    except ImportError:
        raise PipelineError(
            "Playwright is not installed",
            "The playwright Python package is missing.",
            "Run: pip install playwright && playwright install chromium"
        )

    # 4. Manifest file is valid JSON (if it exists)
    from opm_pipeline.config import MANIFEST_PATH
    if MANIFEST_PATH.exists():
        import json
        try:
            with open(MANIFEST_PATH) as f:
                json.load(f)
        except json.JSONDecodeError as e:
            raise PipelineError(
                f"Manifest file is corrupted: {e}",
                f"The file {MANIFEST_PATH} contains invalid JSON. This can happen if "
                "a previous run was interrupted mid-write.",
                f"Delete {MANIFEST_PATH} and run with --rebuild-manifest to regenerate it, "
                "or fix the JSON manually."
            )

    print("All preflight checks passed.")


def _find_prior_file(data_type: str) -> str | None:
    """Find the most recent file of the same data type on HF.

    Returns the parquet filename (e.g. 'accessions_202512.parquet') or None.
    """
    from huggingface_hub import list_repo_files
    from opm_pipeline.config import HF_REPO

    try:
        files = list_repo_files(HF_REPO, repo_type="dataset")
        matches = sorted([f for f in files if f.startswith(data_type) and f.endswith('.parquet')])
        return matches[-1] if matches else None
    except Exception:
        return None


async def run_daily(token: str, data_types: list[str], start_date: str, end_date: str,
                    rebuild_manifest: bool = False, dry_run: bool = False):
    """Main daily pipeline orchestration."""
    from playwright.async_api import async_playwright
    from opm_pipeline.scraper import setup_page, get_site_manifest, download_file_from_card, set_filters, get_card_filename
    from opm_pipeline.converter import convert_to_parquet, get_parquet_metadata
    from opm_pipeline.uploader import upload_to_huggingface, download_existing_parquet
    from opm_pipeline.manifest import load_manifest, save_manifest, compare_manifests, update_manifest_entry
    from opm_pipeline.differ import generate_diff_summary, summarize_new_file
    from opm_pipeline.reporter import generate_report

    # Step 1: Load or rebuild manifest
    if rebuild_manifest:
        print("Rebuilding manifest from HuggingFace...")
        from opm_pipeline.manifest import build_manifest_from_hf
        stored_manifest = build_manifest_from_hf(token)
        save_manifest(stored_manifest)
        print(f"Built manifest with {len(stored_manifest)} entries")
        return
    else:
        stored_manifest = load_manifest()
        print(f"Loaded manifest with {len(stored_manifest)} entries")

    # Step 2: Scrape OPM site for current file listing
    print("Scanning OPM site...")
    async with async_playwright() as playwright:
        browser, context, page = await setup_page(playwright)

        try:
            site_manifest = await get_site_manifest(page, data_types, start_date, end_date)

            if len(site_manifest) == 0:
                raise PipelineError(
                    "OPM site returned zero files",
                    "The scraper connected to data.opm.gov but found no downloadable files. "
                    "This usually means OPM changed their page layout or the site is temporarily broken.",
                    "1. Visit https://data.opm.gov/explore-data/data/data-downloads in a browser and check if it looks normal.\n"
                    "2. If the site looks different, the Playwright selectors in opm_pipeline/scraper.py need updating.\n"
                    "3. If the site looks fine, this may be a temporary issue — wait and try again tomorrow."
                )

            print(f"Found {len(site_manifest)} files on OPM site")

            # Step 3: Compare manifests
            changes = compare_manifests(stored_manifest, site_manifest)
            print(f"New: {len(changes['new'])}, Updated: {len(changes['updated'])}, Unchanged: {len(changes['unchanged'])}")

            if not changes["new"] and not changes["updated"]:
                print("No changes detected. Exiting.")
                return

            # Step 4: Process changed files
            DOWNLOAD_DIR.mkdir(parents=True, exist_ok=True)
            PARQUET_DIR.mkdir(parents=True, exist_ok=True)

            diffs = {}
            new_summaries = {}
            failed_files = []
            changed_keys = changes["new"] + changes["updated"]

            for key in changed_keys:
                site_entry = site_manifest[key]
                data_type = site_entry["data_type"].capitalize()
                card_filename = site_entry["filename"]
                parquet_name = card_filename.replace('.csv', '') + ".parquet"

                print(f"\nProcessing: {parquet_name}")

                try:
                    # Navigate to the right data type page
                    total = await set_filters(page, data_type, start_date, end_date)
                    if total == 0:
                        failed_files.append((key, "No files found after setting filters"))
                        continue

                    # Set to 100 items per page
                    try:
                        rows_dropdown = page.get_by_label("rows per page")
                        await rows_dropdown.select_option('100')
                        await asyncio.sleep(2)
                    except Exception:
                        pass

                    # Find the right card and download
                    downloaded = False
                    while True:
                        buttons = page.locator('button[aria-label^="Download options for"]')
                        count = await buttons.count()

                        for i in range(count):
                            current_filename = await get_card_filename(page, i)
                            if current_filename != card_filename:
                                continue

                            # Find comparison file for diffing
                            old_parquet_path = None
                            compare_label = None
                            if key in changes["updated"]:
                                old_parquet_path = download_existing_parquet(parquet_name, token)
                                compare_label = f"previous version of {parquet_name}"
                            elif key in changes["new"]:
                                prior_file = _find_prior_file(site_entry["data_type"])
                                if prior_file:
                                    old_parquet_path = download_existing_parquet(prior_file, token)
                                    compare_label = prior_file

                            if old_parquet_path and compare_label:
                                print(f"  Comparing against: {compare_label}")

                            # Download new file
                            csv_path = await download_file_from_card(page, i, DOWNLOAD_DIR)
                            parquet_path = convert_to_parquet(csv_path, PARQUET_DIR)
                            metadata = get_parquet_metadata(parquet_path)

                            # Always diff if we have a comparison file
                            if old_parquet_path:
                                diff = generate_diff_summary(old_parquet_path, parquet_path)
                                diff["compared_to"] = compare_label
                                diffs[key] = diff
                            else:
                                # No prior file at all — first ever upload of this type
                                new_summaries[key] = summarize_new_file(parquet_path)

                            # Upload to HuggingFace
                            if dry_run:
                                print(f"  [DRY RUN] Would upload {parquet_name} to {HF_REPO}")
                            else:
                                upload_to_huggingface(parquet_path, parquet_name, token)
                                print(f"  Uploaded {parquet_name} to {HF_REPO}")

                            # Update manifest
                            update_manifest_entry(stored_manifest, key, site_entry, metadata)

                            # Cleanup
                            csv_path.unlink()
                            parquet_path.unlink()

                            downloaded = True
                            break

                        if downloaded:
                            break

                        next_button = page.locator('button[aria-label="Go to next page"]')
                        if await next_button.is_disabled():
                            failed_files.append((key, "Card not found on any page"))
                            break
                        await next_button.click()
                        await asyncio.sleep(2)

                except Exception as e:
                    failed_files.append((key, str(e)))
                    print(f"  ERROR processing {key}: {e}")
                    # Try to dismiss any open popups
                    try:
                        await page.keyboard.press('Escape')
                    except Exception:
                        pass
                    continue

        finally:
            await browser.close()

    # Step 5: Save manifest (even if some files failed — save what we got)
    save_manifest(stored_manifest)
    print(f"\nManifest saved with {len(stored_manifest)} entries")

    # Step 6: Generate report and create issue
    report = generate_report(changes, diffs, new_summaries)

    if failed_files:
        report += "\n\n## Errors\n\n"
        report += "The following files failed to process:\n\n"
        report += "| File | Error |\n|------|-------|\n"
        for key, err in failed_files:
            report += f"| `{key}` | {err[:100]} |\n"

    print("\n" + report)

    today = date.today()
    n_success = len(changed_keys) - len(failed_files)
    title = f"OPM Data Update - {today.isoformat()} ({len(changes['new'])} new, {len(changes['updated'])} updated"
    if failed_files:
        title += f", {len(failed_files)} failed"
    title += ")"

    issue_url = create_github_issue(title, report)
    if issue_url:
        print(f"\nGitHub Issue: {issue_url}")

    # Fail the run if any files failed, so the Action shows red
    if failed_files:
        print(f"\n{len(failed_files)} files failed to process.")
        sys.exit(1)


def report_failure(error: Exception):
    """Create a GitHub Issue explaining the failure with diagnosis and fix steps."""
    today = date.today()

    if isinstance(error, PipelineError):
        body = f"""## Pipeline Failed - {today.isoformat()}

### What happened

{error}

### Diagnosis

{error.diagnosis}

### How to fix

{error.fix}
"""
    else:
        body = f"""## Pipeline Failed - {today.isoformat()}

### What happened

An unexpected error occurred:

```
{traceback.format_exc()}
```

### Diagnosis

This is an unhandled error. Common causes:
- OPM changed their website layout (look for Playwright selector errors like "Timeout" or "strict mode violation")
- Network issues (look for connection errors)
- HuggingFace API changes (look for HTTP errors)
- Disk space (Employment files are ~780 MB)

### How to fix

1. Check the [Actions log](../../actions) for the full error output
2. If it mentions Playwright timeouts or selectors, OPM likely changed their site — `opm_pipeline/scraper.py` needs updating
3. If it's a network error, wait and re-run from the Actions tab (click "Run workflow")
4. If it's a HuggingFace error, check that the HF_TOKEN secret is still valid
"""

    title = f"Pipeline FAILED - {today.isoformat()}"
    create_github_issue(title, body)


async def main():
    parser = argparse.ArgumentParser(description="Daily OPM data pipeline")
    parser.add_argument("--token", default=os.environ.get("HF_TOKEN"), help="HuggingFace token")
    parser.add_argument("--types", nargs="+", default=DATA_TYPES, help="Data types to check")
    parser.add_argument("--start", default=START_DATE, help="Start date (YYYY-MM-DD)")
    parser.add_argument("--end", default=END_DATE, help="End date (YYYY-MM-DD)")
    parser.add_argument("--rebuild-manifest", action="store_true",
                        help="Rebuild manifest from existing HuggingFace repos")
    parser.add_argument("--dry-run", action="store_true",
                        help="Download and process files but skip uploading to HuggingFace")
    args = parser.parse_args()

    try:
        preflight_checks(args.token)
        await run_daily(args.token, args.types, args.start, args.end, args.rebuild_manifest, args.dry_run)
    except PipelineError as e:
        print(f"\nPIPELINE ERROR: {e}")
        print(f"DIAGNOSIS: {e.diagnosis}")
        print(f"FIX: {e.fix}")
        report_failure(e)
        sys.exit(1)
    except Exception as e:
        print(f"\nUNEXPECTED ERROR: {e}")
        traceback.print_exc()
        report_failure(e)
        sys.exit(1)


if __name__ == "__main__":
    asyncio.run(main())
