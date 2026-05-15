"""Daily OPM data pipeline entry point.

Checks OPM site for new/updated files, downloads and uploads them to HuggingFace,
and creates a GitHub Issue summarizing changes. On failure, creates a diagnostic
issue explaining what went wrong.
"""

from __future__ import annotations

import asyncio
import argparse
import json
import os
import sys
import traceback
from datetime import date
from pathlib import Path

from opm_pipeline.config import (
    HF_TOKEN, HF_REPO, DATA_TYPES, START_DATE, END_DATE,
    DOWNLOAD_DIR, PARQUET_DIR, card_name_to_hf_path,
    PHANTOM_V3_EMPLOYMENT_KEYS,
)
from opm_pipeline.reporter import create_github_issue


# How many file uploads to bundle into a single HuggingFace commit.
# HF rate-limits commits at 128/hour per repo, so per-file commits explode
# (and 429) on big refreshes (e.g. when OPM bumps every month from v2 to v3).
# Batching ~50 files/commit means ~14 commits for a full refresh — well under
# the limit — while still checkpointing progress every 50 files. Override
# with $UPLOAD_BATCH_SIZE for one-off local runs that need smaller batches
# (e.g. to keep peak local disk small while processing the big Employment
# files one bite at a time).
UPLOAD_BATCH_SIZE = int(os.environ.get("UPLOAD_BATCH_SIZE", "50"))

# Cached list of files we know are pending download. Populated after a fresh
# OPM site scrape; consumed (and pruned of successful uploads) by subsequent
# runs so we don't burn OPM requests rescanning while there's still a backlog.
# When empty/missing, the pipeline falls back to a full scrape. Set
# OPM_FORCE_RESCAN=1 to force a fresh scrape regardless.
PENDING_PATH = Path("metadata/pending.json")


def load_pending_cache() -> list[dict]:
    if not PENDING_PATH.exists():
        return []
    try:
        with open(PENDING_PATH) as f:
            data = json.load(f)
            return data if isinstance(data, list) else []
    except Exception:
        return []


def save_pending_cache(entries: list[dict]) -> None:
    PENDING_PATH.parent.mkdir(parents=True, exist_ok=True)
    with open(PENDING_PATH, "w") as f:
        json.dump(entries, f, indent=2)

# After this many consecutive download failures, give up on the run. With
# 10-min request spacing each retry takes ~10 minutes of wall time, so the
# total worst-case wait is ~50 minutes before we bail. Override with
# $OPM_BLOCK_THRESHOLD if you want to be more or less aggressive.
DOWNLOAD_BLOCK_THRESHOLD = int(os.environ.get("OPM_BLOCK_THRESHOLD", "5"))


class PipelineError(Exception):
    """Error with a human-readable diagnosis and fix instructions."""
    def __init__(self, message: str, diagnosis: str, fix: str):
        super().__init__(message)
        self.diagnosis = diagnosis
        self.fix = fix


def preflight_checks(token: str, need_playwright: bool = True):
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

    # 3. Playwright is installed (not needed for --rebuild-manifest)
    if need_playwright:
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


def _find_prior_file(data_type: str, current_hf_path: str) -> str | None:
    """Find the best comparison file for a new upload.

    Prefers a prior version of the same month (e.g. v1 when uploading v2).
    Falls back to the most recent file from an older month.
    """
    import re
    from huggingface_hub import list_repo_files
    from opm_pipeline.config import HF_REPO, hf_path_to_date

    current_date = hf_path_to_date(current_hf_path)
    if not current_date:
        return None

    # Parse version from current path
    ver_match = re.search(r'_v(\d+)\.parquet$', current_hf_path)
    current_version = int(ver_match.group(1)) if ver_match else 0

    prefix = f"{data_type.lower()}/"
    try:
        files = list_repo_files(HF_REPO, repo_type="dataset")
        same_month = []
        older_month = []
        for f in files:
            if not f.startswith(prefix) or not f.endswith('.parquet'):
                continue
            if f == current_hf_path:
                continue
            fdate = hf_path_to_date(f)
            if not fdate:
                continue
            fver_match = re.search(r'_v(\d+)\.parquet$', f)
            fver = int(fver_match.group(1)) if fver_match else 0
            if fdate == current_date and fver < current_version:
                same_month.append((fver, f))
            elif fdate < current_date:
                older_month.append((fdate, fver, f))

        # Prefer highest prior version of same month
        if same_month:
            return max(same_month, key=lambda x: x[0])[1]
        if not older_month:
            return None
        # Most recent older month, highest version of that month
        older_month.sort(key=lambda x: (x[0], x[1]), reverse=True)
        return older_month[0][2]
    except Exception:
        return None


def _flush_upload_batch(buffer, stored_manifest, token, dry_run, test, failed_files) -> int:
    """Commit all buffered files to HF in a single commit, then update the manifest.

    Buffer entries are dicts: {parquet_path, hf_path, key, site_entry, metadata}.
    On success: updates manifest, saves to disk, deletes local parquet files.
    On failure: marks every file in the batch as failed (caller can decide what to do).

    Buffer is cleared in either case. Returns the number of files actually uploaded
    (0 if the batch commit failed) so the caller can decide whether the run made
    enough progress to be worth chaining.
    """
    if not buffer:
        return 0

    from huggingface_hub import HfApi, CommitOperationAdd
    from opm_pipeline.manifest import save_manifest, update_manifest_entry

    print(f"\n  Flushing batch of {len(buffer)} files to HF as one commit...")

    if dry_run:
        print(f"  [DRY RUN] Would batch-commit {len(buffer)} files")
        committed = True
    else:
        try:
            ops = [
                CommitOperationAdd(path_in_repo=e["hf_path"], path_or_fileobj=str(e["parquet_path"]))
                for e in buffer
            ]
            HfApi().create_commit(
                repo_id=HF_REPO,
                repo_type="dataset",
                token=token,
                operations=ops,
                commit_message=f"Add/update {len(ops)} files",
            )
            print(f"  Committed batch of {len(buffer)} files to {HF_REPO}")
            committed = True
        except Exception as exc:
            print(f"  ERROR committing batch: {exc}")
            for entry in buffer:
                failed_files.append((entry["key"], _sanitize(str(exc))[:200]))
            committed = False

    n_uploaded = 0
    if committed:
        for entry in buffer:
            update_manifest_entry(stored_manifest, entry["key"], entry["site_entry"], entry["metadata"])
        if not test:
            save_manifest(stored_manifest)
        for entry in buffer:
            try:
                entry["parquet_path"].unlink()
            except Exception:
                pass
        n_uploaded = len(buffer)

    buffer.clear()
    return n_uploaded


async def run_daily(token: str, data_types: list[str], start_date: str, end_date: str,
                    rebuild_manifest: bool = False, dry_run: bool = False, test: bool = False):
    """Main daily pipeline orchestration."""
    from playwright.async_api import async_playwright
    from opm_pipeline.scraper import setup_page, get_site_manifest
    from opm_pipeline.direct_download import download as direct_download, stem_from_hf_path
    from opm_pipeline.converter import convert_to_parquet, get_parquet_metadata
    from opm_pipeline.uploader import upload_to_huggingface, download_existing_parquet
    from opm_pipeline.manifest import load_manifest, save_manifest, compare_manifests, update_manifest_entry, sync_manifest_with_hf
    from opm_pipeline.differ import generate_diff_summary, summarize_new_file
    from opm_pipeline.reporter import generate_report, generate_email_html

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

        # Sync from HF before diffing OPM. If a previous run uploaded files but
        # was killed (e.g. 6h timeout) before save_manifest, those uploads aren't
        # in the manifest and we'd reprocess them. This catches that.
        stored_manifest, recovered_from_hf = sync_manifest_with_hf(stored_manifest, token)
        if recovered_from_hf:
            print(f"Synced {len(recovered_from_hf)} HF entries into manifest (likely from a prior interrupted run)")
            save_manifest(stored_manifest)

    if test:
        print("TEST MODE: narrowing to January 2026, treating all found files as new")
        start_date = "2026-01-01"
        end_date = "2026-01-31"
        stored_manifest = {}  # pretend manifest is empty so all files appear new
        recovered_from_hf = []  # not meaningful in test mode

    # Step 2: Get the work list. Prefer the cached pending list (saved from a
    # prior fresh scrape) over rescanning OPM, since the OPM site is throttling
    # our IP pool aggressively and we don't want to burn requests rediscovering
    # files we already know about. A full scrape only happens when the cache is
    # empty (backlog cleared) or OPM_FORCE_RESCAN=1 is set.
    pending_cache = [] if test else load_pending_cache()
    force_rescan = os.environ.get("OPM_FORCE_RESCAN") == "1"
    # If the only entries left in pending are known phantoms, treat the cache
    # as empty so we fall through to a fresh OPM scan. Otherwise we'd skip the
    # same 10 phantoms every day and never detect new OPM releases.
    has_real_pending = any(p["key"] not in PHANTOM_V3_EMPLOYMENT_KEYS for p in pending_cache)
    use_cache = has_real_pending and not force_rescan and not test

    async with async_playwright() as playwright:
        browser, context, page = await setup_page(playwright)

        try:
            if use_cache:
                # Drop pending entries that are already in the manifest (i.e.
                # already on HF). This catches the case where a prior run
                # uploaded files but the workflow's "Commit manifest changes"
                # step skipped because of partial-failure gating — pending.json
                # in the repo is stale, but sync_manifest_with_hf above made
                # stored_manifest accurate. Without this, we'd re-download
                # everything we already have on HF.
                active_pending = [p for p in pending_cache if p["key"] not in stored_manifest]
                skipped_already_on_hf = len(pending_cache) - len(active_pending)
                if skipped_already_on_hf:
                    print(f"Pending cache had {skipped_already_on_hf} entries already on HF; dropping those")
                print(f"Using cached pending list ({len(active_pending)} files); skipping OPM site scan")
                site_manifest = {
                    p["key"]: {
                        "filename": p["filename"],
                        "version": p["version"],
                        "data_type": p["data_type"],
                    }
                    for p in active_pending
                }
                changes = {
                    "new": [p["key"] for p in active_pending if p.get("kind", "new") == "new"],
                    "updated": [p["key"] for p in active_pending if p.get("kind") == "updated"],
                    "unchanged": [],
                }
            else:
                print("Scanning OPM site...")
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
                # Save the discovered backlog so subsequent runs can skip the scan.
                if not test:
                    new_pending = [
                        {
                            "key": k,
                            "filename": site_manifest[k]["filename"],
                            "version": site_manifest[k]["version"],
                            "data_type": site_manifest[k]["data_type"],
                            "kind": "updated" if k in changes["updated"] else "new",
                        }
                        for k in changes["new"] + changes["updated"]
                    ]
                    save_pending_cache(new_pending)
                    pending_cache = new_pending

            # Filter out known phantom v3 employment URLs — OPM advertises them
            # on the listing but the chunked download endpoint 403s. Leaving
            # them in the queue burns the abort-on-failures budget and gates
            # all the real files behind them. See config.PHANTOM_V3_EMPLOYMENT_KEYS.
            phantom_hits = [k for k in changes["new"] + changes["updated"] if k in PHANTOM_V3_EMPLOYMENT_KEYS]
            if phantom_hits:
                print(f"Skipping {len(phantom_hits)} known-phantom v3 employment URLs (OPM listing advertises, blob returns 403):")
                for k in phantom_hits:
                    print(f"    - {k}")
                changes["new"] = [k for k in changes["new"] if k not in PHANTOM_V3_EMPLOYMENT_KEYS]
                changes["updated"] = [k for k in changes["updated"] if k not in PHANTOM_V3_EMPLOYMENT_KEYS]

            print(f"New: {len(changes['new'])}, Updated: {len(changes['updated'])}, Unchanged: {len(changes['unchanged'])}")
            if changes["new"]:
                print("  New files to download:")
                for f in changes["new"]:
                    print(f"    - {f}")
            if changes["updated"]:
                print("  Updated files to download:")
                for f in changes["updated"]:
                    print(f"    - {f}")

            # Optional cap via OPM_MAX_FILES_PER_RUN (default 0 = no cap).
            # Used to slice big backlogs across multiple runs when desired;
            # the workflow chains another run if this one had no failures.
            full_changed_keys = changes["new"] + changes["updated"]
            max_files = int(os.environ.get("OPM_MAX_FILES_PER_RUN", "0") or "0")
            files_remaining = 0
            if max_files > 0 and len(full_changed_keys) > max_files:
                selected = set(full_changed_keys[:max_files])
                files_remaining = len(full_changed_keys) - max_files
                print(
                    f"  Capping run at {max_files} files "
                    f"({files_remaining} will retry in the next run)"
                )
                changes["new"] = [k for k in changes["new"] if k in selected]
                changes["updated"] = [k for k in changes["updated"] if k in selected]

            # Emit step outputs for downstream workflow steps (e.g. email notification).
            # recovered_from_hf entries also count as "changes worth telling subscribers
            # about" since they reflect an OPM data refresh whose ingestion got split
            # across multiple runs.
            github_output = os.environ.get("GITHUB_OUTPUT")
            if github_output:
                has_changes = (
                    "true" if (changes["new"] or changes["updated"] or recovered_from_hf)
                    else "false"
                )
                parts = []
                if changes["new"]:
                    parts.append(f"{len(changes['new'])} new files")
                if changes["updated"]:
                    parts.append(f"{len(changes['updated'])} updated files")
                if recovered_from_hf:
                    parts.append(f"{len(recovered_from_hf)} recovered from prior run")
                email_subject = f"New EHRI data available on OPM: {', '.join(parts)}"
                email_subject = email_subject[:150]  # Buttondown subject line limit
                changed_keys = changes["new"] + changes["updated"]
                with open(github_output, "a") as _gho:
                    _gho.write(f"new_count={len(changes['new'])}\n")
                    _gho.write(f"updated_count={len(changes['updated'])}\n")
                    _gho.write(f"recovered_count={len(recovered_from_hf)}\n")
                    _gho.write(f"has_changes={has_changes}\n")
                    _gho.write(f"changed_keys={','.join(changed_keys)}\n")
                    _gho.write(f"email_subject={email_subject}\n")
                    _gho.write(f"files_remaining={files_remaining}\n")

            if not changes["new"] and not changes["updated"] and not recovered_from_hf:
                print("No changes detected. Exiting.")
                return

            # Step 4: Process changed files
            DOWNLOAD_DIR.mkdir(parents=True, exist_ok=True)
            PARQUET_DIR.mkdir(parents=True, exist_ok=True)

            diffs = {}
            new_summaries = {}
            failed_files = []
            changed_keys = changes["new"] + changes["updated"]
            # Files we actually started processing (excludes ones skipped
            # because the abort broke the loop early). Used downstream to
            # compute success_count without overcounting untried files.
            attempted_keys = []
            # Buffer of files awaiting batched upload — see _flush_upload_batch.
            upload_buffer = []
            # Tracks consecutive download/network errors. If this hits
            # DOWNLOAD_BLOCK_THRESHOLD we assume OPM is having a wider
            # problem (down, mass-403, network) and bail so we don't keep
            # hammering. Reset to 0 on every successful file.
            consecutive_dl_failures = 0
            aborted_after_failures = False

            for i, key in enumerate(changed_keys):
                # Optional inter-request spacing if OPM_REQUEST_SPACING_SEC is set.
                # No default delay — OPM's chunked endpoint hasn't shown any
                # rate-limiting behavior in testing.
                if i > 0:
                    spacing = int(os.environ.get("OPM_REQUEST_SPACING_SEC", "0"))
                    if spacing > 0:
                        print(f"  Sleeping {spacing}s before next request...")
                        await asyncio.sleep(spacing)

                attempted_keys.append(key)
                site_entry = site_manifest[key]
                hf_path = key  # key is the versioned HF path (e.g. accessions/accessions_202511_v3.parquet)

                print(f"\nProcessing: {hf_path}")

                try:
                    # Find comparison file for diffing (HF lookup, no browser needed).
                    old_parquet_path = None
                    compare_label = None
                    if key in changes["updated"]:
                        old_parquet_path = download_existing_parquet(hf_path, token)
                        compare_label = f"previous version of {hf_path}"
                    elif key in changes["new"]:
                        prior_file = _find_prior_file(site_entry["data_type"], hf_path)
                        if prior_file:
                            old_parquet_path = download_existing_parquet(prior_file, token)
                            compare_label = prior_file

                    if old_parquet_path and compare_label:
                        print(f"  Comparing against: {compare_label}")

                    # Download the file via plain HTTP. OPM's chunked download
                    # endpoint serves files to unauthenticated GET — no cookies,
                    # no user-agent gating, no Playwright/browser needed.
                    stem = stem_from_hf_path(key)
                    csv_path = DOWNLOAD_DIR / f"{stem}.txt"
                    await asyncio.to_thread(direct_download, stem, csv_path)

                    parquet_path = convert_to_parquet(csv_path, PARQUET_DIR)
                    metadata = get_parquet_metadata(parquet_path)

                    # Always diff if we have a comparison file
                    if old_parquet_path:
                        diff = generate_diff_summary(old_parquet_path, parquet_path)
                        diff["compared_to"] = compare_label

                        # Detect globally-new columns: in new file but not in
                        # stored manifest for the comparison file. Catches the case
                        # where OPM adds a column to ALL files at once so both old
                        # and new parquets have it and schema diff shows nothing.
                        # For updated files, the manifest key IS the current key.
                        # For new files, compare_label is the actual prior HF path.
                        if key in changes["updated"]:
                            prior_key = key if key in stored_manifest else None
                        else:
                            prior_key = compare_label if compare_label in stored_manifest else None
                        if prior_key:
                            stored_cols = set(stored_manifest[prior_key].get("columns", []))
                            if stored_cols:  # Skip if no column data (e.g. manifest rebuilt without downloading)
                                import pandas as _pd
                                new_cols = set(_pd.read_parquet(parquet_path).columns)
                                already_flagged = set(diff["schema"].get("added", []))
                                globally_new = sorted(new_cols - stored_cols - already_flagged)
                                if globally_new:
                                    diff["globally_new_columns"] = globally_new
                                    # Find which prior months of this data type also lack these columns
                                    from opm_pipeline.config import hf_path_to_date as _hfdate
                                    dtype = site_entry["data_type"]
                                    affected_dates = {
                                        _hfdate(mk)
                                        for mk, mv in stored_manifest.items()
                                        if mv.get("data_type") == dtype
                                        and mk != key
                                        and mv.get("columns")
                                        and any(c not in mv["columns"] for c in globally_new)
                                        and _hfdate(mk)
                                    }
                                    affected = [d.strftime("%B %Y") for d in sorted(affected_dates)]
                                    diff["globally_new_affected_months"] = affected
                                    print(f"  Globally new columns detected: {globally_new} (affects {len(affected)} prior months)")

                        diffs[key] = diff
                    else:
                        # No prior file at all — first ever upload of this type
                        new_summaries[key] = summarize_new_file(parquet_path)

                    # Buffer the file for batched HF commit; the actual upload
                    # and manifest update happen in _flush_upload_batch (every
                    # UPLOAD_BATCH_SIZE files + once at the end). This keeps us
                    # under HF's 128 commits/hour rate limit on big refreshes.
                    csv_path.unlink()
                    upload_buffer.append({
                        "parquet_path": parquet_path,
                        "hf_path": hf_path,
                        "key": key,
                        "site_entry": site_entry,
                        "metadata": metadata,
                    })
                    consecutive_dl_failures = 0  # success — reset abort counter

                except Exception as e:
                    failed_files.append((key, str(e)))
                    print(f"  ERROR processing {key}: {e}")

                    # Fail-fast for local debugging: bail on first download error
                    # instead of pressing on through retries.
                    if os.environ.get("OPM_FAIL_FAST"):
                        raise

                    # Heuristic: which failures count toward the abort threshold.
                    # 403s are file-specific (almost always a newly-discovered
                    # phantom URL — see PHANTOM_V3_EMPLOYMENT_KEYS) — don't
                    # treat them as systemic. Transient network errors
                    # (timeout, connection reset, incomplete read) do count,
                    # since N in a row likely means OPM is down. Parse/schema
                    # errors don't count.
                    err_lc = str(e).lower()
                    is_403 = "403" in err_lc and ("forbidden" in err_lc or "not found" in err_lc)
                    is_transient_net = any(s in err_lc for s in ("timeout", "connection", "incomplete", "ssl"))
                    if is_403:
                        consecutive_dl_failures = 0
                    elif is_transient_net:
                        consecutive_dl_failures += 1
                    else:
                        consecutive_dl_failures = 0

                    if consecutive_dl_failures >= DOWNLOAD_BLOCK_THRESHOLD:
                        print(
                            f"  Aborting: {DOWNLOAD_BLOCK_THRESHOLD} consecutive "
                            f"download failures — OPM may be temporarily "
                            f"unavailable. Stopping early; tomorrow's scheduled "
                            f"run will retry."
                        )
                        aborted_after_failures = True
                        break

                    continue

                # Flush a batch when it gets full so progress is preserved
                # if the run dies later.
                if len(upload_buffer) >= UPLOAD_BATCH_SIZE:
                    _flush_upload_batch(upload_buffer, stored_manifest, token, dry_run, test, failed_files)

            # Final flush for whatever's left in the buffer.
            _flush_upload_batch(upload_buffer, stored_manifest, token, dry_run, test, failed_files)

        finally:
            await browser.close()

    # Step 5: Save manifest (even if some files failed — save what we got)
    if test:
        print("\nTEST MODE: skipping manifest save")
    else:
        save_manifest(stored_manifest)
        print(f"\nManifest saved with {len(stored_manifest)} entries")

        # Prune successfully-uploaded keys from the pending cache. Failures
        # and unattempted files stay so the next run retries them. When the
        # cache empties, the next run does a fresh scrape to pick up anything
        # OPM published in the meantime.
        upload_set = {k for k in attempted_keys if k not in {fk for fk, _ in failed_files}}
        if pending_cache and upload_set:
            remaining_cache = [p for p in pending_cache if p["key"] not in upload_set]
            save_pending_cache(remaining_cache)
            print(f"Pending cache: {len(remaining_cache)} files still queued (was {len(pending_cache)})")

    # Emit the final pipeline_status as a step output for visibility in the
    # workflow logs.
    final_status = "aborted" if aborted_after_failures else "done"
    failed_keys_set = {fk for fk, _ in failed_files}
    # Only count files we actually attempted (not ones the abort broke past).
    uploaded_keys = [k for k in attempted_keys if k not in failed_keys_set]
    success_count = len(uploaded_keys)
    failed_keys = [fk for fk, _ in failed_files]
    github_output = os.environ.get("GITHUB_OUTPUT")
    if github_output:
        # Recompute email subject + has_changes from actual upload outcomes so
        # downstream notifications don't claim files that failed/were skipped.
        # Last-write-wins for repeated keys in $GITHUB_OUTPUT, so this overrides
        # the placeholders set earlier (before the loop ran).
        actual_has_changes = "true" if (uploaded_keys or recovered_from_hf) else "false"
        subject_parts = []
        if uploaded_keys:
            subject_parts.append(f"{len(uploaded_keys)} file{'s' if len(uploaded_keys) != 1 else ''} uploaded")
        if recovered_from_hf:
            subject_parts.append(f"{len(recovered_from_hf)} recovered from prior run")
        actual_email_subject = (
            f"New EHRI data available on OPM: {', '.join(subject_parts)}"[:150]
            if subject_parts else ""
        )
        with open(github_output, "a") as _gho:
            _gho.write(f"pipeline_status={final_status}\n")
            _gho.write(f"success_count={success_count}\n")
            _gho.write(f"failed_count={len(failed_files)}\n")
            _gho.write(f"has_changes={actual_has_changes}\n")
            if actual_email_subject:
                _gho.write(f"email_subject={actual_email_subject}\n")
            # Multiline outputs use the heredoc form; lists can grow long.
            _gho.write("uploaded_keys<<EOF\n")
            for k in uploaded_keys:
                _gho.write(f"{k}\n")
            _gho.write("EOF\n")
            _gho.write("failed_keys<<EOF\n")
            for k in failed_keys:
                _gho.write(f"{k}\n")
            _gho.write("EOF\n")
    if aborted_after_failures:
        print(f"\nPipeline status: ABORTED — uploaded what we got, waiting for tomorrow's cron.")

    # Step 6: Generate report and create issue
    report = generate_report(changes, diffs, new_summaries)

    if recovered_from_hf:
        # Files that a prior interrupted run uploaded to HF without saving the
        # manifest. They wouldn't otherwise appear in this run's report because
        # the manifest sync at startup already absorbed them. List them here so
        # nothing falls off the radar.
        report += "\n\n## Recovered from a prior interrupted run\n\n"
        report += (
            f"{len(recovered_from_hf)} file(s) were already on HuggingFace from a previous "
            f"run that didn't finish, so they're now in the manifest but skipped here "
            f"(no diff available — the upload happened in the earlier run):\n\n"
        )
        for key in sorted(recovered_from_hf):
            report += f"- `{key}`\n"

    if aborted_after_failures:
        report += "\n\n## Aborted: too many consecutive download failures\n\n"
        report += (
            f"Hit {DOWNLOAD_BLOCK_THRESHOLD} consecutive download failures, which "
            f"usually means OPM is having a wider problem (down, returning errors, "
            f"network). We uploaded whatever made it through and exited early. "
            f"Tomorrow's scheduled run will pick up wherever this one left off via "
            f"`sync_manifest_with_hf`.\n"
        )

    if failed_files:
        report += "\n\n## Errors\n\n"
        report += "The following files failed to process:\n\n"
        report += "| File | Error |\n|------|-------|\n"
        for key, err in failed_files:
            report += f"| `{key}` | {_sanitize(err[:100])} |\n"

    print("\n" + report)

    today = date.today()
    from opm_pipeline.reporter import _is_version_update
    from opm_pipeline.config import hf_path_to_date
    # Title reflects what actually uploaded — not what was queued. A 0/5
    # outcome is a 0-upload run, not a "5 updated, 5 failed" run.
    uploaded_set = set(uploaded_keys)
    new_months_up = [k for k in changes["new"] if k in uploaded_set and not _is_version_update(k)]
    new_versions_up = [k for k in changes["new"] if k in uploaded_set and _is_version_update(k)]
    updated_up = [k for k in changes["updated"] if k in uploaded_set]
    distinct_new_months = len({hf_path_to_date(k) for k in new_months_up if hf_path_to_date(k)})
    title_parts = []
    if new_months_up:
        title_parts.append(f"{distinct_new_months} new month{'s' if distinct_new_months > 1 else ''}")
    if new_versions_up:
        title_parts.append(f"{len(new_versions_up)} updated version{'s' if len(new_versions_up) > 1 else ''}")
    if updated_up:
        title_parts.append(f"{len(updated_up)} updated")
    if recovered_from_hf:
        title_parts.append(f"{len(recovered_from_hf)} recovered")
    if not title_parts:
        title_parts.append("no uploads")
    title = f"EHRI Data Update - {today.isoformat()} ({', '.join(title_parts)}"
    if failed_files:
        title += f", {len(failed_files)} failed"
    title += ")"

    issue_url = create_github_issue(title, report)
    if issue_url:
        print(f"\nGitHub Issue: {issue_url}")

    # Write HTML email body
    email_html = generate_email_html(changes, diffs, new_summaries, today)
    if recovered_from_hf:
        email_html += (
            f"\n<p><strong>Plus {len(recovered_from_hf)} file(s) recovered "
            f"from a previous interrupted run</strong> "
            f"(already on HuggingFace, not re-processed in this run — "
            f"see issue for the list).</p>"
        )
    if issue_url:
        email_html += f"\n<p><a href='{issue_url}'>Full diff report</a></p>"
    with open("email_body.txt", "w") as f:
        f.write(email_html)

    # Fail the run only if NOTHING uploaded successfully. Partial success
    # (some uploaded + some failed — typically a newly-discovered phantom
    # URL or a transient network blip) exits 0 so the workflow's commit-
    # manifest + chain-next-run steps fire. Failures are still surfaced via
    # failed_count in the run summary + Buttondown email subject.
    if failed_files and not aborted_after_failures and not uploaded_keys:
        print(f"\n{len(failed_files)} files failed to process and nothing uploaded.")
        sys.exit(1)
    if failed_files:
        print(f"\n{len(failed_files)} files failed but {len(uploaded_keys)} uploaded — partial success.")


def _sanitize(text: str) -> str:
    """Remove any tokens or secrets from text before putting it in a GitHub Issue."""
    import re
    # Redact HuggingFace tokens (hf_...)
    text = re.sub(r'hf_[A-Za-z0-9]{20,}', '[REDACTED]', text)
    # Redact anything that looks like a bearer token
    text = re.sub(r'Bearer\s+\S+', 'Bearer [REDACTED]', text)
    # Redact the HF_TOKEN env var value if it somehow appears
    token = os.environ.get("HF_TOKEN", "")
    if token and len(token) > 5:
        text = text.replace(token, '[REDACTED]')
    return text


def report_failure(error: Exception):
    """Create a GitHub Issue explaining the failure with diagnosis and fix steps."""
    today = date.today()

    if isinstance(error, PipelineError):
        body = f"""## Pipeline Failed - {today.isoformat()}

### What happened

{_sanitize(str(error))}

### Diagnosis

{_sanitize(error.diagnosis)}

### How to fix

{error.fix}
"""
    else:
        body = f"""## Pipeline Failed - {today.isoformat()}

### What happened

An unexpected error occurred:

```
{_sanitize(traceback.format_exc())}
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
    parser.add_argument("--test", action="store_true",
                        help="Test mode: January 2026 only, treat all files as new, triggers full report/email")
    args = parser.parse_args()

    try:
        preflight_checks(args.token, need_playwright=not args.rebuild_manifest)
        await run_daily(args.token, args.types, args.start, args.end, args.rebuild_manifest, args.dry_run, args.test)
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
