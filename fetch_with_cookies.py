"""Drain the OPM pending list using cookies stolen from your real Chrome.

OPM is fronted by Akamai Bot Manager. Real-browser downloads work because
Chrome solves Akamai's JS challenge → Akamai sets a valid `_abck` cookie →
subsequent download requests pass. This script piggybacks on that: extract
the cookies from your Chrome's encrypted cookie store, then make the same
download requests with curl_cffi impersonating Chrome's TLS handshake. To
Akamai it looks like the same browser session continuing the download flow.

Cookies expire — typically 30 min to a few hours for _abck. If a request
starts 403'ing, stop, open OPM in your real Chrome, click around for a few
seconds (browse the data downloads page, maybe download one file manually),
then restart this script. We re-read cookies on each start.

Requirements:
    pip install curl_cffi browser_cookie3
    Your real Chrome must have been used to load https://data.opm.gov/explore-data/data/data-downloads
    recently (so the cookie jar has fresh _abck etc).

Usage:
    ./venv/bin/python fetch_with_cookies.py            # process all pending
    ./venv/bin/python fetch_with_cookies.py --limit 5  # cap to first N
    ./venv/bin/python fetch_with_cookies.py --spacing 30  # seconds between requests
    ./venv/bin/python fetch_with_cookies.py --dry-run  # download + parse but skip HF upload
"""
from __future__ import annotations

import argparse
import json
import re
import sys
import time
from datetime import date
from pathlib import Path

import browser_cookie3
from curl_cffi import requests as curl_requests
from huggingface_hub import HfApi, CommitOperationAdd

from opm_pipeline.config import HF_REPO, HF_TOKEN, DOWNLOAD_DIR, PARQUET_DIR
from opm_pipeline.converter import convert_to_parquet, get_parquet_metadata
from opm_pipeline.manifest import load_manifest, save_manifest, update_manifest_entry


PENDING_PATH = Path("metadata/pending.json")

# Pin to your installed Chrome (148 as of 2026-05-10). curl_cffi's impersonate
# value just needs to be a recent enough Chrome that Akamai treats the TLS
# fingerprint as legitimate; the UA we override below is what shows in headers.
IMPERSONATE = "chrome"  # latest curl_cffi knows about
USER_AGENT = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/148.0.0.0 Safari/537.36"
)
REFERER = "https://data.opm.gov/explore-data/data/data-downloads"
ORIGIN = "https://data.opm.gov"


def load_pending() -> list[dict]:
    if not PENDING_PATH.exists():
        return []
    with open(PENDING_PATH) as f:
        data = json.load(f)
    return data if isinstance(data, list) else []


def save_pending(entries: list[dict]) -> None:
    PENDING_PATH.parent.mkdir(parents=True, exist_ok=True)
    with open(PENDING_PATH, "w") as f:
        json.dump(entries, f, indent=2)


def build_url(hf_path: str) -> str:
    """Map an HF path ('employment/employment_202503_v3.parquet') to the OPM
    chunked download URL. The URL format mirrors what OPM's UI generates
    when you click the TXT button in a browser."""
    stem = Path(hf_path).stem  # 'employment_202503_v3'
    server_stem = re.sub(r"_v(\d+)$", r"_\1", stem)  # 'employment_202503_3'
    today = date.today().isoformat()
    return (
        f"https://data.opm.gov/api/blob/download/chunked/{server_stem}.txt"
        f"?customFileName={server_stem}_{today}.txt&fileType=application/json"
    )


def parse_hf_path(hf_path: str) -> tuple[str, str, int]:
    """Returns (data_type, yyyymm, version)."""
    m = re.match(r"(\w+)/\1_(\d{6})_v(\d+)\.parquet$", hf_path)
    if not m:
        raise ValueError(f"Cannot parse hf_path: {hf_path}")
    return m.group(1), m.group(2), int(m.group(3))


def grab_cookies() -> dict[str, str]:
    """Extract data.opm.gov cookies from your Chrome cookie store. macOS
    Chrome encrypts cookie values via Keychain; browser_cookie3 handles the
    decryption transparently (will prompt for Keychain access on first run)."""
    jar = browser_cookie3.chrome(domain_name="data.opm.gov")
    cookies = {c.name: c.value for c in jar}
    return cookies


def download_one(url: str, cookies: dict[str, str], dest_path: Path) -> Path:
    headers = {
        "User-Agent": USER_AGENT,
        "Accept": "*/*",
        "Accept-Language": "en-US,en;q=0.9",
        "Referer": REFERER,
        "Origin": ORIGIN,
        "Sec-Fetch-Dest": "empty",
        "Sec-Fetch-Mode": "cors",
        "Sec-Fetch-Site": "same-origin",
    }
    with curl_requests.get(
        url,
        cookies=cookies,
        headers=headers,
        impersonate=IMPERSONATE,
        timeout=600,
        stream=True,
    ) as r:
        if r.status_code != 200:
            raise RuntimeError(f"HTTP {r.status_code} for {url}")
        with open(dest_path, "wb") as f:
            for chunk in r.iter_content(chunk_size=65536):
                if chunk:
                    f.write(chunk)
    return dest_path


def flush_batch(buffer: list[dict], manifest: dict, token: str) -> list[str]:
    """Commit buffered parquets in one HF commit, update manifest + pending,
    delete local parquets. Returns the list of HF paths uploaded."""
    if not buffer:
        return []
    print(f"  Flushing batch of {len(buffer)} to HuggingFace...")
    ops = [
        CommitOperationAdd(path_in_repo=e["hf_path"], path_or_fileobj=str(e["parquet_path"]))
        for e in buffer
    ]
    try:
        HfApi().create_commit(
            repo_id=HF_REPO,
            repo_type="dataset",
            token=token,
            operations=ops,
            commit_message=f"Add/update {len(ops)} files (local catch-up)",
        )
    except Exception as exc:
        print(f"  ERROR: HF commit failed: {exc}")
        return []
    uploaded = []
    for e in buffer:
        update_manifest_entry(
            manifest,
            e["hf_path"],
            {
                "filename": e["hf_path"],
                "version": e["version"],
                "opm_date": "",
                "data_type": e["data_type"],
            },
            e["metadata"],
        )
        uploaded.append(e["hf_path"])
    save_manifest(manifest)
    # Prune pending.json
    pending = load_pending()
    if pending:
        remaining = [p for p in pending if p["key"] not in set(uploaded)]
        save_pending(remaining)
        print(f"  Manifest + pending.json updated ({len(remaining)} files still pending).")
    # Delete local parquets
    for e in buffer:
        try:
            Path(e["parquet_path"]).unlink()
        except Exception:
            pass
    return uploaded


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--limit", type=int, default=0,
                        help="Stop after N successful uploads (0 = all)")
    parser.add_argument("--spacing", type=float, default=30.0,
                        help="Seconds between download requests")
    parser.add_argument("--batch-size", type=int, default=5,
                        help="Files per HF commit")
    parser.add_argument("--dry-run", action="store_true",
                        help="Download + convert, skip HF upload + manifest update")
    args = parser.parse_args()

    if not args.dry_run and not HF_TOKEN:
        print("ERROR: HF_TOKEN not set. Source your .env or pass --dry-run.")
        sys.exit(1)

    pending = load_pending()
    if not pending:
        print("metadata/pending.json is empty — nothing to do.")
        sys.exit(0)

    print(f"Pending: {len(pending)} files.")
    print("Extracting cookies from Chrome (may prompt for Keychain access)...")
    cookies = grab_cookies()
    print(f"  Pulled {len(cookies)} cookies for data.opm.gov. Akamai-looking ones: "
          f"{[k for k in cookies if k in ('_abck', 'ak_bmsc', 'bm_sv', 'akavpau_USADATA_PROD', 'ARRAffinity', 'ASLBSA')]}")
    if "_abck" not in cookies:
        print("WARN: No _abck cookie. Open https://data.opm.gov/explore-data/data/data-downloads in Chrome, click around a bit, then re-run.")

    DOWNLOAD_DIR.mkdir(parents=True, exist_ok=True)
    PARQUET_DIR.mkdir(parents=True, exist_ok=True)
    manifest = load_manifest()
    print(f"Loaded manifest with {len(manifest)} entries.")

    buffer: list[dict] = []
    n_uploaded = 0

    for i, p in enumerate(pending):
        if args.limit and n_uploaded >= args.limit:
            print(f"\nReached --limit {args.limit}. Stopping.")
            break

        hf_path = p["key"]
        if hf_path in manifest:
            print(f"[{i+1}/{len(pending)}] Skipping (already in manifest): {hf_path}")
            continue

        data_type, yyyymm, version = parse_hf_path(hf_path)
        url = build_url(hf_path)
        dest_name = re.sub(r"_v(\d+)$", r"_\1", Path(hf_path).stem) + ".txt"
        dest_path = DOWNLOAD_DIR / dest_name

        if i > 0 and args.spacing > 0:
            print(f"  Sleeping {args.spacing}s before next request...")
            time.sleep(args.spacing)

        print(f"[{i+1}/{len(pending)}] {hf_path}")
        try:
            download_one(url, cookies, dest_path)
        except Exception as exc:
            print(f"  ERROR downloading: {exc}")
            if "403" in str(exc):
                print("\nCookies look stale or invalidated. Open OPM in your real Chrome, "
                      "browse the data downloads page for a few seconds, then re-run this script. "
                      "Any files already uploaded are preserved in HF + pending.json.")
            else:
                print("  (non-403 — likely transient; re-run will retry)")
            break

        try:
            parquet_path = convert_to_parquet(dest_path, PARQUET_DIR)
            metadata = get_parquet_metadata(parquet_path)
        except Exception as exc:
            print(f"  ERROR converting: {exc}")
            dest_path.unlink(missing_ok=True)
            continue

        # Delete CSV immediately to save disk
        dest_path.unlink(missing_ok=True)

        if args.dry_run:
            print(f"  [DRY-RUN] Would upload {parquet_path.name} (rows={metadata.get('row_count')})")
            parquet_path.unlink()
            n_uploaded += 1
            continue

        buffer.append({
            "hf_path": hf_path,
            "parquet_path": parquet_path,
            "data_type": data_type,
            "version": version,
            "metadata": metadata,
        })
        n_uploaded += 1
        print(f"  Buffered ({len(buffer)}/{args.batch_size}). Rows: {metadata.get('row_count'):,}")

        if len(buffer) >= args.batch_size:
            flushed = flush_batch(buffer, manifest, HF_TOKEN)
            if not flushed:
                # Don't drop the buffer if commit failed — leave for the next run
                print("  Batch flush failed. Keeping parquets locally; re-run will retry.")
                break
            buffer = []

    # Final flush
    if buffer and not args.dry_run:
        print(f"\nFinal flush of {len(buffer)} buffered files...")
        flush_batch(buffer, manifest, HF_TOKEN)

    print(f"\nDone. Processed {n_uploaded} files this run.")
    remaining = len(load_pending())
    print(f"Pending after this run: {remaining}")


if __name__ == "__main__":
    main()
