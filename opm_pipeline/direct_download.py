"""Download OPM EHRI files via plain HTTP. No browser, no cookies, no headers.

The chunked-download endpoint at /api/blob/download/chunked/* serves files
without any bot-detection challenge. Plain `httpx.get(url)` returns 200 with
the file body — no User-Agent, no Referer, no cookies, no TLS impersonation.

This was confirmed 2026-05-15 from both residential (Comcast cable) and
datacenter (GH Actions Ubuntu) IPs. The 403s the pipeline experienced in
May 2026 were misdiagnosed as IP gating / Akamai bot detection; the actual
issue was that ~10 specific v3 employment URLs return 403 "Not found" because
OPM's listing page advertises Version: 3 for months whose v3 blob never
landed in storage. See PHANTOM_V3_EMPLOYMENT_KEYS in config.py for the skip
list, and the memory note `project_opm_akamai_block.md` for the full
post-mortem.

This is the primary downloader for the daily pipeline. The Playwright
session in scraper.py is still used to enumerate OPM's listing page (which
is a Blazor SPA), but the actual file downloads happen here.
"""
from __future__ import annotations

from datetime import date
from pathlib import Path
from typing import Optional

import httpx


BASE = "https://data.opm.gov/api/blob/download/chunked"


def build_url(server_stem: str) -> str:
    """Map a server stem ('employment_202511_2') to the OPM download URL.

    Matches the URL the Blazor UI generates when you click TXT. The
    customFileName and fileType query params are cosmetic but kept so the
    request looks identical to a real browser click in OPM's access logs.
    """
    today = date.today().isoformat()
    return (
        f"{BASE}/{server_stem}.txt"
        f"?customFileName={server_stem}_{today}.txt&fileType=application/json"
    )


def stem_from_hf_path(hf_path: str) -> str:
    """Convert HF-versioned path → OPM server stem.

    'employment/employment_202503_v3.parquet' -> 'employment_202503_3'
    """
    import re
    name = Path(hf_path).stem  # employment_202503_v3
    return re.sub(r"_v(\d+)$", r"_\1", name)


def download(
    server_stem: str,
    dest: Path,
    *,
    timeout: float = 600.0,
    proxy: Optional[str] = None,
    progress_every_mb: int = 50,
) -> Path:
    """Stream one OPM file to disk. Returns the path written.

    Raises httpx.HTTPStatusError on non-200. A 403 with body 'Not found' is
    most likely a phantom v3 URL — see PHANTOM_V3_EMPLOYMENT_KEYS in
    config.py.
    """
    url = build_url(server_stem)
    dest.parent.mkdir(parents=True, exist_ok=True)

    client_kwargs = {"timeout": timeout, "follow_redirects": True}
    if proxy:
        client_kwargs["proxy"] = proxy

    with httpx.Client(**client_kwargs) as client:
        with client.stream("GET", url) as r:
            r.raise_for_status()
            total = 0
            next_log = progress_every_mb * 1024 * 1024
            with open(dest, "wb") as f:
                for chunk in r.iter_bytes(chunk_size=65536):
                    f.write(chunk)
                    total += len(chunk)
                    if total >= next_log:
                        print(f"  {total // (1024 * 1024)} MB...")
                        next_log += progress_every_mb * 1024 * 1024
    return dest


if __name__ == "__main__":
    import argparse

    ap = argparse.ArgumentParser(description="Download one OPM file by server stem.")
    ap.add_argument("stem", help="e.g. accessions_202601_1, employment_202511_2")
    ap.add_argument("--out", type=Path, default=Path("downloads"))
    ap.add_argument("--proxy", help="HTTP(S) proxy URL, e.g. http://user:pass@host:port")
    args = ap.parse_args()

    dest = args.out / f"{args.stem}.txt"
    download(args.stem, dest, proxy=args.proxy)
    print(f"wrote {dest} ({dest.stat().st_size:,} bytes)")
