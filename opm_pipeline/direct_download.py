"""Download OPM EHRI files via plain HTTP. No browser, no Akamai dance.

Background — why this exists alongside fetch_with_cookies.py and scraper.py:

The chunked-download endpoint at /api/blob/download/chunked/* is fronted by
Akamai Bot Manager, and it started 403'ing the daily pipeline around
2026-05-07. What looked like a browser-fingerprint problem (and got treated
as one — Chrome UA, stealth init scripts, curl_cffi TLS impersonation,
laundered _abck cookies via browser_cookie3) is actually IP-reputation
gating at the source ASN.

Tested 2026-05-14 from a residential cable connection:
    >>> import httpx
    >>> r = httpx.get("https://data.opm.gov/api/blob/download/chunked/"
    ...               "accessions_202601_1.txt")
    >>> r.status_code, r.headers["content-type"]
    (200, 'application/octet-stream')

No User-Agent, no Referer, no cookies, no TLS impersonation. Pulled 14.9 MB
of real pipe-delimited accessions data in one shot. The /explore-data page
is *not* IP-gated (which is why scraper.py's list step keeps working from
GH Actions); only the download endpoint is.

Implications for deployment:
- From a residential IP, none of the cookie-stealing in fetch_with_cookies.py
  is load-bearing. This module is the simpler path.
- From a datacenter IP (Azure / AWS / GCP / DO / most VPN exits, which
  includes GitHub-hosted Actions runners), the same call 403s. To run the
  daily pipeline unattended you need residential egress:
    1. Self-hosted runner on a residential connection — cheapest, most
       robust. Any always-on box at home registered as a GH self-hosted
       runner. Zero ongoing cost.
    2. Residential proxy as a 1-line patch — pass `proxy=` to httpx.stream
       and keep running on GH-hosted Actions. Monthly volume is small
       (the employment file dominates at ~780 MB; everything else is tiny),
       so the cheapest residential plans ($5/mo class) cover it.
    3. Tailscale/WireGuard tunnel from Actions → home node → internet.
       Free if you already have that setup; more moving parts than #1.

If this module starts 403'ing from a residential IP too, that means OPM
tightened the gate further (per-cookie/JS challenge for everyone, not just
datacenter ASNs). At that point fall back to fetch_with_cookies.py and
re-evaluate.
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
    progress_every_mb: int = 10,
) -> Path:
    """Stream one OPM file to disk. Returns the path written.

    Raises httpx.HTTPStatusError on non-200. A 403 from a residential IP
    means the gate has gotten stricter — read the module docstring.
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
