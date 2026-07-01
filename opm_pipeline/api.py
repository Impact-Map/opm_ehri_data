"""OPM Federal Workforce Data public API client.

OPM publishes an official, documented, versioned REST API at
https://data.opm.gov/api/v1/files. Files are served as parquet directly
(zstd-compressed, all columns typed as string — the pipeline's format), so
there is no format-conversion step: download and mirror as-is.

Two endpoints are used:
  - GET /api/v1/files/{dataset}?current=true   -> JSON list of current files
  - GET /api/v1/files/{dataset}/{year}/{month}/{version}/download  -> parquet

`dataset` is one of accessions | separations | employment.
"""
from __future__ import annotations

import re
from pathlib import Path

import httpx

from .config import card_name_to_hf_path

API_BASE = "https://data.opm.gov/api/v1/files"

# HF path -> (dataset, YYYY, MM, version). Matches card_name_to_hf_path output.
_HF_PATH_RE = re.compile(
    r"^(accessions|separations|employment)/\1_(\d{4})(\d{2})_v(\d+)\.parquet$"
)


def hf_path_to_api_parts(hf_path: str) -> tuple[str, str, str, int]:
    """'accessions/accessions_202605_v1.parquet' -> ('accessions', '2026', '05', 1)."""
    m = _HF_PATH_RE.match(hf_path)
    if not m:
        raise ValueError(f"Cannot parse HF path for API download: {hf_path!r}")
    return m.group(1), m.group(2), m.group(3), int(m.group(4))


def list_files(dataset: str, *, current: bool = True, timeout: float = 60.0) -> list[dict]:
    """Return the API's metadata records for one dataset.

    Each record looks like:
      {"filename": "accessions_202605_1", "publishDate": "2026-06-30T16:41:47.880Z",
       "version": 1, "current": true, "month": "05", "year": "2026"}
    """
    params = {"current": "true"} if current else {}
    r = httpx.get(f"{API_BASE}/{dataset}", params=params, timeout=timeout)
    r.raise_for_status()
    data = r.json()
    return data if isinstance(data, list) else []


def _ym_int(year: str, month: str) -> int:
    return int(year) * 100 + int(month)


def get_site_manifest(data_types: list[str], start_date: str, end_date: str) -> dict:
    """Build the pipeline's site manifest from the OPM API.

    Returns a dict keyed by versioned HF path
    (e.g. 'accessions/accessions_202605_v1.parquet') with
    {filename, version, data_type, opm_date} values.

    Only *current* files are listed (one per month per dataset); non-current
    versions already on HF stay put, they simply aren't re-advertised.
    Restricted to the [start_date, end_date] month range so --test and
    date-narrowed runs work.
    """
    start_ym = int(start_date[:4] + start_date[5:7])
    end_ym = int(end_date[:4] + end_date[5:7])

    site_manifest: dict[str, dict] = {}
    for data_type in data_types:
        dataset = data_type.lower()
        for rec in list_files(dataset, current=True):
            year, month = rec["year"], rec["month"]
            if not (start_ym <= _ym_int(year, month) <= end_ym):
                continue
            version = int(rec["version"])
            card_name = f"{data_type} data from {_MONTH_NAME[month]} {year}"
            hf_path = card_name_to_hf_path(card_name, version)
            site_manifest[hf_path] = {
                "filename": card_name,
                "version": version,
                "data_type": dataset,
                "opm_date": rec.get("publishDate", ""),
            }
    return site_manifest


def download_file(hf_path: str, dest: Path, *, timeout: float = 600.0,
                  progress_every_mb: int = 25) -> Path:
    """Stream one file's parquet from the API download endpoint to `dest`.

    Raises httpx.HTTPStatusError on non-200 (e.g. 404 if the version isn't
    actually downloadable, 429 if throttled).
    """
    dataset, year, month, version = hf_path_to_api_parts(hf_path)
    url = f"{API_BASE}/{dataset}/{year}/{month}/{version}/download"
    dest.parent.mkdir(parents=True, exist_ok=True)

    with httpx.Client(timeout=timeout, follow_redirects=True) as client:
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


# month number -> English name, for reconstructing the card name the config
# helpers expect. (config._NUM_MONTH is keyed the same way but is private.)
_MONTH_NAME = {
    "01": "January", "02": "February", "03": "March", "04": "April",
    "05": "May", "06": "June", "07": "July", "08": "August",
    "09": "September", "10": "October", "11": "November", "12": "December",
}
