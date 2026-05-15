"""Constants, paths, and environment variables for the OPM pipeline."""

import os
from pathlib import Path
from dotenv import load_dotenv

load_dotenv()

HF_TOKEN = os.environ.get("HF_TOKEN")
HF_USERNAME = "impactproject"
HF_REPO = f"{HF_USERNAME}/opm-ehri-data"

OPM_URL = "https://data.opm.gov/explore-data/data/data-downloads"

DATA_TYPES = ["Accessions", "Separations", "Employment"]

START_DATE = "2000-01-01"
END_DATE = "2026-12-31"

DOWNLOAD_DIR = Path("data/downloads")
PARQUET_DIR = Path("data/parquet")
MANIFEST_PATH = Path("metadata/file_manifest.json")

SIZE_ESTIMATES = {
    "Accessions": 6,
    "Separations": 6,
    "Employment": 780,
}

# OPM's listing page advertises Version: 3 for these months, but the underlying
# chunked download endpoint returns 403 "Not found" — the v3 blob never landed
# in OPM's storage. Other 100+ v3 months work fine. Probed 2026-05-15; OPM has
# been emailed. Remove entries here once OPM publishes the missing blobs (or
# pulls the v3 advertisement from their listing).
PHANTOM_V3_EMPLOYMENT_KEYS = frozenset({
    "employment/employment_202503_v3.parquet",
    "employment/employment_202501_v3.parquet",
    "employment/employment_202412_v3.parquet",
    "employment/employment_202411_v3.parquet",
    "employment/employment_202410_v3.parquet",
    "employment/employment_202409_v3.parquet",
    "employment/employment_202408_v3.parquet",
    "employment/employment_202407_v3.parquet",
    "employment/employment_202406_v3.parquet",
    "employment/employment_202405_v3.parquet",
    "employment/employment_202306_v3.parquet",
})

# Month name -> number mapping for filename conversion
_MONTH_NUM = {
    "January": "01", "February": "02", "March": "03", "April": "04",
    "May": "05", "June": "06", "July": "07", "August": "08",
    "September": "09", "October": "10", "November": "11", "December": "12",
}


def card_name_to_hf_path(card_name: str, version: int = 1) -> str:
    """Convert OPM card name to HF repo path.

    'Accessions data from November 2025', version=3 -> 'accessions/accessions_202511_v3.parquet'
    """
    import re
    stem = card_name.replace('.csv', '').replace('.txt', '').replace('.parquet', '')
    m = re.match(r'(Accessions|Separations|Employment) data from (\w+) (\d{4})', stem)
    if not m:
        raise ValueError(f"Cannot parse card name: {card_name}")
    data_type = m.group(1).lower()
    month_num = _MONTH_NUM[m.group(2)]
    year = m.group(3)
    return f"{data_type}/{data_type}_{year}{month_num}_v{version}.parquet"


def hf_path_to_date(hf_path: str):
    """Parse YYYYMM from an HF path like 'accessions/accessions_202511_v3.parquet'.

    Returns a date object (first of that month) or None.
    """
    import re
    from datetime import date
    m = re.search(r'_(\d{4})(\d{2})(?:_v\d+)?\.parquet$', hf_path)
    if not m:
        return None
    return date(int(m.group(1)), int(m.group(2)), 1)


_NUM_MONTH = {v: k for k, v in _MONTH_NUM.items()}


def hf_path_to_card_stem(hf_path: str):
    """Reverse of card_name_to_hf_path (ignores version).

    'accessions/accessions_202511_v3.parquet' -> 'Accessions data from November 2025'
    """
    import re
    m = re.match(r'^(accessions|separations|employment)/\1_(\d{4})(\d{2})(?:_v\d+)?\.parquet$', hf_path)
    if not m:
        return None
    data_type = m.group(1).capitalize()
    year = m.group(2)
    month_name = _NUM_MONTH[m.group(3)]
    return f"{data_type} data from {month_name} {year}"
