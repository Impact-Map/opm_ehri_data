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

# HF paths for URLs that OPM advertises on its listing page but whose
# underlying chunked download blob returns 403 "Not found" — phantoms.
# Entries here get filtered out of the download queue at iteration time,
# and the daily run probes each one in parallel to detect resolution
# (OPM has occasionally published the missing blob later, in which case
# the entry should be removed from this set).
#
# The original 11-month set (May 2024–Mar 2025, plus Jun 2023) all
# resolved on 2026-05-18 after Abigail emailed OPM — the pipeline auto-
# detected and ingested them. The set is empty now; add new entries
# manually if more phantoms appear (the run_daily.py 403 handler keeps
# the pipeline running even without an entry here, but failed files
# show up in the daily issue).
PHANTOM_V3_EMPLOYMENT_KEYS = frozenset()

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
