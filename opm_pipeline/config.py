"""Constants, paths, and environment variables for the OPM pipeline."""

import os
from pathlib import Path
from dotenv import load_dotenv

load_dotenv()

HF_TOKEN = os.environ.get("HF_TOKEN")
HF_USERNAME = "abigailhaddad"
HF_REPO = f"{HF_USERNAME}/opm-federal-workforce"

OPM_URL = "https://data.opm.gov/explore-data/data/data-downloads"

DATA_TYPES = ["Accessions", "Separations", "Employment"]

START_DATE = "2025-07-01"
END_DATE = "2026-12-31"

DOWNLOAD_DIR = Path("data/downloads")
PARQUET_DIR = Path("data/parquet")
MANIFEST_PATH = Path("metadata/file_manifest.json")

SIZE_ESTIMATES = {
    "Accessions": 6,
    "Separations": 6,
    "Employment": 780,
}

# Month name -> number mapping for filename conversion
_MONTH_NUM = {
    "January": "01", "February": "02", "March": "03", "April": "04",
    "May": "05", "June": "06", "July": "07", "August": "08",
    "September": "09", "October": "10", "November": "11", "December": "12",
}


def card_name_to_hf_path(card_name: str) -> str:
    """Convert OPM card name to HF repo path.

    'Accessions data from November 2025' -> 'accessions/accessions_202511.parquet'
    'Accessions data from November 2025.csv' -> 'accessions/accessions_202511.parquet'
    """
    import re
    stem = card_name.replace('.csv', '').replace('.txt', '').replace('.parquet', '')
    m = re.match(r'(Accessions|Separations|Employment) data from (\w+) (\d{4})', stem)
    if not m:
        raise ValueError(f"Cannot parse card name: {card_name}")
    data_type = m.group(1).lower()
    month_num = _MONTH_NUM[m.group(2)]
    year = m.group(3)
    return f"{data_type}/{data_type}_{year}{month_num}.parquet"


def hf_path_to_date(hf_path: str):
    """Parse YYYYMM from an HF path like 'accessions/accessions_202511.parquet'.

    Returns a date object (first of that month) or None.
    """
    import re
    from datetime import date
    m = re.search(r'_(\d{4})(\d{2})\.parquet$', hf_path)
    if not m:
        return None
    return date(int(m.group(1)), int(m.group(2)), 1)
