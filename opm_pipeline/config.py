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
