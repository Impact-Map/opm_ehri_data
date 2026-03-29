"""Load, save, and compare file manifests for change detection."""

import json
from datetime import datetime, timezone
from pathlib import Path

from .config import MANIFEST_PATH


def load_manifest(path: Path = MANIFEST_PATH) -> dict:
    """Load manifest from JSON file. Returns empty dict if not found."""
    if not path.exists():
        return {}
    with open(path) as f:
        return json.load(f)


def save_manifest(manifest: dict, path: Path = MANIFEST_PATH):
    """Save manifest to JSON file."""
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, 'w') as f:
        json.dump(manifest, f, indent=2)


def compare_manifests(stored: dict, site: dict) -> dict:
    """Compare stored manifest against what's on OPM site.

    Returns {"new": [...], "updated": [...], "unchanged": [...]}.
    Each entry is the repo_name key.
    """
    new = []
    updated = []
    unchanged = []

    for repo_name, site_entry in site.items():
        if repo_name not in stored:
            new.append(repo_name)
        else:
            stored_entry = stored[repo_name]
            # Check if version or opm_date changed
            if site_entry.get("version") != stored_entry.get("version"):
                updated.append(repo_name)
            elif (site_entry.get("opm_date", "") != stored_entry.get("opm_date", "")
                  and stored_entry.get("opm_date", "") != ""):
                # Only flag opm_date change if the stored entry actually had a date
                # (entries rebuilt from HF don't have opm_date)
                updated.append(repo_name)
            else:
                unchanged.append(repo_name)

    return {"new": new, "updated": updated, "unchanged": unchanged}


def update_manifest_entry(manifest: dict, repo_name: str, site_entry: dict, metadata: dict) -> dict:
    """Update a single manifest entry with site info and parquet metadata."""
    manifest[repo_name] = {
        "filename": site_entry.get("filename", ""),
        "version": site_entry.get("version", 0),
        "opm_date": site_entry.get("opm_date", ""),
        "data_type": site_entry.get("data_type", ""),
        "columns": metadata.get("columns", []),
        "row_count": metadata.get("row_count", 0),
        "file_hash": metadata.get("file_hash", ""),
        "last_updated": datetime.now(timezone.utc).isoformat(),
    }
    return manifest


def build_manifest_from_hf(token: str) -> dict:
    """Bootstrap manifest by querying existing files in the single HF repo.

    Reads parquet schema and metadata (footer only, ~KB per file) to populate
    column info, row counts, and file hashes. Sets opm_date to "" since HF
    doesn't have that info — compare_manifests handles this gracefully.

    The manifest key is the HF path (e.g. 'accessions/accessions_202511_v3.parquet').
    """
    from huggingface_hub import repo_exists, list_repo_files, HfFileSystem
    import pyarrow.parquet as pq
    import hashlib
    import re

    from .config import HF_REPO, hf_path_to_card_stem

    HF_PATH_RE = re.compile(r'^(accessions|separations|employment)/\1_(\d{6})(?:_v(\d+))?\.parquet$')

    manifest = {}

    if not repo_exists(HF_REPO, repo_type="dataset", token=token):
        return manifest

    files = list(list_repo_files(HF_REPO, repo_type="dataset", token=token))
    fs = HfFileSystem(token=token)

    for filename in files:
        m = HF_PATH_RE.match(filename)
        if not m:
            continue
        data_type = m.group(1)
        version = int(m.group(3)) if m.group(3) else 0

        columns = []
        row_count = 0
        try:
            pf = pq.ParquetFile(f"datasets/{HF_REPO}/{filename}", filesystem=fs)
            columns = pf.schema_arrow.names
            row_count = pf.metadata.num_rows
        except Exception as e:
            print(f"  Warning: could not read metadata for {filename}: {e}")

        # Generate a card-style filename for consistency with pipeline entries
        card_name = hf_path_to_card_stem(filename) or filename

        manifest[filename] = {
            "filename": card_name,
            "version": version,
            "opm_date": "",
            "data_type": data_type,
            "columns": columns,
            "row_count": row_count,
            "last_updated": datetime.now(timezone.utc).isoformat(),
        }

    return manifest
