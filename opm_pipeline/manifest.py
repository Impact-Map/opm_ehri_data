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
            if (site_entry.get("version") != stored_entry.get("version")
                    or site_entry.get("opm_date") != stored_entry.get("opm_date")):
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

    Reads only the file listing — no downloads needed. The manifest key is
    the HF path (e.g. 'accessions/accessions_202511_v3.parquet').
    """
    from huggingface_hub import repo_exists, list_repo_files
    import re

    from .config import HF_REPO

    HF_PATH_RE = re.compile(r'^(accessions|separations|employment)/\1_(\d{6})(?:_v(\d+))?\.parquet$')

    manifest = {}

    if not repo_exists(HF_REPO, repo_type="dataset", token=token):
        return manifest

    files = list_repo_files(HF_REPO, repo_type="dataset", token=token)

    for filename in files:
        m = HF_PATH_RE.match(filename)
        if not m:
            continue
        data_type = m.group(1)
        version = int(m.group(3)) if m.group(3) else 0

        manifest[filename] = {
            "filename": filename,
            "version": version,
            "data_type": data_type,
            "last_updated": datetime.now(timezone.utc).isoformat(),
        }

    return manifest
