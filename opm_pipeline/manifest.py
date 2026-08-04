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


def sync_manifest_with_hf(stored: dict, token: str) -> tuple[dict, list]:
    """Patch the stored manifest to match what's actually in the HF repo.

    Run at the start of each daily pipeline so that uploads from a previously
    killed run (e.g. one that hit the 6h GitHub Actions timeout mid-loop and
    never got to save_manifest) aren't redone on the next run.

    Lightweight: lists HF files, parses versions out of the filenames, and
    adds any HF entries the manifest is missing. Does NOT read parquet
    footers (build_manifest_from_hf does that — it's much slower for 700+
    files). Does NOT remove manifest entries that aren't in HF.

    Returns (manifest, added_keys) — the list of HF paths that were missing
    from the manifest and got added. Caller can surface these in the run
    report so nothing falls off the radar (they won't appear in the OPM diff
    because they're now in the manifest at the right version).
    """
    from huggingface_hub import list_repo_files
    from huggingface_hub.errors import RepositoryNotFoundError
    import re

    from .config import HF_REPO, hf_path_to_card_stem
    from .hf_retry import hf_call

    HF_PATH_RE = re.compile(
        r'^(accessions|separations|employment)/\1_(\d{6})(?:_v(\d+))?\.parquet$'
    )

    # One HF call, not two: list_repo_files raises if the repo is missing, so a
    # separate repo_exists() check just doubles our exposure to 429s.
    try:
        files = hf_call(list_repo_files, HF_REPO, repo_type="dataset", token=token)
    except RepositoryNotFoundError:
        return stored, []
    added_keys = []
    for filename in files:
        m = HF_PATH_RE.match(filename)
        if not m:
            continue
        if filename in stored:
            continue
        # Manifest doesn't know about this HF file — likely uploaded by a prior
        # run that didn't get to save_manifest. Add a minimal entry so the OPM
        # diff treats it as already-known. Columns/row_count stay empty; if/when
        # this file is re-processed they'll be populated then.
        data_type = m.group(1)
        version = int(m.group(3)) if m.group(3) else 0
        stored[filename] = {
            "filename": hf_path_to_card_stem(filename) or filename,
            "version": version,
            "opm_date": "",
            "data_type": data_type,
            "columns": [],
            "row_count": 0,
            "last_updated": datetime.now(timezone.utc).isoformat(),
        }
        added_keys.append(filename)

    return stored, added_keys


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
    from huggingface_hub import list_repo_files, HfFileSystem
    from huggingface_hub.errors import RepositoryNotFoundError
    import pyarrow.parquet as pq
    import hashlib
    import re

    from .config import HF_REPO, hf_path_to_card_stem
    from .hf_retry import hf_call

    HF_PATH_RE = re.compile(r'^(accessions|separations|employment)/\1_(\d{6})(?:_v(\d+))?\.parquet$')

    manifest = {}

    try:
        files = list(hf_call(list_repo_files, HF_REPO, repo_type="dataset", token=token))
    except RepositoryNotFoundError:
        return manifest
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
