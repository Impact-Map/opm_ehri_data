"""HuggingFace upload and download operations."""

from __future__ import annotations

import time
from pathlib import Path
from huggingface_hub import HfApi, create_repo, list_repo_files, hf_hub_download

from .config import HF_REPO
from .hf_retry import hf_call


def get_repo_files(token: str) -> set[str]:
    """Get the set of files in the single HF repo. Returns empty set if repo doesn't exist."""
    try:
        return set(hf_call(list_repo_files, HF_REPO, repo_type="dataset", token=token))
    except Exception:
        return set()


def is_already_uploaded(filename: str, token: str, repo_files: set[str] | None = None) -> bool:
    """Check if a specific parquet file already exists in the HF repo."""
    if repo_files is not None:
        return filename in repo_files
    return filename in get_repo_files(token)


def upload_to_huggingface(parquet_path: Path, filename: str, token: str, max_retries: int = 3):
    """Upload a parquet file to the single HF repo with its OPM-derived name."""
    api = HfApi()

    try:
        create_repo(HF_REPO, repo_type="dataset", token=token, exist_ok=True)
    except Exception:
        pass

    for attempt in range(max_retries):
        try:
            api.upload_file(
                path_or_fileobj=str(parquet_path),
                path_in_repo=filename,
                repo_id=HF_REPO,
                repo_type="dataset",
                token=token,
            )
            return HF_REPO
        except Exception as e:
            if attempt < max_retries - 1:
                wait_time = 2 ** (attempt + 1)
                time.sleep(wait_time)
            else:
                raise e
    return HF_REPO


def download_existing_parquet(filename: str, token: str) -> Path | None:
    """Download an existing parquet file from HF for diffing. Returns path or None."""
    try:
        files = hf_call(list_repo_files, HF_REPO, repo_type="dataset", token=token)
        if filename not in files:
            return None
        path = hf_call(
            hf_hub_download,
            repo_id=HF_REPO, filename=filename, repo_type="dataset", token=token,
        )
        return Path(path)
    except Exception:
        return None
