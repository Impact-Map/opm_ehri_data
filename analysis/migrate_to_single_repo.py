"""
One-time migration: move all data from individual HF repos into the single
abigailhaddad/opm-federal-workforce repo, then delete the old repos.
"""

import re
import sys
import time
from pathlib import Path
from huggingface_hub import (
    HfApi, list_datasets, hf_hub_download, create_repo,
    list_repo_files, delete_repo,
)
from dotenv import load_dotenv
from tqdm import tqdm
import os

load_dotenv()

HF_TOKEN = os.environ.get("HF_TOKEN")
HF_USERNAME = "abigailhaddad"
NEW_REPO = f"{HF_USERNAME}/opm-federal-workforce"


def get_old_repos() -> list[str]:
    """Find all individual opm-federal-* dataset repos."""
    datasets = list_datasets(author=HF_USERNAME, search="opm-federal-")
    repos = []
    for ds in datasets:
        # Skip the new combined repo
        if ds.id == NEW_REPO:
            continue
        # Only include repos matching the old pattern
        if re.match(rf'^{HF_USERNAME}/opm-federal-(accessions|separations|employment)-\d{{6}}$', ds.id):
            repos.append(ds.id)
    return sorted(repos)


def repo_id_to_filename(repo_id: str) -> str:
    """Convert old repo ID to a filename for the new repo.

    abigailhaddad/opm-federal-accessions-202511 -> accessions_202511.parquet
    """
    name = repo_id.split("/", 1)[1]  # opm-federal-accessions-202511
    name = name.replace("opm-federal-", "")  # accessions-202511
    name = name.replace("-", "_")  # accessions_202511
    return f"{name}.parquet"


def main():
    if not HF_TOKEN:
        print("Error: HF_TOKEN required")
        sys.exit(1)

    api = HfApi()

    # Step 1: Find old repos
    print("Finding old repos...")
    old_repos = get_old_repos()
    print(f"Found {len(old_repos)} repos to migrate")

    if not old_repos:
        print("Nothing to migrate.")
        return

    # Step 2: Create new repo
    print(f"\nEnsuring {NEW_REPO} exists...")
    create_repo(NEW_REPO, repo_type="dataset", token=HF_TOKEN, exist_ok=True)

    # Check what's already in the new repo
    existing_files = set(list_repo_files(NEW_REPO, repo_type="dataset", token=HF_TOKEN))
    print(f"New repo already has {len(existing_files)} files")

    # Step 3: Migrate each old repo
    migrated = []
    skipped = []
    failed = []

    for repo_id in tqdm(old_repos, desc="Migrating"):
        new_filename = repo_id_to_filename(repo_id)

        if new_filename in existing_files:
            skipped.append(repo_id)
            continue

        try:
            # Download from old repo
            local_path = hf_hub_download(
                repo_id=repo_id, filename="data.parquet",
                repo_type="dataset", token=HF_TOKEN,
            )

            # Upload to new repo with proper name
            api.upload_file(
                path_or_fileobj=local_path,
                path_in_repo=new_filename,
                repo_id=NEW_REPO,
                repo_type="dataset",
                token=HF_TOKEN,
            )

            migrated.append(repo_id)
            time.sleep(2)
        except Exception as e:
            failed.append((repo_id, str(e)[:80]))
            time.sleep(5)  # back off more on errors

    # Summary
    print(f"\n{'='*60}")
    print(f"Migrated: {len(migrated)}")
    print(f"Skipped (already exists): {len(skipped)}")
    print(f"Failed: {len(failed)}")

    if failed:
        print("\nFailed repos:")
        for repo_id, err in failed:
            print(f"  {repo_id}: {err}")

    # Step 4: Delete old repos
    repos_to_delete = migrated + skipped  # both migrated and already-present
    if not repos_to_delete:
        return

    print(f"\nDeleting {len(repos_to_delete)} old repos...")
    deleted = 0
    for repo_id in tqdm(repos_to_delete, desc="Deleting old repos"):
        try:
            delete_repo(repo_id, repo_type="dataset", token=HF_TOKEN)
            deleted += 1
        except Exception as e:
            print(f"  Failed to delete {repo_id}: {e}")

    print(f"Deleted {deleted} old repos.")


if __name__ == "__main__":
    main()
