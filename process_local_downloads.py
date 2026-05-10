"""Watch a download directory for OPM files, convert + upload + delete them.

Pairs with the Tampermonkey userscript (tools/opm_autodownloader.user.js): the
userscript downloads files into ~/Downloads one at a time; this script picks
them up as they arrive, converts each to parquet, commits it to HuggingFace,
updates the local manifest and prunes metadata/pending.json, then deletes the
original .txt so disk usage stays bounded.

Run in a separate terminal:
    ./venv/bin/python process_local_downloads.py

Stop with Ctrl-C. Anything still in the upload buffer is flushed first.
"""
from __future__ import annotations

import argparse
import json
import re
import signal
import sys
import time
from pathlib import Path

from huggingface_hub import HfApi, CommitOperationAdd

from opm_pipeline.config import HF_REPO, HF_TOKEN, PARQUET_DIR
from opm_pipeline.converter import convert_to_parquet, get_parquet_metadata
from opm_pipeline.manifest import load_manifest, save_manifest, update_manifest_entry


PENDING_PATH = Path("metadata/pending.json")

# Matches the customFileName Chrome saves OPM downloads as, e.g.
# "employment_202503_3_2026-05-10.txt" or "employment_202503_3_2026-05-10 (1).txt".
OPM_FILENAME_RE = re.compile(
    r"^(?P<type>accessions|separations|employment)_"
    r"(?P<yyyymm>\d{6})_"
    r"(?P<version>\d+)_"
    r"(?P<date>\d{4}-\d{2}-\d{2})"
    r"(?: \(\d+\))?"
    r"\.txt$"
)


def hf_path_from_download(filename: str) -> str | None:
    """Map a downloaded OPM .txt filename to its HF parquet path."""
    m = OPM_FILENAME_RE.match(filename)
    if not m:
        return None
    t, ym, v = m.group("type"), m.group("yyyymm"), m.group("version")
    return f"{t}/{t}_{ym}_v{v}.parquet"


def is_download_complete(path: Path, settle_seconds: float = 5.0) -> bool:
    """Returns True once the file size hasn't changed for `settle_seconds`.

    Chrome writes a sibling `<name>.crdownload` while downloading; once it
    renames to the final `.txt`, the file should stop growing. We still wait
    a few seconds of size stability to be safe.
    """
    if not path.exists():
        return False
    # If a .crdownload sibling exists, Chrome is still downloading.
    if path.with_suffix(path.suffix + ".crdownload").exists():
        return False
    try:
        size0 = path.stat().st_size
        time.sleep(settle_seconds)
        return path.stat().st_size == size0 and size0 > 0
    except FileNotFoundError:
        return False


def load_pending() -> list[dict]:
    if not PENDING_PATH.exists():
        return []
    try:
        with open(PENDING_PATH) as f:
            data = json.load(f)
            return data if isinstance(data, list) else []
    except Exception:
        return []


def save_pending(entries: list[dict]) -> None:
    PENDING_PATH.parent.mkdir(parents=True, exist_ok=True)
    with open(PENDING_PATH, "w") as f:
        json.dump(entries, f, indent=2)


def prune_pending(uploaded_keys: list[str]) -> int:
    pending = load_pending()
    if not pending:
        return 0
    upload_set = set(uploaded_keys)
    remaining = [p for p in pending if p["key"] not in upload_set]
    if len(remaining) != len(pending):
        save_pending(remaining)
    return len(remaining)


def flush_batch(buffer: list[dict], manifest: dict, token: str) -> tuple[int, list[str]]:
    """Commit a batch of (parquet_path, hf_path) to HF in one commit. Returns
    (uploaded_count, uploaded_keys). On failure, returns (0, [])."""
    if not buffer:
        return 0, []
    print(f"[watcher] Flushing batch of {len(buffer)} to HuggingFace...")
    ops = [
        CommitOperationAdd(path_in_repo=e["hf_path"], path_or_fileobj=str(e["parquet_path"]))
        for e in buffer
    ]
    try:
        HfApi().create_commit(
            repo_id=HF_REPO,
            repo_type="dataset",
            token=token,
            operations=ops,
            commit_message=f"Add/update {len(ops)} files (local catch-up)",
        )
    except Exception as exc:
        print(f"[watcher] ERROR committing batch: {exc}")
        return 0, []
    uploaded_keys = []
    for e in buffer:
        update_manifest_entry(
            manifest,
            e["hf_path"],
            {
                "filename": e["hf_path"],
                "version": e["version"],
                "opm_date": e["opm_date"],
                "data_type": e["data_type"],
            },
            e["metadata"],
        )
        uploaded_keys.append(e["hf_path"])
    save_manifest(manifest)
    remaining = prune_pending(uploaded_keys)
    print(f"[watcher] Committed {len(uploaded_keys)}. Manifest + pending.json updated ({remaining} files still pending).")
    # Delete local parquets
    for e in buffer:
        try:
            Path(e["parquet_path"]).unlink()
        except Exception:
            pass
    return len(uploaded_keys), uploaded_keys


def process_one(txt_path: Path, manifest: dict) -> dict | None:
    """Convert one .txt to parquet, build an upload entry, delete the .txt."""
    hf_path = hf_path_from_download(txt_path.name)
    if not hf_path:
        print(f"[watcher] Ignoring (unrecognized name): {txt_path.name}")
        return None

    m = OPM_FILENAME_RE.match(txt_path.name)
    data_type = m.group("type")
    version = int(m.group("version"))
    opm_date = m.group("date")

    print(f"[watcher] Converting {txt_path.name} -> parquet")
    PARQUET_DIR.mkdir(parents=True, exist_ok=True)
    parquet_path = convert_to_parquet(txt_path, PARQUET_DIR)
    metadata = get_parquet_metadata(parquet_path)

    # Delete the .txt now — disk pressure matters
    try:
        txt_path.unlink()
    except Exception as exc:
        print(f"[watcher] WARN: could not delete {txt_path.name}: {exc}")

    return {
        "txt_name": txt_path.name,
        "parquet_path": parquet_path,
        "hf_path": hf_path,
        "data_type": data_type,
        "version": version,
        "opm_date": opm_date,
        "metadata": metadata,
    }


def main():
    parser = argparse.ArgumentParser(description="Watch a directory for OPM .txt downloads and process them.")
    parser.add_argument("--watch-dir", default=str(Path.home() / "Downloads"),
                        help="Directory to watch (default: ~/Downloads)")
    parser.add_argument("--poll-interval", type=float, default=10.0,
                        help="Seconds between scans of the watch directory")
    parser.add_argument("--batch-size", type=int, default=5,
                        help="How many parquets to accumulate before each HF commit")
    parser.add_argument("--dry-run", action="store_true",
                        help="Convert + delete .txt but skip HF upload and manifest write")
    args = parser.parse_args()

    if not HF_TOKEN and not args.dry_run:
        print("ERROR: HF_TOKEN not set. Source your .env or pass --dry-run.")
        sys.exit(1)

    watch_dir = Path(args.watch_dir).expanduser()
    if not watch_dir.is_dir():
        print(f"ERROR: watch dir does not exist: {watch_dir}")
        sys.exit(1)

    print(f"[watcher] Watching {watch_dir} every {args.poll_interval}s. Batch size {args.batch_size}. Ctrl-C to stop.")
    manifest = load_manifest()
    print(f"[watcher] Loaded manifest with {len(manifest)} entries; {len(load_pending())} files pending.")

    buffer: list[dict] = []
    processed_set: set[str] = set()
    stopping = False

    def handle_sigint(signum, frame):
        nonlocal stopping
        print("\n[watcher] Ctrl-C received — flushing any buffered work then exiting.")
        stopping = True

    signal.signal(signal.SIGINT, handle_sigint)

    while not stopping:
        try:
            for entry in sorted(watch_dir.iterdir()):
                if not entry.is_file() or not entry.name.endswith(".txt"):
                    continue
                if entry.name in processed_set:
                    continue
                if not OPM_FILENAME_RE.match(entry.name):
                    continue
                if not is_download_complete(entry):
                    continue

                result = process_one(entry, manifest)
                if not result:
                    processed_set.add(entry.name)
                    continue
                processed_set.add(result["txt_name"])
                buffer.append(result)

                if not args.dry_run and len(buffer) >= args.batch_size:
                    flush_batch(buffer, manifest, HF_TOKEN)
                    buffer = []
        except Exception as exc:
            print(f"[watcher] Scan error: {exc}")

        if stopping:
            break

        # Sleep in small slices so Ctrl-C is responsive
        slept = 0.0
        while slept < args.poll_interval and not stopping:
            time.sleep(0.5)
            slept += 0.5

    # Final flush on exit
    if buffer and not args.dry_run:
        print(f"[watcher] Final flush of {len(buffer)} buffered files...")
        flush_batch(buffer, manifest, HF_TOKEN)


if __name__ == "__main__":
    main()
