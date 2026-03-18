"""One-off script: diff Jan 2026 vs Dec 2025 for all data types and send via Buttondown."""

import os
import tempfile
from pathlib import Path
from datetime import date

from huggingface_hub import hf_hub_download
from opm_pipeline.config import HF_REPO, HF_TOKEN
from opm_pipeline.differ import generate_diff_summary

import requests
from dotenv import load_dotenv
load_dotenv()

BUTTONDOWN_API_KEY = os.environ.get("BUTTONDOWN_API_KEY")
TOKEN = HF_TOKEN

PAIRS = [
    ("accessions",  "accessions/accessions_202512.parquet",  "accessions/accessions_202601.parquet"),
    ("separations", "separations/separations_202512.parquet", "separations/separations_202601.parquet"),
    ("employment",  "employment/employment_202512.parquet",   "employment/employment_202601.parquet"),
]


def download(hf_path: str, dest: Path) -> Path:
    local = hf_hub_download(
        repo_id=HF_REPO, filename=hf_path, repo_type="dataset",
        token=TOKEN, local_dir=str(dest),
    )
    return Path(local)


def fmt(n: int) -> str:
    return f"{n:,}"

def sign(n: int) -> str:
    return f"+{n:,}" if n >= 0 else f"{n:,}"

def pct(p: float) -> str:
    return f"+{p}%" if p >= 0 else f"{p}%"


def top_proportional_changes(vc: dict, n: int = 4, min_old: int = 100) -> list:
    """Return top N value changes by proportional shift, deduplicating code/label column pairs."""
    # Skip _code columns if the label column also exists
    label_cols = {c for c in vc if not c.endswith("_code")}
    skip = {c for c in vc if c.endswith("_code") and c[:-5] in label_cols}

    # Skip date-ish columns
    date_keywords = ("date", "yyyymm", "yyyyq", "year", "month")

    candidates = []
    for col, info in vc.items():
        if col in skip:
            continue
        if any(kw in col.lower() for kw in date_keywords):
            continue
        for v in info["value_changes"]:
            if v["old_count"] < min_old:
                continue
            prop = v["diff"] / v["old_count"] * 100
            candidates.append((abs(prop), prop, col, v))

    candidates.sort(reverse=True)
    return candidates[:n]


def generate_email_html(diffs: dict) -> str:
    parts = ["<h2>EHRI Data Update: January 2026 vs December 2025</h2>"]

    for key, diff in diffs.items():
        dtype = key.replace("_202601", "").capitalize()
        parts.append(f"<h3>{dtype}</h3>")

        rc = diff.get("row_counts", {})
        if rc:
            parts.append(
                f"<p><strong>Total records:</strong> {fmt(rc['old'])} → {fmt(rc['new'])} "
                f"({sign(rc['diff'])}, {pct(rc['pct_change'])})</p>"
            )

        top = top_proportional_changes(diff.get("value_counts", {}))
        if top:
            parts.append("<ul>")
            for _, prop, col, v in top:
                prop_str = f"+{prop:.0f}%" if prop >= 0 else f"{prop:.0f}%"
                parts.append(
                    f"<li><strong>{col} — {v['value']}</strong>: "
                    f"{prop_str} ({fmt(v['old_count'])} → {fmt(v['new_count'])})</li>"
                )
            parts.append("</ul>")

    return "\n".join(parts)


def main():
    diffs = {}

    with tempfile.TemporaryDirectory() as tmpdir:
        tmp = Path(tmpdir)
        for dtype, dec_path, jan_path in PAIRS:
            key = f"{dtype}_202601"
            print(f"Downloading {dec_path} ...")
            old = download(dec_path, tmp)
            print(f"Downloading {jan_path} ...")
            new = download(jan_path, tmp)
            print(f"Diffing {dtype} ...")
            diff = generate_diff_summary(old, new)
            diffs[key] = diff
            print(f"  Rows: {diff['row_counts']}")

    body = generate_email_html(diffs)
    subject = "New EHRI Data: January 2026 (3 new files)"

    print("\n--- EMAIL BODY ---")
    print(body[:800])

    if not BUTTONDOWN_API_KEY:
        print("\nNo BUTTONDOWN_API_KEY — printing only.")
        return

    resp = requests.post(
        "https://api.buttondown.email/v1/emails",
        headers={
            "Authorization": f"Token {BUTTONDOWN_API_KEY}",
            "Content-Type": "application/json",
            "X-Buttondown-Live-Dangerously": "true",
        },
        json={"subject": subject, "body": body, "status": "about_to_send"},
    )
    print(f"\nButtondown response: {resp.status_code} {resp.text[:300]}")


if __name__ == "__main__":
    main()
