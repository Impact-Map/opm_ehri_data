"""One-off: regenerate email_body.txt for Feb 2026 data from HuggingFace.

Downloads the new and prior parquets, diffs them, and writes email_body.txt.
Run send_email.py afterward to actually send it.
"""
from __future__ import annotations

import os
from datetime import date

from opm_pipeline.uploader import download_existing_parquet
from opm_pipeline.differ import generate_diff_summary
from opm_pipeline.reporter import generate_email_html

TOKEN = os.environ.get("HF_TOKEN")

FILES = [
    ("accessions/accessions_202602_v1.parquet", "accessions/accessions_202601_v1.parquet"),
    ("separations/separations_202602_v1.parquet", "separations/separations_202601_v1.parquet"),
    ("employment/employment_202602_v1.parquet", "employment/employment_202601_v1.parquet"),
]

changes = {"new": [f[0] for f in FILES], "updated": []}
diffs = {}

for new_key, old_key in FILES:
    print(f"Downloading {new_key} and {old_key}...")
    new_path = download_existing_parquet(new_key, TOKEN)
    old_path = download_existing_parquet(old_key, TOKEN)
    if new_path and old_path:
        diff = generate_diff_summary(old_path, new_path)
        diff["compared_to"] = old_key
        diffs[new_key] = diff
        print(f"  Diffed: {diff.get('row_counts', {})}")
    else:
        print(f"  SKIP: missing file (new={new_path}, old={old_path})")

email_html = generate_email_html(changes, diffs, {}, date(2026, 4, 1))

# Add link to existing issue
email_html += "\n<p><a href='https://github.com/Impact-Map/opm_ehri_data/issues/35'>Full diff report</a></p>"

with open("email_body.txt", "w") as f:
    f.write(email_html)

print(f"\nWrote email_body.txt ({len(email_html)} chars)")
