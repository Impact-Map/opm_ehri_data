# EHRI Federal Workforce Data Pipeline

This repo scrapes OPM/EHRI federal workforce data and publishes it to a single HuggingFace dataset: `impactproject/opm-ehri-data`.

## What This Does

Downloads three types of data from https://data.opm.gov/explore-data/data/data-downloads:
- **Accessions**: New federal hires (~6 MB/month CSV, ~0.2 MB parquet)
- **Separations**: Federal employee departures (~6 MB/month CSV, ~0.2 MB parquet)
- **Employment**: Full federal workforce snapshot (~780 MB/month CSV, ~30 MB parquet)

All files go into one HF repo, named by their OPM source and version (e.g. `accessions/accessions_202511_v3.parquet`).

## Key Files

- `run_daily.py` — Daily pipeline entry point (what the GitHub Action runs)
- `opm_pipeline/` — Core package: config, scraper, converter, uploader, manifest, differ, reporter
- `metadata/file_manifest.json` — Tracks what's on OPM site (version, row counts, columns); used for globally-new column detection
- `.github/workflows/daily_check.yml` — Daily cron at 10 AM ET
- `backfill_batch.py` — Backfill script (completed; available if needed again)
- `.github/workflows/backfill.yml` — Backfill workflow (currently disabled)
- `send_email.py` — Sends Buttondown email; reads `email_body.txt` written by the pipeline
- `demo.ipynb` — Public demo notebook (Colab-compatible) for exploring the data

## Technical Details

- OPM files are **pipe-delimited** (`|`), not comma-separated
- All columns read as strings (`dtype=str`) to avoid mixed-type issues
- Parquet uses zstd compression (~96% size reduction)
- Uses Playwright because OPM site is a Blazor app with no direct download URLs
- HF org hardcoded to `impactproject` in `opm_pipeline/config.py`

## Email Notifications (Buttondown)

On successful runs with new or updated files, the pipeline sends an email to Buttondown subscribers. Requires a `BUTTONDOWN_API_KEY` secret in GitHub repo settings.

- Subject: `New EHRI data available on OPM: X new months, Y updated files` (truncated to 150 chars)
- Body: HTML email with per-file diff summary (row counts, top proportional changes by agency/pay_plan), link to GitHub issue
- "New months" = new calendar month of data (v1 files); "updated" = revised version (v2+) of existing month
- New/removed columns are reported per file; if OPM retroactively adds a column to prior months, it's flagged as "retroactively added to prior months: Month YYYY–Month YYYY (N months)"
- Failures do NOT trigger emails (only GitHub issues)
- Email body written to `email_body.txt` by pipeline, read by `send_email.py`

See `HANDOFF.md` for operator guide (credentials, troubleshooting, Buttondown setup).

## Running

```bash
export HF_TOKEN=your_token_here
python run_daily.py                    # Daily check
python run_daily.py --rebuild-manifest # Seed manifest from HF (reads parquet schemas via footer, no full download)
python run_daily.py --test             # Test mode: January 2026 only, full report/email, no manifest save
python backfill_batch.py               # One batch of historical backfill
```
