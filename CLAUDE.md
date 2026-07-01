# EHRI Federal Workforce Data Pipeline

This repo mirrors OPM/EHRI federal workforce data to a single HuggingFace dataset (`impactproject/opm-ehri-data`) and notifies subscribers of new releases.

## What This Does

Pulls three datasets from OPM's official public API (`https://data.opm.gov/api/v1/files`, launched July 2026):
- **Accessions**: New federal hires (~60 KiB–1 MiB/month parquet)
- **Separations**: Federal employee departures (~100 KiB–4 MiB/month parquet)
- **Employment**: Full federal workforce snapshot (~26–75 MiB/month parquet)

The API serves parquet directly (zstd-compressed, all columns typed as string), so there's no scraping and no format conversion. All files go into one HF repo, named by their OPM source and version (e.g. `accessions/accessions_202511_v3.parquet`).

HuggingFace's role is the *queryable, throttle-free mirror* plus the diff/email value-add — the API is the durable source, but it's file-download-only (no content querying) and rate-limited.

## Key Files

- `run_daily.py` — Daily pipeline entry point (what the GitHub Action runs): list API → diff vs manifest → download new parquet → batch-commit to HF → diff/email
- `opm_pipeline/` — Core package: config, `api` (OPM API client), converter (parquet metadata), uploader, manifest, differ, reporter
- `metadata/file_manifest.json` — Tracks what's been mirrored (version, publishDate, row counts, columns); used for globally-new column detection
- `.github/workflows/daily_check.yml` — Daily cron at 14:00 UTC (~10 AM ET) + hourly on days 1–7 (OPM's monthly drop can land any hour)
- `send_email.py` — Sends Buttondown email; reads `email_body.txt` written by the pipeline
- `demo.ipynb` — Public demo notebook (Colab-compatible) for exploring the data

## Technical Details

- Files come from the API already as parquet (zstd, all columns string) — no CSV, no Playwright, no conversion
- API endpoints: `GET /api/v1/files/{dataset}?current=true` (JSON list) and `GET /api/v1/files/{dataset}/{year}/{month}/{version}/download` (parquet). `dataset` ∈ accessions|separations|employment
- The API also exposes full history back to 2005 and every prior version (`current=false`), so a re-backfill is just `get_site_manifest` without the current filter — no dedicated backfill script needed
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
```
