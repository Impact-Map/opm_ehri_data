# OPM Federal Workforce Data Pipeline

This repo scrapes OPM federal workforce data and publishes it to a single HuggingFace dataset: `abigailhaddad/opm-federal-workforce`.

## What This Does

Downloads three types of data from https://data.opm.gov/explore-data/data/data-downloads:
- **Accessions**: New federal hires (~6 MB/month CSV, ~0.2 MB parquet)
- **Separations**: Federal employee departures (~6 MB/month CSV, ~0.2 MB parquet)
- **Employment**: Full federal workforce snapshot (~780 MB/month CSV, ~30 MB parquet)

All files go into one HF repo, named by their OPM source (e.g. `accessions_202511.parquet`).

## Key Files

- `run_daily.py` — Daily pipeline entry point (what the GitHub Action runs)
- `opm_pipeline/` — Core package: config, scraper, converter, uploader, manifest, differ, reporter
- `metadata/file_manifest.json` — Tracks what's on OPM site (version, row counts, columns)
- `.github/workflows/daily_check.yml` — Daily cron at 10 AM ET
- `analysis/` — Notebooks, bulk download script, web viewer (not part of the pipeline)

## Technical Details

- OPM files are **pipe-delimited** (`|`), not comma-separated
- All columns read as strings (`dtype=str`) to avoid mixed-type issues
- Parquet uses zstd compression (~96% size reduction)
- Uses Playwright because OPM site is a Blazor app with no direct download URLs
- HF username hardcoded to `abigailhaddad` in `opm_pipeline/config.py`

## Running

```bash
export HF_TOKEN=your_token_here
python run_daily.py                    # Daily check
python run_daily.py --rebuild-manifest # Seed manifest from HF
```
