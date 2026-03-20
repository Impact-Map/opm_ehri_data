# OPM Federal Workforce Data

> **Note:** This was put together quickly and may have errors. Please verify anything important against the original source at [data.opm.gov](https://data.opm.gov).

OPM already has premade visualizations on [data.opm.gov](https://data.opm.gov) that cover common questions about the federal workforce — workforce size, demographics, separations, etc. If one of those answers your question, use that.

This repo scrapes OPM's raw data (accessions, separations, employment), converts it to parquet, and publishes it to a single HuggingFace dataset. A daily GitHub Action checks for new or updated files and creates a GitHub Issue summarizing what changed.

## Data on HuggingFace

All data lives in one dataset: **[impactproject/opm-ehri-data](https://huggingface.co/datasets/impactproject/opm-ehri-data)**

Files are named after their OPM source, e.g. `accessions_202511.parquet`, `employment_202511.parquet`.

- **Accessions** (new hires): all available months
- **Separations** (departures): all available months
- **Employment** (workforce snapshots): all available months (notebook loads most recent 6 by default — they're large)

**[→ Open the demo notebook in Colab](https://colab.research.google.com/github/Impact-Map/opm_ehri_data/blob/main/demo.ipynb)** — loads all available months automatically, no setup needed.

Or query directly with DuckDB — no download needed:

```python
import duckdb

url = "https://huggingface.co/datasets/impactproject/opm-ehri-data/resolve/main/accessions/accessions_202601.parquet"
df = duckdb.execute(f"SELECT * FROM read_parquet('{url}')").df()
```

## How It Works

1. A daily GitHub Action runs `run_daily.py`
2. Playwright opens the OPM site (a Blazor app with no direct download URLs) and reads what files are available
3. Compares against `metadata/file_manifest.json` to detect new or updated files
4. Downloads changed files, converts CSV to parquet, uploads to HuggingFace
5. Creates a GitHub Issue with a summary: row count changes, schema changes, and per-column value diffs

## Setup

**Requirements:**
- Python 3.9+
- A [HuggingFace token](https://huggingface.co/settings/tokens) with write access, set as `HF_TOKEN` environment variable
- For GitHub Actions: add `HF_TOKEN` as a repository secret

```bash
pip install -r requirements.txt
playwright install chromium
```

**Run the daily check manually:**
```bash
export HF_TOKEN=your_token_here
python run_daily.py
```

**Rebuild the manifest from existing HuggingFace data:**
```bash
python run_daily.py --rebuild-manifest
```

**Bulk backfill (download everything):**
```bash
python analysis/download_and_upload.py --start 2021-01-01 --end 2025-11-30
```

## Repo Structure

```
run_daily.py                  # Daily pipeline entry point
opm_pipeline/                 # Core package
  config.py                   #   Constants, paths, env vars
  scraper.py                  #   Playwright automation for OPM site
  converter.py                #   CSV-to-parquet conversion
  uploader.py                 #   HuggingFace upload/download
  manifest.py                 #   Change detection via file manifest
  differ.py                   #   Compare old vs new parquet files
  reporter.py                 #   Generate markdown reports, create GitHub Issues
metadata/
  file_manifest.json          # Tracks what's on OPM (committed to repo)
.github/workflows/
  daily_check.yml             # Runs daily at 10 AM ET
analysis/                     # Notebooks, one-off scripts, web viewer
```

## Technical Details

- OPM files are **pipe-delimited** (`|`), not comma-separated
- All columns read as strings (`dtype=str`) to avoid mixed-type issues
- Parquet uses zstd compression (~96% size reduction)
- Pipeline has resume logic: checks HuggingFace before downloading, skips existing files
- Employment files are ~780 MB CSV / ~30 MB parquet — fits in GitHub Actions memory

## Other Resources

- [OPM Visualization Catalog](https://newfedscope.netlify.app/) — Searchable index of OPM's built-in dashboards
- [Colab Notebook](https://colab.research.google.com/github/Impact-Map/opm_ehri_data/blob/main/demo.ipynb) — Load and explore data without downloading
