# EHRI Federal Workforce Data

Public dataset of OPM/EHRI federal workforce data: accessions (new hires), separations (departures), and employment (point-in-time snapshots). Updated daily by an automated pipeline.

OPM also has premade visualizations on [data.opm.gov](https://data.opm.gov) that cover common questions about the federal workforce — workforce size, demographics, separations, etc. If one of those answers your question, use that.

## Using the Data

The data is **public and free to use** — no account or token needed.

All data lives in one HuggingFace dataset: **[impactproject/opm-ehri-data](https://huggingface.co/datasets/impactproject/opm-ehri-data)**

Files are versioned and organized in folders, e.g. `accessions/accessions_202511_v3.parquet`.

- **Accessions** (new hires): all available months since 2015
- **Separations** (departures): all available months since 2015
- **Employment** (workforce snapshots): all available months since 2022

### Interactive notebook

**[→ Open the demo notebook in Colab](https://colab.research.google.com/github/Impact-Map/opm_ehri_data/blob/main/demo.ipynb)** — loads available months automatically, no setup needed.

### Query directly with DuckDB

```python
import duckdb

url = "https://huggingface.co/datasets/impactproject/opm-ehri-data/resolve/main/accessions/accessions_202601_v1.parquet"
df = duckdb.execute(f"SELECT * FROM read_parquet('{url}')").df()
```

### Download with Python

```python
from huggingface_hub import hf_hub_download

path = hf_hub_download("impactproject/opm-ehri-data", "accessions/accessions_202601_v1.parquet", repo_type="dataset")
```

---

## About the Pipeline

Everything below is for people maintaining or developing the pipeline itself. You don't need any of this to use the data.

### How It Works

1. A daily GitHub Action runs `run_daily.py`
2. Playwright opens the OPM site (a Blazor app with no direct download URLs) and reads what files are available
3. Compares against `metadata/file_manifest.json` to detect new or updated files
4. Downloads changed files, converts CSV to parquet, uploads to HuggingFace
5. Creates a GitHub Issue with a summary: row count changes, schema changes, and per-column value diffs

### Developer Setup

**Requirements:**
- Python 3.9+
- A [HuggingFace token](https://huggingface.co/settings/tokens) with write access to the `impactproject` org
- For GitHub Actions: add `HF_TOKEN` as a repository secret

```bash
pip install -r requirements.txt
playwright install chromium

export HF_TOKEN=your_token_here
python run_daily.py                    # Daily check
python run_daily.py --rebuild-manifest # Rebuild manifest from HF
python run_daily.py --test             # Test mode (Jan 2026 only, no manifest save)
```

### Repo Structure

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
demo.ipynb                    # Colab-compatible demo notebook
```

### Technical Details

- OPM files are **pipe-delimited** (`|`), not comma-separated
- All columns read as strings (`dtype=str`) to avoid mixed-type issues
- Parquet uses zstd compression (~96% size reduction)
- Pipeline has resume logic: checks HuggingFace before downloading, skips existing files
- Employment files are ~780 MB CSV / ~30 MB parquet — fits in GitHub Actions memory

### Operations

See [HANDOFF.md](HANDOFF.md) for the operator guide: credentials, troubleshooting, email notifications.
