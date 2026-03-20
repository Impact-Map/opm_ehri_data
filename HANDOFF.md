# OPM Data Pipeline — Operator Guide

Everything you need to know to keep this pipeline running.

## What This System Does

Every day at 10 AM ET, a GitHub Action automatically:

1. Checks the OPM website for new or updated federal workforce data files
2. Downloads any changes, converts them to an efficient format (parquet)
3. Uploads them to a public HuggingFace dataset
4. Creates a GitHub Issue summarizing what changed (or what went wrong)

**You do not need to run anything manually.** If everything is working, you'll see GitHub Issues with titles like:

> OPM Data Update - 2026-03-17 (2 new, 1 updated)

If something breaks, you'll see:

> Pipeline FAILED - 2026-03-17

The failure issue will contain the full error output and plain-English instructions for how to fix it.

## Where Things Live

| What | Where |
|------|-------|
| The data | https://huggingface.co/datasets/impactproject/opm-ehri-data |
| The code | This GitHub repo |
| Daily run results | GitHub Issues tab in this repo |
| Run history | Actions tab in this repo |
| HuggingFace token | GitHub repo Settings > Secrets > `HF_TOKEN` |

## Cleanup

The old GitHub repo redirect at https://github.com/Impact-Map/fedscope_new can be deleted — it's just a redirect from the previous owner's repo name and is no longer needed.

## How to Tell If Things Are Working

1. Go to the **Issues** tab in this repo
2. Look for recent issues. You should see one every day or two that says "OPM Data Update" (success) or nothing if there were no changes on OPM that day
3. If you see "Pipeline FAILED", read the issue — it will tell you what happened and what to do

You can also check the **Actions** tab to see the history of all runs (green = success, red = failure).

## Common Problems and Fixes

### "Pipeline FAILED" — HuggingFace token expired

**What the issue will say:** "HuggingFace authentication failed"

**How to fix:**
1. Go to https://huggingface.co/settings/tokens
2. Click **Create new token**
3. Name it something like `opm-pipeline`
4. Set access to **Write**
5. Copy the token
6. In this GitHub repo, go to Settings > Secrets and variables > Actions
7. Update the `HF_TOKEN` secret with the new token
8. Go to Actions tab, click "Daily OPM Data Check", click "Run workflow" to test

The token must belong to a HuggingFace account that has **write access** to the `impactproject` organization's datasets.

### "Pipeline FAILED" — OPM changed their website

**What the issue will say:** Playwright timeout errors, "zero files found", or selector errors

**How to fix:** This requires a developer. The file `opm_pipeline/scraper.py` contains the code that navigates OPM's website, and it relies on specific HTML element names. When OPM redesigns their page, these need to be updated.

**What to tell a developer:** "The OPM website at data.opm.gov/explore-data/data/data-downloads changed its layout. The Playwright selectors in `opm_pipeline/scraper.py` need to be updated to match the new HTML."

### "Pipeline FAILED" — network/temporary error

**What the issue will say:** Connection errors, timeouts

**How to fix:** Wait a day. The pipeline runs daily, so it will retry automatically. If it fails multiple days in a row, the OPM site may be down — check https://data.opm.gov manually.

### No issues appearing at all

The pipeline only creates issues when something changes or something fails. If OPM hasn't published new data, you won't see any issues. Check the **Actions** tab — you should see green runs every day even when there's nothing new.

If there are no runs at all, the GitHub Action may be disabled. Go to Actions > "Daily OPM Data Check" and click "Enable workflow" if you see that option.

## How to Run It Manually

If you need to trigger a run outside the daily schedule:

1. Go to the **Actions** tab in this repo
2. Click "Daily OPM Data Check" in the left sidebar
3. Click the "Run workflow" button
4. Click the green "Run workflow" button in the dropdown

## Credentials

This system needs two credentials, stored as GitHub repo secrets (Settings > Secrets and variables > Actions):

### HuggingFace Token (`HF_TOKEN`)

- **What it does:** Allows the pipeline to upload data to HuggingFace
- **How to generate a new one:**
  1. Log in to https://huggingface.co with an account that's a member of the `impactproject` organization
  2. Go to https://huggingface.co/settings/tokens
  3. Click **Create new token**, set access to **Write**
  4. Copy the token and add it as the `HF_TOKEN` secret in this repo's GitHub settings

If you need to move the dataset to a different HuggingFace account or org, update `HF_USERNAME` in `opm_pipeline/config.py`.

### Buttondown API Key (`BUTTONDOWN_API_KEY`)

- **What it does:** Sends email notifications to subscribers when new data is published
- **This is optional** — the pipeline works fine without it; you just won't get email notifications
- **How to set up from scratch:**
  1. Create a free account at https://buttondown.email
  2. Go to Settings > API to find your API key
  3. Add the API key as the `BUTTONDOWN_API_KEY` secret in this repo's GitHub settings
  4. People can subscribe to your Buttondown newsletter to get notified of new data

If the `BUTTONDOWN_API_KEY` secret is missing or empty, the email step will fail but the rest of the pipeline (data download, upload, GitHub issue) will still succeed.

## What the Data Looks Like

The HuggingFace dataset contains versioned parquet files organized in folders:

- `accessions/accessions_202511_v3.parquet` — new federal hires for November 2025 (version 3)
- `separations/separations_202511_v3.parquet` — federal employee departures for November 2025
- `employment/employment_202511_v2.parquet` — full workforce snapshot for November 2025

"Accessions" = new hires. "Separations" = people leaving. "Employment" = everyone currently employed.

Version numbers (v1, v2, v3) reflect OPM's own revisions — when OPM updates a previously published month, the pipeline uploads it with the new version number.

## If You Need to Change Something

| I want to... | Do this |
|---|---|
| Change when it runs | Edit `.github/workflows/daily_check.yml`, change the `cron` line |
| Change which data types it checks | Edit `opm_pipeline/config.py`, change `DATA_TYPES` |
| Change the date range | Edit `opm_pipeline/config.py`, change `START_DATE` / `END_DATE` |
| Change the HuggingFace account | Edit `opm_pipeline/config.py`, change `HF_USERNAME`; update `HF_TOKEN` secret |
| Re-download everything from scratch | Delete `metadata/file_manifest.json`, run with `--rebuild-manifest` |

## Contact

If something is broken and you can't fix it with the steps above, the issue is likely in `opm_pipeline/scraper.py` (OPM changed their site).
