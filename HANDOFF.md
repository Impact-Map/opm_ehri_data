# OPM Data Pipeline — Operator Guide

This guide is for whoever is maintaining this pipeline going forward. It's meant to be readable by someone who isn't a developer.

## What This System Does

Every day at 10 AM ET, a GitHub Action automatically:

1. Checks the OPM website (data.opm.gov) for new or updated federal workforce data
2. Downloads any new files, converts them to parquet (a compact, efficient format), and uploads them to HuggingFace
3. Creates a GitHub Issue summarizing what changed — row counts, schema changes, and the biggest shifts by agency and pay plan
4. Optionally sends an email to subscribers with a shorter version of the same summary

**You do not need to run anything manually.** When it's working, you'll see GitHub Issues like:

> OPM Data Update - 2026-03-17 (2 new, 1 updated)

When something breaks:

> Pipeline FAILED - 2026-03-17

The failure issue includes the full error output and plain-English instructions for how to fix it.

## Where Things Live

| What | Where |
|------|-------|
| The data | https://huggingface.co/datasets/impactproject/opm-ehri-data |
| The code | This GitHub repo |
| Daily run results | GitHub Issues tab |
| Run history | Actions tab |
| Secrets (tokens) | GitHub repo Settings > Secrets and variables > Actions |

## Cleanup

The old GitHub repo redirect at https://github.com/Impact-Map/fedscope_new can be deleted.

## What the Data Looks Like

The HuggingFace dataset contains versioned parquet files organized in folders:

- `accessions/accessions_202511_v3.parquet` — new federal hires for November 2025 (version 3)
- `separations/separations_202511_v3.parquet` — departures for November 2025
- `employment/employment_202511_v2.parquet` — full workforce snapshot for November 2025

**Accessions** = new hires. **Separations** = people leaving. **Employment** = everyone currently employed at a point in time.

Version numbers (v1, v2, v3) reflect OPM's own revisions. When OPM updates a previously published month, the pipeline uploads the new version alongside the old one.

## Notifications: GitHub Issues vs. Email

The pipeline has two notification channels. They serve different audiences:

### GitHub Issues (always on)

Every time the pipeline detects new or changed data, it creates a GitHub Issue with the full technical details:
- Row count changes for each file
- New or removed columns (schema changes)
- The top shifts by agency, pay plan, and other dimensions (e.g., "VA accessions up 15%")
- Columns retroactively added to prior months

If the pipeline fails, it creates an issue with the full error output and a diagnosis. **GitHub Issues are the primary way to monitor the pipeline.** Anyone watching this repo will get notified.

### Email via Buttondown (optional)

The pipeline can also send a shorter HTML email to subscribers via [Buttondown](https://buttondown.email), a simple email newsletter service. The email contains:
- A heading like "EHRI Data Update: March 2026"
- Per-file row count changes
- The top proportional changes (same highlights as the GitHub Issue, but fewer details)

**Current state:** The `BUTTONDOWN_API_KEY` secret in this repo is from the previous owner's personal Buttondown account. Emails currently send from that account. You have a few options:

1. **Create your own Buttondown account** — sign up at buttondown.email, get your API key from Settings > API, and replace the `BUTTONDOWN_API_KEY` secret. Subscribers will need to re-subscribe to the new account. This is free for small subscriber lists.
2. **Use a different email service** — you'd need to modify `send_email.py` (it's ~35 lines). The pipeline writes the email body to `email_body.txt` and the subject line is passed via environment variable.
3. **Turn off emails entirely** — just delete the `BUTTONDOWN_API_KEY` secret. The pipeline will still work; the email step will fail silently and everything else (data upload, GitHub Issue) will succeed normally.

## How to Tell If Things Are Working

1. Go to the **Issues** tab in this repo
2. Look for recent issues — you should see "OPM Data Update" issues whenever OPM publishes new data (typically monthly)
3. If you see "Pipeline FAILED", read the issue — it tells you what happened and what to do

You can also check the **Actions** tab to see every run (green = success, red = failure). Runs happen daily even when there's nothing new on OPM.

## Common Problems and Fixes

### "Pipeline FAILED" — HuggingFace token expired

**What the issue will say:** "HuggingFace authentication failed"

**How to fix:**
1. Log in to https://huggingface.co with an account that's a member of the `impactproject` organization
2. Go to https://huggingface.co/settings/tokens
3. Click **Create new token**
4. Name it something like `opm-pipeline` and set access to **Write**
5. Copy the token
6. In this GitHub repo, go to Settings > Secrets and variables > Actions
7. Update the `HF_TOKEN` secret with the new token
8. Go to Actions tab, click "Daily OPM Data Check", click "Run workflow" to test

The token must belong to a HuggingFace account with **write access** to the `impactproject` organization's datasets.

### "Pipeline FAILED" — OPM changed their website

**What the issue will say:** Playwright timeout errors, "zero files found", or selector errors

**What's happening:** The pipeline uses a browser automation tool (Playwright) to navigate OPM's website because OPM doesn't provide direct download links. When OPM redesigns their page, the automation breaks.

**How to fix:** This requires a developer. The file `opm_pipeline/scraper.py` contains the browser automation code. Tell the developer: "The OPM website at data.opm.gov/explore-data/data/data-downloads changed its layout. The Playwright selectors in `opm_pipeline/scraper.py` need to be updated to match the new HTML."

### "Pipeline FAILED" — network/temporary error

**What the issue will say:** Connection errors, timeouts

**How to fix:** Wait a day. The pipeline runs daily, so it will retry automatically. If it fails multiple days in a row, the OPM site may be down — check https://data.opm.gov manually.

### No issues appearing at all

The pipeline only creates issues when something changes or something fails. If OPM hasn't published new data, you won't see any issues. Check the **Actions** tab — you should see runs every day even when there's nothing new.

If there are no runs at all, the GitHub Action may be disabled. Go to Actions > "Daily OPM Data Check" and click "Enable workflow" if you see that option.

## How to Run It Manually

If you need to trigger a run outside the daily schedule:

1. Go to the **Actions** tab in this repo
2. Click "Daily OPM Data Check" in the left sidebar
3. Click the "Run workflow" button
4. Click the green "Run workflow" button in the dropdown
5. (Optional) Check "test_mode" to do a dry run that processes January 2026 data without changing anything

## Credentials

This system uses two secrets, stored in GitHub repo Settings > Secrets and variables > Actions:

### HuggingFace Token (`HF_TOKEN`) — required

- **What it does:** Lets the pipeline upload parquet files to the HuggingFace dataset
- **How to get a new one:**
  1. Log in to https://huggingface.co with an account that's a member of the `impactproject` org
  2. Go to https://huggingface.co/settings/tokens
  3. Click **Create new token**, name it, set access to **Write**
  4. Copy the token and update the `HF_TOKEN` secret

If you ever move the dataset to a different HuggingFace org, also update `HF_USERNAME` in `opm_pipeline/config.py`.

### Buttondown API Key (`BUTTONDOWN_API_KEY`) — optional

- **What it does:** Sends email notifications when new data is published
- **How to set up your own:**
  1. Create a free account at https://buttondown.email
  2. Go to Settings > API to find your API key
  3. Add or update the `BUTTONDOWN_API_KEY` secret in this repo
  4. Share your Buttondown subscribe link so people can sign up for notifications

If this secret is missing, the email step fails but everything else (data upload, GitHub Issue) still works fine.

## If You Need to Change Something

| I want to... | Do this |
|---|---|
| Change when it runs | Edit `.github/workflows/daily_check.yml`, change the `cron` line |
| Change which data types it checks | Edit `opm_pipeline/config.py`, change `DATA_TYPES` |
| Change the date range | Edit `opm_pipeline/config.py`, change `START_DATE` / `END_DATE` |
| Move to a different HuggingFace account | Edit `opm_pipeline/config.py`, change `HF_USERNAME`; update `HF_TOKEN` secret |
| Re-download everything from scratch | Delete `metadata/file_manifest.json`, run with `--rebuild-manifest` |
| Stop email notifications | Delete the `BUTTONDOWN_API_KEY` secret |
| Use a different email service | Modify `send_email.py` (~35 lines); it reads `email_body.txt` and sends via API |

## Contact

If something is broken and you can't fix it with the steps above, the issue is likely in `opm_pipeline/scraper.py` (OPM changed their site). A developer familiar with Playwright/browser automation can fix it.
