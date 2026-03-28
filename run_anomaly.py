"""CLI entry point for anomaly detection.

Usage:
    python run_anomaly.py --month 202601                    # Compare Jan 2026 to prior month
    python run_anomaly.py --month 202601 --compare 202412   # Explicit comparison
    python run_anomaly.py --latest                          # Auto-detect latest month
    python run_anomaly.py --changed "employment/employment_202512_v2.parquet,separations/separations_202601_v1.parquet"
                                                            # Re-run all months affected by these file changes
    python run_anomaly.py --backfill                        # All available month pairs
"""

from __future__ import annotations

import argparse
import os
import re
import sys
from pathlib import Path

from opm_pipeline.anomaly_detector import (
    get_employment_months, detect_anomalies, save_findings,
)

DOCS_DATA_DIR = Path("docs/data")


MIN_MONTH = "202412"  # Only run anomaly detection for recent data


def _extract_months_from_changed(changed_keys: list[str]) -> set[str]:
    """Given a list of changed HF file paths, figure out which YYYYMM months are affected.

    Any data type (employment, separations, accessions) touching month X
    means we should re-run X. If employment for month X changed, we should
    also re-run X+1 (since it compares against X).
    Only considers months >= MIN_MONTH.
    """
    pattern = re.compile(r"^(employment|separations|accessions)/\1_(\d{6})_v\d+\.parquet$")
    direct_months = set()
    employment_months_changed = set()

    for key in changed_keys:
        m = pattern.match(key)
        if m:
            data_type, yyyymm = m.group(1), m.group(2)
            if yyyymm < MIN_MONTH:
                continue
            direct_months.add(yyyymm)
            if data_type == "employment":
                employment_months_changed.add(yyyymm)

    # If employment for month X changed, also re-run X+1
    # (because X+1's report compares against X)
    dependent_months = set()
    for yyyymm in employment_months_changed:
        year, month = int(yyyymm[:4]), int(yyyymm[4:])
        if month == 12:
            next_m = f"{year+1}01"
        else:
            next_m = f"{year}{month+1:02d}"
        dependent_months.add(next_m)

    return direct_months | dependent_months


def main():
    parser = argparse.ArgumentParser(description="Federal workforce anomaly detection")
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--month", help="Target month (YYYYMM)")
    group.add_argument("--latest", action="store_true", help="Auto-detect latest month")
    group.add_argument("--changed", help="Comma-separated list of changed HF file paths")
    group.add_argument("--backfill", action="store_true", help="Generate for all month pairs")

    parser.add_argument("--compare", help="Comparison month (YYYYMM). Default: prior month")
    parser.add_argument("--baseline", help="Baseline month (YYYYMM) to compare latest against. Saved separately from month-over-month reports.")
    parser.add_argument("--token", default=os.environ.get("HF_TOKEN"), help="HuggingFace token")
    parser.add_argument("--top-n", type=int, default=50, help="Number of findings (default 50)")
    parser.add_argument("--output-dir", type=Path, default=DOCS_DATA_DIR, help="Output directory")

    args = parser.parse_args()

    months = get_employment_months(args.token)
    month_list = sorted(months.keys())

    if len(month_list) < 2:
        print("Need at least 2 employment months.")
        sys.exit(1)

    if args.backfill:
        pairs = [(month_list[i], month_list[i - 1]) for i in range(1, len(month_list))]
    elif args.latest:
        pairs = [(month_list[-1], month_list[-2])]
    elif args.changed:
        changed_keys = [k.strip() for k in args.changed.split(",") if k.strip()]
        target_months = _extract_months_from_changed(changed_keys)
        # Only run for months where we actually have employment data
        # and a prior month to compare against
        pairs = []
        for m in sorted(target_months):
            if m in months:
                idx = month_list.index(m)
                if idx > 0:
                    pairs.append((m, month_list[idx - 1]))
        if not pairs:
            print(f"No runnable month pairs from changed files: {changed_keys}")
            print(f"  Affected months: {target_months}")
            print(f"  Available employment months: {month_list}")
            sys.exit(0)
    else:
        new_month = args.month
        if new_month not in months:
            print(f"Month {new_month} not found. Available: {month_list}")
            sys.exit(1)
        if args.compare:
            old_month = args.compare
        else:
            idx = month_list.index(new_month)
            if idx == 0:
                print(f"No prior month for {new_month}.")
                sys.exit(1)
            old_month = month_list[idx - 1]
        pairs = [(new_month, old_month)]

    # If --baseline, add a baseline comparison for latest vs that month
    baseline_pair = None
    if args.baseline:
        if args.baseline not in months:
            print(f"Baseline month {args.baseline} not found. Available: {month_list}")
            sys.exit(1)
        baseline_pair = (month_list[-1], args.baseline)

    # Deduplicate pairs (same month might come from multiple changed files)
    pairs = list(dict.fromkeys(pairs))

    for new_month, old_month in pairs:
        print(f"\n{'='*60}")
        print(f"Anomaly detection: {old_month} → {new_month}")
        print(f"{'='*60}")

        results = detect_anomalies(
            new_month, old_month, token=args.token,
            top_n=args.top_n,
        )

        out_file = save_findings(results, args.output_dir)
        print(f"Saved to {out_file}")
        print(f"  {len(results['findings'])} findings, {len(results['renames_detected'])} renames")

    # Baseline comparison
    if baseline_pair:
        new_month, old_month = baseline_pair
        file_key = f"{new_month}_vs_{old_month}"
        print(f"\n{'='*60}")
        print(f"Baseline comparison: {old_month} → {new_month}")
        print(f"{'='*60}")

        results = detect_anomalies(
            new_month, old_month, token=args.token,
            top_n=args.top_n,
        )

        out_file = save_findings(results, args.output_dir, key=file_key)
        print(f"Saved to {out_file}")
        print(f"  {len(results['findings'])} findings, {len(results['renames_detected'])} renames")


if __name__ == "__main__":
    main()
