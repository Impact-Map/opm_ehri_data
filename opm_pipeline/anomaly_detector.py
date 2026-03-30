"""Detect anomalies in federal workforce data between two employment snapshots.

Produces narrative-ready output: gov-wide summary, agency profiles,
cross-cutting themes, and individual outlier findings.
"""

from __future__ import annotations

import re
import json
from datetime import datetime, timezone
from pathlib import Path

import duckdb
import pandas as pd
import numpy as np
from huggingface_hub import list_repo_files, hf_hub_download

from opm_pipeline.config import HF_REPO

# Dimensions to scan
DIMENSIONS = [
    ("age_bracket", "Age Bracket"),
    ("education_level_bracket", "Education Level"),
    ("grade", "Grade"),
    ("pay_plan", "Pay Plan"),
    ("supervisory_status", "Supervisory Status"),
    ("occupational_category", "Occupational Category"),
    ("duty_station_state", "Duty Station State"),
    ("duty_station_county", "Duty Station County"),
    ("veteran_indicator", "Veteran Status"),
    ("work_schedule", "Work Schedule"),
    ("occupational_series", "Occupational Series"),
    ("tenure", "Tenure"),
    ("appointment_type", "Appointment Type"),
]

MIN_COUNT_DEFAULT = 200
MIN_CHANGE_DEFAULT = 50
TOP_N_DEFAULT = 50


def get_employment_months(token: str | None = None) -> dict[str, str]:
    """Return {YYYYMM: hf_path} for the best employment file per month."""
    pattern = re.compile(r"^employment/employment_(\d{6})_v(\d+)\.parquet$")
    best: dict[str, tuple[int, str]] = {}
    for f in list_repo_files(HF_REPO, repo_type="dataset", token=token):
        m = pattern.match(f)
        if m:
            yyyymm, ver = m.group(1), int(m.group(2))
            if yyyymm not in best or ver > best[yyyymm][0]:
                best[yyyymm] = (ver, f)
    return {m: best[m][1] for m in sorted(best)}


def _get_separations_path(month: str, token: str | None = None) -> str | None:
    """Download and return local path for the best separations file for a given month."""
    pattern = re.compile(r"^separations/separations_(\d{6})_v(\d+)\.parquet$")
    best: tuple[int, str] | None = None
    for f in list_repo_files(HF_REPO, repo_type="dataset", token=token):
        m = pattern.match(f)
        if m and m.group(1) == month:
            ver = int(m.group(2))
            if best is None or ver > best[0]:
                best = (ver, f)
    if best is None:
        return None
    return _hf_local(best[1], token)


def _hf_local(hf_path: str, token: str | None = None) -> str:
    """Download a file from HuggingFace and return the local path (cached)."""
    return hf_hub_download(
        repo_id=HF_REPO, filename=hf_path, repo_type="dataset", token=token,
    )


def load_employment(hf_path: str, token: str | None = None) -> pd.DataFrame:
    """Download and load an employment parquet file from HuggingFace."""
    local = _hf_local(hf_path, token)
    df = duckdb.execute(f"SELECT * FROM read_parquet('{local}')").df()
    df["count"] = df["count"].astype(int)
    return df


def _name_similarity(a: str, b: str) -> float:
    """Fraction of words shared between two agency names."""
    words_a = set(a.upper().split())
    words_b = set(b.upper().split())
    # Remove trivial words
    stop = {"OF", "THE", "AND", "FOR", "IN", "TO", "A", "AN"}
    words_a -= stop
    words_b -= stop
    if not words_a or not words_b:
        return 0.0
    shared = words_a & words_b
    return len(shared) / min(len(words_a), len(words_b))


def detect_renames(
    df_old: pd.DataFrame, df_new: pd.DataFrame, tolerance: float = 0.05,
) -> list[dict]:
    """Find agencies that disappeared and reappeared under a new name.

    Requires both headcount proximity AND name similarity (at least 40%
    shared words) to avoid false matches over long time spans.
    """
    old_totals = df_old.groupby("agency_subelement")["count"].sum()
    new_totals = df_new.groupby("agency_subelement")["count"].sum()

    disappeared = set(old_totals.index) - set(new_totals.index)
    appeared = set(new_totals.index) - set(old_totals.index)

    renames = []
    matched_new: set[str] = set()
    for old_name in disappeared:
        old_n = old_totals[old_name]
        if old_n < 10:
            continue
        best_match = None
        best_diff = float("inf")
        for new_name in appeared - matched_new:
            new_n = new_totals[new_name]
            diff = abs(new_n - old_n) / old_n
            if diff < tolerance and diff < best_diff:
                # Also require name similarity
                if _name_similarity(old_name, new_name) >= 0.4:
                    best_match = new_name
                    best_diff = diff
        if best_match:
            matched_new.add(best_match)
            renames.append({
                "old_name": old_name,
                "new_name": best_match,
                "old_count": int(old_n),
                "new_count": int(new_totals[best_match]),
            })
    return renames


# ── Gov-wide summary ──────────────────────────────────────────────────────────

def _build_summary(df_old, df_new, exclude):
    """Build gov-wide summary stats."""
    old_total = int(df_old[~df_old["agency_subelement"].isin(exclude)]["count"].sum())
    new_total = int(df_new[~df_new["agency_subelement"].isin(exclude)]["count"].sum())
    change = new_total - old_total
    pct = round(change / old_total * 100, 1) if old_total else 0

    # Agency-level changes
    old_by_agency = (
        df_old[~df_old["agency_subelement"].isin(exclude)]
        .groupby("agency_subelement")["count"].sum()
    )
    new_by_agency = (
        df_new[~df_new["agency_subelement"].isin(exclude)]
        .groupby("agency_subelement")["count"].sum()
    )
    merged = pd.DataFrame({"old": old_by_agency, "new": new_by_agency}).fillna(0)
    merged["change"] = merged["new"] - merged["old"]
    n_shrinking = int((merged["change"] < 0).sum())
    n_growing = int((merged["change"] > 0).sum())
    n_stable = int((merged["change"] == 0).sum())

    # Biggest movers (by absolute change, min 100 employees)
    big = merged[merged[["old", "new"]].max(axis=1) >= 100].copy()
    big["pct"] = (big["change"] / big["old"].replace(0, np.nan) * 100).round(1).fillna(0)
    top_losses = big.nsmallest(5, "change")
    top_gains = big.nlargest(5, "change")
    top_gains = top_gains[top_gains["change"] > 0]

    return {
        "old_total": old_total,
        "new_total": new_total,
        "change": change,
        "pct_change": pct,
        "agencies_shrinking": n_shrinking,
        "agencies_growing": n_growing,
        "agencies_stable": n_stable,
        "top_losses": [
            {"agency": name, "change": int(row["change"]), "pct": float(row["pct"]),
             "old": int(row["old"]), "new": int(row["new"])}
            for name, row in top_losses.iterrows()
        ],
        "top_gains": [
            {"agency": name, "change": int(row["change"]), "pct": float(row["pct"]),
             "old": int(row["old"]), "new": int(row["new"])}
            for name, row in top_gains.iterrows()
        ],
    }


# ── Agency profiles ───────────────────────────────────────────────────────────

def _build_agency_profiles(df_old, df_new, outliers_df, changes_df, exclude, min_size=500):
    """Build per-agency profiles showing overall change and what's driving it."""
    old_by = (
        df_old[~df_old["agency_subelement"].isin(exclude)]
        .groupby(["agency", "agency_subelement"])["count"].sum()
        .reset_index().rename(columns={"count": "old_total"})
    )
    new_by = (
        df_new[~df_new["agency_subelement"].isin(exclude)]
        .groupby(["agency", "agency_subelement"])["count"].sum()
        .reset_index().rename(columns={"count": "new_total"})
    )
    merged = old_by.merge(new_by, on=["agency", "agency_subelement"], how="outer").fillna(0)
    merged["old_total"] = merged["old_total"].astype(int)
    merged["new_total"] = merged["new_total"].astype(int)
    merged["change"] = merged["new_total"] - merged["old_total"]
    merged["pct_change"] = (merged["change"] / merged["old_total"].replace(0, np.nan) * 100).round(1)

    # Only profile agencies big enough and with outlier findings
    agencies_with_findings = set(outliers_df["agency_subelement"].unique())
    merged = merged[
        (merged["agency_subelement"].isin(agencies_with_findings)) &
        (merged[["old_total", "new_total"]].max(axis=1) >= min_size)
    ]

    profiles = []
    for _, row in merged.sort_values("change").iterrows():
        subagency = row["agency_subelement"]
        # Get this agency's outlier findings
        agency_outliers = outliers_df[outliers_df["agency_subelement"] == subagency]
        drivers = []
        for _, o in agency_outliers.nlargest(5, "abs_z").iterrows():
            drivers.append({
                "dimension": str(o["dimension"]),
                "group": str(o["group"]),
                "old_count": int(o["old_count"]),
                "new_count": int(o["new_count"]),
                "change": int(o["change"]),
                "pct_change": round(float(o["pct_change"]), 1),
            })

        profiles.append({
            "agency": str(row["agency"]),
            "agency_subelement": str(subagency),
            "old_total": int(row["old_total"]),
            "new_total": int(row["new_total"]),
            "change": int(row["change"]),
            "pct_change": float(row["pct_change"]) if pd.notna(row["pct_change"]) else None,
            "drivers": drivers,
        })

    return profiles


# ── Separations summary ──────────────────────────────────────────────────────

def _build_separations_summary(new_month, token, exclude):
    """Load separations for the new month and summarize by category and agency."""
    local = _get_separations_path(new_month, token)
    if not local:
        return None

    df = duckdb.execute(f"SELECT * FROM read_parquet('{local}')").df()
    df["count"] = df["count"].astype(int)
    # Filter to effective date matching the month
    if "personnel_action_effective_date_yyyymm" in df.columns:
        df = df[df["personnel_action_effective_date_yyyymm"] == new_month]

    if df.empty:
        return None

    df = df[~df["agency_subelement"].isin(exclude)]
    total = int(df["count"].sum())

    # By category
    by_cat = (
        df.groupby("separation_category")["count"].sum()
        .sort_values(ascending=False)
        .reset_index()
    )
    categories = [
        {"category": row["separation_category"], "count": int(row["count"]),
         "pct": round(int(row["count"]) / total * 100, 1)}
        for _, row in by_cat.iterrows()
    ]

    # Top agencies by separation count
    by_agency = (
        df.groupby(["agency", "agency_subelement"])["count"].sum()
        .reset_index().sort_values("count", ascending=False)
    )
    top_agencies = [
        {"agency": row["agency"], "agency_subelement": row["agency_subelement"],
         "count": int(row["count"])}
        for _, row in by_agency.head(10).iterrows()
    ]

    # DRP breakdown
    drp = None
    if "drp_indicator" in df.columns:
        drp_total = int(df[df["drp_indicator"] == "Y"]["count"].sum())
        drp_pct = round(drp_total / total * 100, 1) if total else 0
        # DRP by agency
        drp_by_agency = (
            df[df["drp_indicator"] == "Y"]
            .groupby(["agency", "agency_subelement"])["count"].sum()
            .reset_index().sort_values("count", ascending=False)
        )
        drp_top = [
            {"agency": row["agency"], "agency_subelement": row["agency_subelement"],
             "count": int(row["count"])}
            for _, row in drp_by_agency.head(10).iterrows()
        ]
        # DRP by separation category
        drp_by_cat = (
            df[df["drp_indicator"] == "Y"]
            .groupby("separation_category")["count"].sum()
            .sort_values(ascending=False).reset_index()
        )
        drp_categories = [
            {"category": row["separation_category"], "count": int(row["count"]),
             "pct": round(int(row["count"]) / drp_total * 100, 1) if drp_total else 0}
            for _, row in drp_by_cat.iterrows()
        ]
        drp = {
            "total": drp_total,
            "pct_of_all_separations": drp_pct,
            "by_category": drp_categories,
            "top_agencies": drp_top,
        }

    return {
        "total": total,
        "by_category": categories,
        "top_agencies": top_agencies,
        "drp": drp,
    }


# ── Dimension changes & outliers ─────────────────────────────────────────────

EXCLUDE_VALUES = {
    "duty_station_county": {"REDACTED", "NO DATA REPORTED", "INVALID"},
    "duty_station_state": {"REDACTED", "NO DATA REPORTED", "INVALID"},
}


def _compute_changes(df_old, df_new, dim_col, min_count, exclude_agencies):
    """Compute headcount changes for one dimension across all agencies."""
    if dim_col not in df_old.columns or dim_col not in df_new.columns:
        return pd.DataFrame()

    group_cols = ["agency", "agency_subelement", dim_col]
    skip_vals = EXCLUDE_VALUES.get(dim_col, set())

    old_filtered = df_old[
        ~df_old["agency_subelement"].isin(exclude_agencies) &
        ~df_old[dim_col].isin(skip_vals)
    ]
    new_filtered = df_new[
        ~df_new["agency_subelement"].isin(exclude_agencies) &
        ~df_new[dim_col].isin(skip_vals)
    ]

    old_agg = (
        old_filtered.groupby(group_cols)["count"].sum().reset_index()
        .rename(columns={"count": "old_count"})
    )
    new_agg = (
        new_filtered.groupby(group_cols)["count"].sum().reset_index()
        .rename(columns={"count": "new_count"})
    )

    merged = old_agg.merge(new_agg, on=group_cols, how="outer").fillna(0)
    merged["agency"] = merged["agency"].replace(0, np.nan).ffill()
    merged["old_count"] = merged["old_count"].astype(int)
    merged["new_count"] = merged["new_count"].astype(int)

    merged = merged[
        (merged["old_count"] >= min_count) & (merged["new_count"] >= min_count)
    ].copy()

    if merged.empty:
        return merged

    merged["change"] = merged["new_count"] - merged["old_count"]
    merged["pct_change"] = merged["change"] / merged["old_count"] * 100

    mean_pct = merged["pct_change"].mean()
    std_pct = merged["pct_change"].std()
    if std_pct > 0:
        merged["z_score"] = (merged["pct_change"] - mean_pct) / std_pct
    else:
        merged["z_score"] = 0.0

    merged["abs_z"] = merged["z_score"].abs()
    merged["combined_score"] = merged["pct_change"].abs() * merged["abs_z"]

    return merged


# ── Themes (cross-agency patterns) ───────────────────────────────────────────

def _detect_themes(outliers_df, changes_df, min_agencies=4):
    """Find cross-agency patterns that are disproportionate vs their dimension."""
    # Median % change per group and per dimension overall
    dim_baselines = (
        changes_df.groupby(["dimension", "group"])["pct_change"]
        .median().reset_index().rename(columns={"pct_change": "group_median_pct"})
    )
    dim_overall = (
        changes_df.groupby("dimension")["pct_change"]
        .median().reset_index().rename(columns={"pct_change": "dim_median_pct"})
    )
    dim_baselines = dim_baselines.merge(dim_overall, on="dimension")
    dim_baselines["deviation"] = dim_baselines["group_median_pct"] - dim_baselines["dim_median_pct"]

    themes = []
    for (dim, grp), group_df in outliers_df.groupby(["dimension", "group"]):
        if len(group_df) < min_agencies:
            continue
        n_neg = (group_df["pct_change"] < 0).sum()
        n_pos = (group_df["pct_change"] > 0).sum()
        if n_neg < min_agencies and n_pos < min_agencies:
            continue

        if n_neg >= min_agencies:
            subset = group_df[group_df["pct_change"] < 0]
            direction = "dropped"
        else:
            subset = group_df[group_df["pct_change"] > 0]
            direction = "increased"

        baseline = dim_baselines[
            (dim_baselines["dimension"] == dim) & (dim_baselines["group"] == grp)
        ]
        if len(baseline) > 0:
            group_median = round(float(baseline.iloc[0]["group_median_pct"]), 1)
            dim_median = round(float(baseline.iloc[0]["dim_median_pct"]), 1)
            deviation = round(float(baseline.iloc[0]["deviation"]), 1)
        else:
            group_median = round(float(subset["pct_change"].median()), 1)
            dim_median = 0.0
            deviation = group_median

        if abs(deviation) < 5:
            continue

        pct_min = round(abs(subset["pct_change"].min()), 1)
        pct_max = round(abs(subset["pct_change"].max()), 1)
        total_change = int(subset["change"].sum())

        # Per-agency detail for this theme
        agencies = []
        for _, r in subset.sort_values("change").iterrows():
            agencies.append({
                "agency": str(r.get("agency", "")),
                "agency_subelement": str(r["agency_subelement"]),
                "old_count": int(r["old_count"]),
                "new_count": int(r["new_count"]),
                "change": int(r["change"]),
                "pct_change": round(float(r["pct_change"]), 1),
            })

        themes.append({
            "dimension": str(dim),
            "group": str(grp),
            "direction": direction,
            "agency_count": len(subset),
            "pct_range": [min(pct_min, pct_max), max(pct_min, pct_max)],
            "total_change": total_change,
            "group_median_pct": group_median,
            "dimension_median_pct": dim_median,
            "deviation_pp": deviation,
            "agencies": agencies,
        })

    themes.sort(key=lambda t: abs(t["deviation_pp"]), reverse=True)
    return themes


# ── Main entry point ─────────────────────────────────────────────────────────

def detect_anomalies(
    new_month: str,
    old_month: str,
    token: str | None = None,
    top_n: int = TOP_N_DEFAULT,
    min_count: int = MIN_COUNT_DEFAULT,
    min_change: int = MIN_CHANGE_DEFAULT,
    z_threshold: float = 2.5,
) -> dict:
    """Compare two employment months and return narrative-ready anomaly data."""
    months = get_employment_months(token)
    if new_month not in months:
        raise ValueError(f"Month {new_month} not found. Available: {sorted(months)}")
    if old_month not in months:
        raise ValueError(f"Month {old_month} not found. Available: {sorted(months)}")

    print(f"Loading {old_month}...", flush=True)
    df_old = load_employment(months[old_month], token)
    if "snapshot_yyyymm" in df_old.columns:
        df_old = df_old[df_old["snapshot_yyyymm"] == old_month]

    print(f"Loading {new_month}...", flush=True)
    df_new = load_employment(months[new_month], token)
    if "snapshot_yyyymm" in df_new.columns:
        df_new = df_new[df_new["snapshot_yyyymm"] == new_month]

    # Renames
    print("Detecting agency renames...", flush=True)
    renames = detect_renames(df_old, df_new)
    exclude = set()
    for r in renames:
        exclude.add(r["old_name"])
        exclude.add(r["new_name"])
    if renames:
        print(f"  Found {len(renames)} rename(s)")

    # Gov-wide summary
    print("Building summary...", flush=True)
    summary = _build_summary(df_old, df_new, exclude)

    # Separations
    print("Loading separations...", flush=True)
    separations = _build_separations_summary(new_month, token, exclude)

    # Scan dimensions
    all_changes = []
    all_outliers = []
    available_dims = [
        (col, label) for col, label in DIMENSIONS
        if col in df_old.columns and col in df_new.columns
    ]

    for dim_col, dim_label in available_dims:
        print(f"Scanning {dim_label}...", flush=True)
        changes = _compute_changes(df_old, df_new, dim_col, min_count, exclude)
        if changes.empty:
            continue
        changes["dimension"] = dim_label
        changes["dim_col"] = dim_col
        changes["group"] = changes[dim_col]
        all_changes.append(changes)
        outliers = changes[
            (changes["abs_z"] >= z_threshold) &
            (changes["change"].abs() >= min_change)
        ].copy()
        all_outliers.append(outliers)

    if not all_changes:
        return _build_output(new_month, old_month, summary, renames, separations, [], [], [])

    outliers_df = pd.concat(all_outliers, ignore_index=True) if all_outliers else pd.DataFrame()
    changes_df = pd.concat(all_changes, ignore_index=True)

    # Themes
    print("Detecting themes...", flush=True)
    themes = _detect_themes(outliers_df, changes_df) if not outliers_df.empty else []

    # Agency profiles
    print("Building agency profiles...", flush=True)
    profiles = _build_agency_profiles(
        df_old, df_new, outliers_df, changes_df, exclude
    ) if not outliers_df.empty else []

    # Individual findings (deduped, ranked)
    findings = []
    if not outliers_df.empty:
        deduped = []
        for _, group in outliers_df.groupby("agency_subelement"):
            deduped.append(group.nlargest(3, "abs_z"))
        deduped_df = pd.concat(deduped).sort_values("combined_score", ascending=False)

        for rank, (_, row) in enumerate(deduped_df.head(top_n).iterrows(), 1):
            findings.append({
                "rank": rank,
                "agency": str(row.get("agency", "")),
                "agency_subelement": str(row["agency_subelement"]),
                "dimension": str(row["dimension"]),
                "group": str(row["group"]),
                "old_count": int(row["old_count"]),
                "new_count": int(row["new_count"]),
                "change": int(row["change"]),
                "pct_change": round(float(row["pct_change"]), 1),
            })

    result = _build_output(
        new_month, old_month, summary, renames, separations, themes, profiles, findings,
    )
    print(f"\nDone: {len(findings)} findings, {len(themes)} themes, {len(profiles)} agency profiles.")
    return result


def _build_output(new_month, old_month, summary, renames, separations,
                  themes, agency_profiles, findings):
    return {
        "month": new_month,
        "compared_to": old_month,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "summary": summary,
        "renames_detected": renames,
        "separations": separations,
        "themes": themes,
        "agency_profiles": agency_profiles,
        "findings": findings,
    }


def save_findings(results: dict, output_dir: Path, key: str | None = None) -> Path:
    """Write findings JSON and update the index file.

    key: filename stem (without .json). Defaults to the month.
         Use e.g. '202601_vs_202412' for baseline comparisons.
    """
    output_dir.mkdir(parents=True, exist_ok=True)

    file_key = key or results["month"]
    month_file = output_dir / f"{file_key}.json"
    # Replace any NaN/Infinity with null for valid JSON
    json_str = json.dumps(results, indent=2)
    json_str = json_str.replace(": NaN", ": null").replace(": Infinity", ": null").replace(": -Infinity", ": null")
    with open(month_file, "w") as f:
        f.write(json_str)

    index_file = output_dir / "index.json"
    if index_file.exists():
        with open(index_file) as f:
            index = json.load(f)
    else:
        index = {"months": [], "baselines": []}

    entry = {
        "key": file_key,
        "month": results["month"],
        "compared_to": results["compared_to"],
        "total_findings": len(results["findings"]),
        "generated_at": results["generated_at"],
    }

    # Baseline comparisons go in a separate list
    is_baseline = key and key != results["month"]
    list_name = "baselines" if is_baseline else "months"

    if list_name not in index:
        index[list_name] = []
    index[list_name] = [m for m in index[list_name] if m.get("key", m.get("month")) != file_key]
    index[list_name].append(entry)
    index[list_name].sort(key=lambda m: m["key"], reverse=True)

    with open(index_file, "w") as f:
        json.dump(index, f, indent=2)

    return month_file
