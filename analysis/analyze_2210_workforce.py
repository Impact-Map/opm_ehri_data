"""
Analyze IT workforce (2210 series) changes by agency since January 2025.

Outputs:
1. Baseline 2210 count by agency (January 2025 snapshot)
2. Separations by agency and month (Feb 2025+)
3. Accessions by agency and month (Feb 2025+)
"""

from pathlib import Path
from huggingface_hub import hf_hub_download
import pandas as pd
from dotenv import load_dotenv
from tqdm import tqdm

load_dotenv()

HF_REPO = "impactproject/opm-ehri-data"
OUTPUT_DIR = Path("data/analysis")
SERIES_CODE = "2210"  # IT Specialist

# Baseline month and months to analyze for changes
BASELINE_MONTH = "202501"
CHANGE_MONTHS = ["202502", "202503", "202504", "202505", "202506",
                 "202507", "202508", "202509", "202510", "202511"]


def download_dataset(data_type: str, month: str) -> pd.DataFrame:
    """Download a single month's dataset."""
    filename = f"{data_type}_{month}.parquet"
    path = hf_hub_download(repo_id=HF_REPO, filename=filename, repo_type="dataset")
    return pd.read_parquet(path)


def get_baseline_counts() -> pd.DataFrame:
    """Get January 2025 employment counts for 2210 by agency."""
    print(f"\nDownloading baseline employment data ({BASELINE_MONTH})...")
    df = download_dataset("employment", BASELINE_MONTH)

    # Filter to 2210 and aggregate by agency
    it_df = df[df["occupational_series_code"] == SERIES_CODE]
    it_df["count"] = pd.to_numeric(it_df["count"], errors="coerce").fillna(0)

    baseline = it_df.groupby(["agency", "agency_code"])["count"].sum().reset_index()
    baseline.columns = ["agency", "agency_code", "baseline_jan2025"]
    baseline = baseline.sort_values("baseline_jan2025", ascending=False)

    print(f"  Found {len(baseline)} agencies with 2210 employees")
    print(f"  Total 2210 workforce: {baseline['baseline_jan2025'].sum():,.0f}")

    return baseline


def get_monthly_changes(data_type: str) -> pd.DataFrame:
    """Get monthly separations or accessions for 2210 by agency.

    Note: Filters on personnel_action_effective_date_yyyymm to exclude
    delayed reporting from prior years (each file has ~50 rows from 2 years ago).
    """
    print(f"\nDownloading {data_type} data...")
    date_col = "personnel_action_effective_date_yyyymm"

    all_data = []
    for month in tqdm(CHANGE_MONTHS, desc=f"  {data_type}"):
        try:
            df = download_dataset(data_type, month)
            # Filter to 2210 AND only include actions that actually happened in 2025
            it_df = df[(df["occupational_series_code"] == SERIES_CODE) &
                       (df[date_col].str.startswith("2025"))]
            it_df = it_df.copy()
            it_df["count"] = pd.to_numeric(it_df["count"], errors="coerce").fillna(0)

            monthly = it_df.groupby(["agency", "agency_code"])["count"].sum().reset_index()
            monthly["month"] = month
            all_data.append(monthly)
        except Exception as e:
            print(f"    Error with {month}: {e}")

    if not all_data:
        return pd.DataFrame()

    combined = pd.concat(all_data, ignore_index=True)

    # Pivot to wide format: one column per month
    pivoted = combined.pivot_table(
        index=["agency", "agency_code"],
        columns="month",
        values="count",
        fill_value=0
    ).reset_index()

    # Flatten column names
    pivoted.columns = [f"{data_type}_{col}" if col not in ["agency", "agency_code"] else col
                       for col in pivoted.columns]

    # Add total column
    month_cols = [c for c in pivoted.columns if c.startswith(f"{data_type}_20")]
    pivoted[f"{data_type}_total"] = pivoted[month_cols].sum(axis=1)

    return pivoted


def main():
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    print("="*70)
    print("ANALYZING 2210 (IT SPECIALIST) WORKFORCE CHANGES")
    print("="*70)

    # Get baseline
    baseline = get_baseline_counts()

    # Get separations and accessions
    separations = get_monthly_changes("separations")
    accessions = get_monthly_changes("accessions")

    # Merge everything
    print("\nMerging data...")
    result = baseline.merge(separations, on=["agency", "agency_code"], how="left")
    result = result.merge(accessions, on=["agency", "agency_code"], how="left")
    result = result.fillna(0)

    # Calculate net change and percentage
    result["net_change"] = result["accessions_total"] - result["separations_total"]
    result["pct_change"] = (result["net_change"] / result["baseline_jan2025"] * 100).round(2)
    result["pct_separated"] = (result["separations_total"] / result["baseline_jan2025"] * 100).round(2)

    # Sort by percentage change (most negative first)
    result = result.sort_values("pct_change")

    # Save
    output_file = OUTPUT_DIR / "2210_workforce_analysis.csv"
    result.to_csv(output_file, index=False)

    # Summary
    print("\n" + "="*70)
    print("SUMMARY")
    print("="*70)

    total_baseline = result["baseline_jan2025"].sum()
    total_sep = result["separations_total"].sum()
    total_acc = result["accessions_total"].sum()
    total_net = total_acc - total_sep

    print(f"\nTotal 2210 workforce (Jan 2025): {total_baseline:,.0f}")
    print(f"Total separations (Feb-Nov 2025): {total_sep:,.0f}")
    print(f"Total accessions (Feb-Nov 2025):  {total_acc:,.0f}")
    print(f"Net change:                       {total_net:+,.0f} ({total_net/total_baseline*100:+.1f}%)")

    # Filter to agencies with at least 50 employees to avoid noise
    significant = result[result["baseline_jan2025"] >= 50]

    print(f"\nTop 10 agencies by % workforce lost (min 50 employees):")
    print("-"*90)
    worst = significant.head(10)[["agency_code", "agency", "baseline_jan2025",
                                   "separations_total", "accessions_total", "pct_change"]]
    for _, row in worst.iterrows():
        agency_name = row['agency'][:45] + "..." if len(row['agency']) > 45 else row['agency']
        print(f"  {row['pct_change']:+6.1f}% | Base: {row['baseline_jan2025']:>5,.0f} | "
              f"Sep: {row['separations_total']:>4,.0f} | Acc: {row['accessions_total']:>4,.0f} | "
              f"{agency_name}")

    print(f"\n  Output: {output_file}")
    size_kb = output_file.stat().st_size / 1024
    print(f"  Rows: {len(result)}")
    print(f"  Size: {size_kb:.1f} KB")


if __name__ == "__main__":
    main()
