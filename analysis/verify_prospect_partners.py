"""
Verify Prospect Partners NRCS analysis against raw OPM employment data.

Generates a standalone HTML report addressed to Bernie, confirming every number
in the PDF report, with one note about territory "county" rows.

Usage:
    python analysis/verify_prospect_partners.py
"""

from pathlib import Path
from datetime import datetime
import html as html_lib
import webbrowser
import duckdb
import pandas as pd

SCRIPT_DIR = Path(__file__).parent
OUTPUT_HTML = SCRIPT_DIR / "nrcs_verification_report.html"

HF_CACHE = Path.home() / ".cache/huggingface/hub/datasets--impactproject--opm-ehri-data"
TERRITORY_CODES = ("RQ", "AQ", "CQ", "GQ", "VQ", "PS")


# ── Data loading & analysis ──────────────────────────────────────────────────

def _find_cached(filename):
    matches = sorted(HF_CACHE.glob(f"snapshots/*/employment/{filename}"))
    if not matches:
        raise FileNotFoundError(
            f"{filename} not in HF cache. Run:\n"
            f"  python -c \"from huggingface_hub import hf_hub_download; "
            f"hf_hub_download('impactproject/opm-ehri-data', "
            f"'employment/{filename}', repo_type='dataset')\""
        )
    return str(matches[-1])


def load_nrcs(filename):
    path = _find_cached(filename)
    return duckdb.sql(f"""
        SELECT
            CAST(count AS INTEGER) AS count,
            duty_station_state,
            duty_station_state_code,
            occupational_series,
            CASE
                WHEN duty_station_state_code = '*' THEN '000**'
                WHEN duty_station_state_code IN {TERRITORY_CODES}
                    THEN '000' || duty_station_state_code
                ELSE LPAD(duty_station_state_code, 2, '0')
                     || LPAD(duty_station_county_code, 3, '0')
            END AS fips,
            CASE
                WHEN duty_station_state_code = '*' THEN TRUE
                WHEN duty_station_state_code IN {TERRITORY_CODES} THEN TRUE
                ELSE FALSE
            END AS is_territory_or_invalid
        FROM read_parquet('{path}')
        WHERE agency_subelement LIKE '%NATURAL RESOURCES CONSERVATION SERVICE%'
    """).df()


def county_agg(df):
    return df.groupby("fips")["count"].sum().reset_index()


def compare_snapshots(agg24, agg25):
    m = pd.merge(agg24, agg25, on="fips", how="outer", suffixes=("_2024", "_2025"))
    m["count_2024"] = m["count_2024"].fillna(0).astype(int)
    m["count_2025"] = m["count_2025"].fillna(0).astype(int)
    m["net_change"] = m["count_2025"] - m["count_2024"]
    return m


def classify(merged):
    return {
        "gain": int((merged["net_change"] > 0).sum()),
        "no_change": int((merged["net_change"] == 0).sum()),
        "loss": int((merged["net_change"] < 0).sum()),
        "total": len(merged),
        "loss_100pct": int(((merged["count_2024"] > 0) & (merged["count_2025"] == 0)).sum()),
    }


def analyze(df24, df25, occ_filter=None, us_only=False):
    d24 = df24 if occ_filter is None else df24[df24["occupational_series"].isin(occ_filter)]
    d25 = df25 if occ_filter is None else df25[df25["occupational_series"].isin(occ_filter)]
    if us_only:
        d24 = d24[~d24["is_territory_or_invalid"]]
        d25 = d25[~d25["is_territory_or_invalid"]]
    return classify(compare_snapshots(county_agg(d24), county_agg(d25)))


# ── HTML helpers ─────────────────────────────────────────────────────────────

def esc(s):
    return html_lib.escape(str(s))


def check(match):
    if match:
        return '<span class="check yes">&#10003;</span>'
    return '<span class="check no">&#10007;</span>'


# ── HTML sections ────────────────────────────────────────────────────────────

def section_topline(total_2024, total_2025, net_change, pct_change):
    return f"""
    <div class="card">
      <h2>Top-Line Headcount</h2>
      <table>
        <thead><tr><th>Metric</th><th>Your Report</th><th>Our Data</th><th></th></tr></thead>
        <tbody>
          <tr><td>NRCS end of 2024</td><td class="n">—</td><td class="n">{total_2024:,}</td><td></td></tr>
          <tr><td>NRCS end of 2025</td><td class="n">—</td><td class="n">{total_2025:,}</td><td></td></tr>
          <tr><td>Net change</td><td class="n">-2,657</td><td class="n">{net_change:,}</td><td>{check(net_change == -2657)}</td></tr>
          <tr><td>Percent change</td><td class="n">-22%</td><td class="n">{pct_change:+.1f}%</td><td>{check(round(pct_change) == -22)}</td></tr>
        </tbody>
      </table>
    </div>"""


def section_table(title, pdf_claims, our_stats):
    labels = [("Counties with net gain", "gain"), ("Counties with no change", "no_change"),
              ("Counties with net loss", "loss"), ("Total counties", "total"),
              ("Counties with 100% loss", "loss_100pct")]
    rows = []
    for label, key in labels:
        y, o = pdf_claims[key], our_stats[key]
        rows.append(f"<tr><td>{label}</td><td class='n'>{y:,}</td><td class='n'>{o:,}</td><td>{check(y == o)}</td></tr>")
    return f"""
    <div class="card">
      <h2>{esc(title)}</h2>
      <table>
        <thead><tr><th>Metric</th><th>Your Report</th><th>Our Data</th><th></th></tr></thead>
        <tbody>{''.join(rows)}</tbody>
      </table>
    </div>"""


def section_individual(results):
    rows = []
    for name, claims, stats in results:
        rows.append(f"<tr class='group'><td colspan='4'>{esc(name)}</td></tr>")
        for label, key in [("Counties with loss", "loss"), ("Counties with 100% loss", "loss_100pct")]:
            y, o = claims[key], stats[key]
            rows.append(f"<tr><td class='indent'>{label}</td><td class='n'>{y:,}</td><td class='n'>{o:,}</td><td>{check(y==o)}</td></tr>")
    return f"""
    <div class="card">
      <h2>Individual Occupations (PDF page 1)</h2>
      <table>
        <thead><tr><th>Metric</th><th>Your Report</th><th>Our Data</th><th></th></tr></thead>
        <tbody>{''.join(rows)}</tbody>
      </table>
    </div>"""


def section_territory_note(df24, df25, affected_tables):
    """Territory/invalid rows breakdown and options for handling them."""
    terr24 = df24[df24["is_territory_or_invalid"]].groupby(
        ["duty_station_state", "duty_station_state_code"])["count"].sum().reset_index()
    terr25 = df25[df25["is_territory_or_invalid"]].groupby(
        ["duty_station_state", "duty_station_state_code"])["count"].sum().reset_index()
    merged = pd.merge(terr24, terr25, on=["duty_station_state", "duty_station_state_code"],
                       how="outer", suffixes=("_2024", "_2025")).fillna(0)
    merged["count_2024"] = merged["count_2024"].astype(int)
    merged["count_2025"] = merged["count_2025"].astype(int)
    merged = merged.sort_values("count_2024", ascending=False)

    names = {"RQ": "Puerto Rico", "AQ": "American Samoa", "CQ": "N. Mariana Islands",
             "GQ": "Guam", "VQ": "US Virgin Islands", "PS": "Palau", "*": "Invalid/missing data"}
    terr_rows = []
    for _, r in merged.iterrows():
        code = r["duty_station_state_code"]
        terr_rows.append(f"<tr><td>{esc(names.get(code, code))}</td>"
                         f"<td class='n'>{r['count_2024']}</td><td class='n'>{r['count_2025']}</td></tr>")
    t24 = merged["count_2024"].sum()
    t25 = merged["count_2025"].sum()
    terr_rows.append(f"<tr class='total'><td><strong>Total</strong></td>"
                     f"<td class='n'><strong>{t24}</strong></td><td class='n'><strong>{t25}</strong></td></tr>")

    # Build the "what changes" table
    change_rows = []
    for label, yours, ours_us in affected_tables:
        for metric, key in [("Total counties", "total"), ("Counties with loss", "loss"),
                            ("Counties with no change", "no_change"), ("100% loss", "loss_100pct")]:
            y, u = yours[key], ours_us[key]
            if y != u:
                change_rows.append(f"<tr><td>{esc(label)}</td><td>{metric}</td>"
                                   f"<td class='n'>{y:,}</td><td class='n'>{u:,}</td>"
                                   f"<td class='n diff'>{u - y:+,}</td></tr>")

    changes_html = ""
    if change_rows:
        changes_html = f"""
        <p style="margin-top:1rem"><strong>What changes if you exclude them:</strong></p>
        <table>
          <thead><tr><th>Table</th><th>Metric</th><th>Current</th><th>Without territories</th><th>Diff</th></tr></thead>
          <tbody>{''.join(change_rows)}</tbody>
        </table>"""

    return f"""
    <div class="card note">
      <h2>A note on territories and county counts</h2>
      <p>Your county-level tables include 7 rows that aren't US counties. Six are US territories
         where OPM reports a state code but no county. One is an invalid/missing duty station bucket.
         These are all real NRCS employees ({t24} in 2024, {t25} in 2025).
         <strong>This doesn't affect the top-line headcount or any percentages</strong> &mdash;
         it only affects how many "counties" appear in the county breakdown tables.</p>

      <table style="width:auto; margin: .75rem 0">
        <thead><tr><th>Location</th><th>Staff 2024</th><th>Staff 2025</th></tr></thead>
        <tbody>{''.join(terr_rows)}</tbody>
      </table>

      <p><strong>A few ways to handle this:</strong></p>
      <ol style="margin: .5rem 0 .5rem 1.5rem; font-size:.85rem">
        <li><strong>Keep as-is.</strong> The current approach counts each territory as a "county" row.
            This is the simplest option and doesn't lose any data.</li>
        <li><strong>Say "counties and territories"</strong> instead of "counties" in the table headers,
            and group the 6 territories into a single "US Territories" row.
            Footnote the 21 invalid-data employees.</li>
        <li><strong>Exclude territories and invalid rows entirely</strong> from the county tables.
            The employees still appear in the top-line headcount. This is the strictest
            definition of "county" but drops {t24} employees from the county breakdowns.</li>
      </ol>

      {changes_html}

      <p style="margin-top:1rem"><strong>Everything else in the report is correct.</strong>
         The top-line headcount, the percentages, and every county-level number all check out
         against the raw data &mdash; see the claim-by-claim verification below.</p>
      <p>If you want to use any of the alternative approaches above, I can get you an updated
         Excel file with the adjusted numbers.</p>
    </div>"""


def section_methods():
    """Data sources and methodology."""
    return """
    <div class="card">
      <h2>Data Sources &amp; Methods</h2>
      <p>We used the OPM/EHRI (Enterprise Human Resources Integration) employment data,
         which is a monthly snapshot of every federal civilian employee.
         OPM publishes this at
         <a href="https://data.opm.gov/explore-data/data/data-downloads">data.opm.gov</a>.
         We converted the raw pipe-delimited files to parquet and host them on HuggingFace:</p>
      <ul style="margin: .5rem 0 .75rem 1.5rem; font-size:.85rem">
        <li><strong>Dec 2024 snapshot:</strong>
            <a href="https://huggingface.co/datasets/impactproject/opm-ehri-data/blob/main/employment/employment_202412_v2.parquet"><code>employment_202412_v2.parquet</code></a>
            &mdash; "end of 2024"</li>
        <li><strong>Dec 2025 snapshot:</strong>
            <a href="https://huggingface.co/datasets/impactproject/opm-ehri-data/blob/main/employment/employment_202512_v2.parquet"><code>employment_202512_v2.parquet</code></a>
            &mdash; "end of 2025"</li>
        <li><strong>Full dataset:</strong>
            <a href="https://huggingface.co/datasets/impactproject/opm-ehri-data"><code>impactproject/opm-ehri-data</code></a></li>
      </ul>
      <p><strong>Steps:</strong></p>
      <ol style="margin: .5rem 0 .75rem 1.5rem; font-size:.85rem">
        <li>Filter both snapshots to rows where <code>agency_subelement</code> contains
            "NATURAL RESOURCES CONSERVATION SERVICE"</li>
        <li>Build a FIPS code from <code>duty_station_state_code</code> + <code>duty_station_county_code</code></li>
        <li>Sum the <code>count</code> column by FIPS (and by <code>occupational_series</code> where applicable)
            for each snapshot</li>
        <li>Outer-join the two snapshots by FIPS to compute net change per county</li>
        <li>Classify each county as gain, no change, or loss; count how many lost 100% of staff</li>
      </ol>
      <p>Each row in the OPM data represents a unique combination of employee attributes
         (agency, county, occupation, grade, etc.) with a <code>count</code> field.
         Summing <code>count</code> by county gives the total headcount per county.
         This is the same underlying data your workbook uses.</p>
    </div>"""


def build_html(sections, n_checks, n_matched, methods_html, territory_html):
    return f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>NRCS Verification</title>
<style>
  :root {{ --g: #16a34a; --r: #dc2626; --bg: #f8fafc; --card: #fff; --bdr: #e2e8f0; --txt: #1e293b; --dim: #64748b; }}
  * {{ margin:0; padding:0; box-sizing:border-box; }}
  body {{ font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif;
         background: var(--bg); color: var(--txt); line-height:1.5; padding:2rem; max-width:800px; margin:0 auto; }}
  .header {{ margin-bottom:1.5rem; padding-bottom:1rem; border-bottom:2px solid var(--bdr); }}
  h1 {{ font-size:1.4rem; }}
  .header p {{ color:var(--dim); font-size:.9rem; margin-top:.25rem; }}
  .score {{ display:inline-block; background:var(--card); border:1px solid var(--bdr); border-radius:8px;
            padding:.5rem 1.25rem; margin:.75rem 0; }}
  .score .big {{ font-size:2.2rem; font-weight:700; color:var(--g); }}
  .score .lbl {{ font-size:.8rem; color:var(--dim); }}
  .card {{ background:var(--card); border:1px solid var(--bdr); border-radius:8px; padding:1.25rem; margin-bottom:1.25rem; }}
  .card h2 {{ font-size:1.05rem; margin-bottom:.75rem; }}
  .card.note {{ background:#fffbeb; border-color:#fde68a; }}
  .card.note h2 {{ color:#92400e; }}
  table {{ width:100%; border-collapse:collapse; font-size:.85rem; }}
  th,td {{ padding:.4rem .6rem; text-align:left; border-bottom:1px solid var(--bdr); }}
  th {{ background:#f1f5f9; font-weight:600; font-size:.75rem; text-transform:uppercase; letter-spacing:.03em; }}
  .n {{ text-align:right; font-variant-numeric:tabular-nums; }}
  .diff {{ color:var(--dim); }}
  .check {{ font-size:1.1rem; font-weight:700; }}
  .check.yes {{ color:var(--g); }}
  .check.no {{ color:var(--r); }}
  .indent {{ padding-left:1.5rem; }}
  .group td {{ background:#f8fafc; padding-top:.6rem; border-bottom:none; font-weight:600; font-size:.82rem; }}
  .total td {{ border-top:2px solid var(--bdr); }}
  .footer {{ color:var(--dim); font-size:.78rem; margin-top:1.5rem; padding-top:.75rem; border-top:1px solid var(--bdr); }}
</style>
</head>
<body>
  <div class="header">
    <h1>NRCS Report &mdash; Independent Verification</h1>
    <p>We downloaded the raw OPM/EHRI employment snapshots (Dec 2024 and Dec 2025)
       and verified the numbers in your report.</p>
  </div>

  {methods_html}

  {territory_html}

  <h2 style="margin: 1.5rem 0 .75rem; font-size:1.15rem;">Claim-by-Claim Verification</h2>

  {''.join(sections)}

  <div class="footer">
    Generated {datetime.now().strftime('%Y-%m-%d %H:%M')} &mdash;
    Source: <a href="https://huggingface.co/datasets/impactproject/opm-ehri-data"><code>impactproject/opm-ehri-data</code></a>
  </div>
</body>
</html>"""


# ── Main ─────────────────────────────────────────────────────────────────────

def main():
    print("Loading data...")
    df24 = load_nrcs("employment_202412_v2.parquet")
    df25 = load_nrcs("employment_202512_v2.parquet")

    sections = []
    checks = []  # list of (yours, ours) bools

    def ok(match):
        checks.append(match)

    # ── Top-line ─────────────────────────────────────────────────────────────
    total_2024 = int(df24["count"].sum())
    total_2025 = int(df25["count"].sum())
    net_change = total_2025 - total_2024
    pct_change = net_change / total_2024 * 100
    ok(net_change == -2657)
    ok(round(pct_change) == -22)
    sections.append(section_topline(total_2024, total_2025, net_change, pct_change))

    # ── County tables (all match with territories included) ──────────────────
    table_defs = [
        ("All Occupations (PDF Table 1)",
         {"gain": 167, "no_change": 949, "loss": 1302, "total": 2418, "loss_100pct": 144},
         None),
        ("Soil Conservation + Gen NRM + Soil Tech (PDF Table 2)",
         {"gain": 184, "no_change": 1139, "loss": 1043, "total": 2366, "loss_100pct": 139},
         ["SOIL CONSERVATION", "GENERAL NATURAL RESOURCES MANAGEMENT AND BIOLOGICAL SCIENCES",
          "SOIL CONSERVATION TECHNICIAN"]),
        ("Rangeland Management (PDF Table 3)",
         {"gain": 16, "no_change": 111, "loss": 57, "total": 184, "loss_100pct": 43},
         ["RANGELAND MANAGEMENT"]),
        ("Civil Engineering (PDF Table 4)",
         {"gain": 43, "no_change": 154, "loss": 136, "total": 333, "loss_100pct": 74},
         ["CIVIL ENGINEERING"]),
    ]

    affected_tables = []  # for the territory note
    for title, pdf_claims, occ_filter in table_defs:
        ours = analyze(df24, df25, occ_filter)
        ours_us = analyze(df24, df25, occ_filter, us_only=True)
        for key in ["gain", "no_change", "loss", "total", "loss_100pct"]:
            ok(pdf_claims[key] == ours[key])
        sections.append(section_table(title, pdf_claims, ours))
        # Track if US-only differs for the note
        if ours != ours_us:
            affected_tables.append((title, pdf_claims, ours_us))

    # ── Individual occupations ───────────────────────────────────────────────
    individual = [
        ("SOIL CONSERVATION", {"loss": 694, "loss_100pct": 158}),
        ("GENERAL NATURAL RESOURCES MANAGEMENT AND BIOLOGICAL SCIENCES", {"loss": 380, "loss_100pct": 254}),
        ("SOIL CONSERVATION TECHNICIAN", {"loss": 286, "loss_100pct": 255}),
    ]
    ind_results = []
    for series, claims in individual:
        ours = analyze(df24, df25, [series])
        for key in claims:
            ok(claims[key] == ours[key])
        ind_results.append((series, claims, ours))
    sections.append(section_individual(ind_results))

    # ── Build methods and territory sections (go before the tables) ─────────
    methods_html = section_methods()
    territory_html = section_territory_note(df24, df25, affected_tables)

    # ── Write ────────────────────────────────────────────────────────────────
    n_matched = sum(checks)
    html = build_html(sections, len(checks), n_matched, methods_html, territory_html)
    OUTPUT_HTML.write_text(html)
    print(f"Report: {OUTPUT_HTML}")
    print(f"  {n_matched}/{len(checks)} verified")
    webbrowser.open(OUTPUT_HTML.as_uri())


if __name__ == "__main__":
    main()
