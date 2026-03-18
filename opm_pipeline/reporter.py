"""Generate markdown reports and create GitHub Issues."""

from __future__ import annotations

import subprocess
from datetime import date


def _fmt(n: int) -> str:
    """Format integer with commas."""
    return f"{n:,}"


def _sign(n: int | float) -> str:
    """Format with explicit +/- sign."""
    return f"+{n:,}" if n >= 0 else f"{n:,}"


def _render_diff(key: str, diff: dict, lines: list):
    """Render a diff section for one file."""
    compared_to = diff.get("compared_to")
    if compared_to:
        lines.append(f"*Compared to: `{compared_to}`*")
        lines.append("")

    # Row count
    rc = diff.get("row_counts", {})
    if rc:
        lines.append(f"**Rows:** {_fmt(rc['old'])} → {_fmt(rc['new'])} ({_sign(rc['diff'])}, {_sign(rc['pct_change'])}%)")
        lines.append("")

    # Schema changes
    schema = diff.get("schema", {})
    new_col_summaries = diff.get("new_col_summaries", {})
    if schema.get("added"):
        lines.append(f"**New columns:** {', '.join(f'`{c}`' for c in schema['added'])}")
        for col in schema["added"]:
            if col in new_col_summaries:
                top = ", ".join(f"`{v}` ({_fmt(n)})" for v, n in new_col_summaries[col][:5])
                lines.append(f"  - `{col}` top values: {top}")
        lines.append("")
    if schema.get("removed"):
        lines.append(f"**Removed columns:** {', '.join(f'`{c}`' for c in schema['removed'])}")
        lines.append("")

    # Top 5 columns by change magnitude — gains and losses
    vc = diff.get("value_counts", {})
    if vc:
        top_cols = list(vc.items())[:5]
        for col, info in top_cols:
            changes = info["value_changes"]
            gains = [e for e in changes if e["diff"] > 0][:5]
            losses = [e for e in changes if e["diff"] < 0][:5]
            n_new = len(info["new_values"])
            n_removed = len(info["removed_values"])

            summary_parts = [f"{info['total_values_changed']} values changed"]
            if n_new:
                summary_parts.append(f"{n_new} new")
            if n_removed:
                summary_parts.append(f"{n_removed} removed")

            lines.append(f"<details><summary><code>{col}</code> — {', '.join(summary_parts)}</summary>")
            lines.append("")
            lines.append("| Biggest gains | | Biggest losses | |")
            lines.append("|---|---|---|---|")
            for i in range(max(len(gains), len(losses))):
                g = gains[i] if i < len(gains) else None
                l = losses[i] if i < len(losses) else None
                g_str = f"`{g['value']}` | {_sign(g['diff'])}" if g else " | "
                l_str = f"`{l['value']}` | {_sign(l['diff'])}" if l else " | "
                lines.append(f"| {g_str} | {l_str} |")
            lines.append("")
            lines.append("</details>")
            lines.append("")

        if len(vc) > 5:
            lines.append(f"*{len(vc) - 5} more columns with changes not shown.*")
            lines.append("")

    if not vc and not schema.get("added") and not schema.get("removed") and not rc:
        lines.append("No significant changes detected.")
        lines.append("")


def _render_new_summary(key: str, summary: dict, lines: list):
    """Render a summary for a truly new file (no prior data to compare against)."""
    if not summary:
        lines.append("No summary available.")
        lines.append("")
        return

    lines.append(f"*First file of this type — no prior data to compare against.*")
    lines.append("")
    lines.append(f"**{_fmt(summary.get('row_count', 0))} rows** across **{len(summary.get('columns', []))} columns**")
    lines.append("")

    col_summaries = summary.get("column_summaries", {})
    if col_summaries:
        # Show a few highlights inline
        for col, info in list(col_summaries.items())[:5]:
            top3 = ", ".join(f"`{v}` ({_fmt(c)})" for v, c in info["top_values"][:3])
            lines.append(f"- **{col}** ({_fmt(info['unique_count'])} values): {top3}")
        lines.append("")

        # Full table collapsed
        lines.append(f"<details><summary>All {len(col_summaries)} categorical columns</summary>")
        lines.append("")
        lines.append("| Column | Unique Values | Top 3 |")
        lines.append("|--------|--------------|-------|")
        for col, info in col_summaries.items():
            top3 = ", ".join(f"`{v}` ({_fmt(c)})" for v, c in info["top_values"][:3])
            lines.append(f"| `{col}` | {_fmt(info['unique_count'])} | {top3} |")
        lines.append("")
        lines.append("</details>")
        lines.append("")


def _is_version_update(hf_path: str) -> bool:
    """True if this is a new version of an existing month (v2+), not a brand-new month."""
    import re
    m = re.search(r'_v(\d+)\.parquet$', hf_path)
    return bool(m and int(m.group(1)) > 1)


def generate_report(changes: dict, diffs: dict, new_summaries: dict, run_date: date = None) -> str:
    """Generate a markdown report of pipeline changes.

    Uses GitHub-flavored markdown with <details> for expandable sections.
    """
    if run_date is None:
        run_date = date.today()

    lines = []

    # Split "new" into new months (v1) vs new versions (v2+)
    new_months = [k for k in changes["new"] if not _is_version_update(k)]
    new_versions = [k for k in changes["new"] if _is_version_update(k)]

    # --- Summary ---
    lines.append(f"## EHRI Data Pipeline Report - {run_date.isoformat()}")
    lines.append("")

    parts = []
    if new_months:
        parts.append(f"**{len(new_months)} new month{'s' if len(new_months) > 1 else ''}**")
    if new_versions:
        parts.append(f"**{len(new_versions)} updated version{'s' if len(new_versions) > 1 else ''}**")
    if changes["updated"]:
        parts.append(f"**{len(changes['updated'])} updated**")
    if parts:
        lines.append(f"{', '.join(parts)} processed.")
        lines.append("")

    if new_months:
        lines.append(f"**New months:** {', '.join(f'`{k}`' for k in new_months)}")
        lines.append("")
    if new_versions:
        lines.append(f"**New versions:** {', '.join(f'`{k}`' for k in new_versions)}")
        lines.append("")
    if changes["updated"]:
        lines.append(f"**Updated:** {', '.join(f'`{k}`' for k in changes['updated'])}")
        lines.append("")

    lines.append("---")
    lines.append("")

    # --- Each file — cap per-file budget so large batches still fit in 65k ---
    all_keys = changes["new"] + changes["updated"]
    char_budget = 60000 // max(len(all_keys), 1)

    for key in all_keys:
        lines.append(f"### `{key}`")
        lines.append("")

        file_lines: list[str] = []
        if key in diffs:
            _render_diff(key, diffs[key], file_lines)
        elif key in new_summaries:
            _render_new_summary(key, new_summaries[key], file_lines)
        else:
            file_lines.append("No data available.")
            file_lines.append("")

        section = "\n".join(file_lines)
        if len(section) > char_budget:
            section = section[:char_budget] + f"\n\n*(section truncated)*"
        lines.append(section)

    return "\n".join(lines)


def _top_proportional_changes(vc: dict, n: int = 4, min_old: int = 100) -> list:
    """Return top N value changes by proportional shift, deduplicating code/label column pairs."""
    label_cols = {c for c in vc if not c.endswith("_code")}
    skip = {c for c in vc if c.endswith("_code") and c[:-5] in label_cols}
    date_keywords = ("date", "yyyymm", "yyyyq", "year", "month")

    candidates = []
    for col, info in vc.items():
        if col in skip:
            continue
        if any(kw in col.lower() for kw in date_keywords):
            continue
        for v in info["value_changes"]:
            if v["old_count"] < min_old:
                continue
            prop = v["diff"] / v["old_count"] * 100
            candidates.append((abs(prop), prop, col, v))

    candidates.sort(reverse=True)
    return candidates[:n]


def generate_email_html(changes: dict, diffs: dict, new_summaries: dict, run_date: date = None) -> str:
    """Generate HTML email body matching the send_jan_dec_email.py format."""
    if run_date is None:
        run_date = date.today()

    import re as _re
    from .config import hf_path_to_date
    all_keys = changes["new"] + changes["updated"]
    data_dates = sorted({hf_path_to_date(k) for k in all_keys if hf_path_to_date(k)})
    if len(data_dates) == 1:
        heading_month = data_dates[0].strftime("%B %Y")
    elif len(data_dates) > 1:
        heading_month = " & ".join(d.strftime("%B %Y") for d in data_dates)
    else:
        heading_month = run_date.strftime("%B %Y")
    parts = [f"<h2>EHRI Data Update: {heading_month}</h2>"]

    for key in changes["new"] + changes["updated"]:
        dtype = key.split("/")[0].capitalize()
        d = hf_path_to_date(key)
        month_str = d.strftime("%B %Y") if d else ""
        ver_m = _re.search(r'_v(\d+)\.parquet$', key)
        ver_str = f" v{ver_m.group(1)}" if ver_m else ""
        parts.append(f"<h3>{dtype} — {month_str}{ver_str}</h3>")

        if key in diffs:
            diff = diffs[key]
            compared_to = diff.get("compared_to")
            if compared_to:
                from .config import hf_path_to_card_stem
                human = hf_path_to_card_stem(compared_to) or compared_to
                filename = compared_to.split("/")[-1]
                parts.append(f"<p><em>Compared to: {human} ({filename})</em></p>")
            rc = diff.get("row_counts", {})
            if rc:
                pct = rc['pct_change']
                pct_str = f"+{pct:.2f}%" if pct >= 0 else f"{pct:.2f}%"
                parts.append(
                    f"<p><strong>Total records:</strong> {_fmt(rc['old'])} → {_fmt(rc['new'])} "
                    f"({_sign(rc['diff'])}, {pct_str})</p>"
                )
            schema = diff.get("schema", {})
            if schema.get("added"):
                cols = ", ".join(f"<code>{c}</code>" for c in schema["added"])
                parts.append(f"<p><strong>New columns:</strong> {cols}</p>")
            if schema.get("removed"):
                cols = ", ".join(f"<code>{c}</code>" for c in schema["removed"])
                parts.append(f"<p><strong>Removed columns:</strong> {cols}</p>")
            top = _top_proportional_changes(diff.get("value_counts", {}))
            if top:
                parts.append("<ul>")
                for _, prop, col, v in top:
                    prop_str = f"+{prop:.0f}%" if prop >= 0 else f"{prop:.0f}%"
                    parts.append(
                        f"<li><strong>{col} — {v['value']}</strong>: "
                        f"{prop_str} ({_fmt(v['old_count'])} → {_fmt(v['new_count'])})</li>"
                    )
                parts.append("</ul>")
        elif key in new_summaries:
            s = new_summaries[key]
            parts.append(f"<p>New file — {_fmt(s.get('row_count', 0))} rows.</p>")

    return "\n".join(parts)


def create_github_issue(title: str, body: str) -> str | None:
    """Create a GitHub issue using `gh` CLI. Returns issue URL or None on failure."""
    # GitHub issue body limit is 65536 chars
    if len(body) > 65000:
        body = body[:65000] + "\n\n*(report truncated — see Actions log for full output)*"
    try:
        result = subprocess.run(
            ["gh", "issue", "create", "--title", title, "--body-file", "-"],
            input=body, capture_output=True, text=True, check=True,
        )
        url = result.stdout.strip()
        print(f"Created issue: {url}")
        return url
    except (FileNotFoundError, OSError) as e:
        print(f"Warning: `gh` CLI not available: {e}. Skipping issue creation.")
        return None
    except subprocess.CalledProcessError as e:
        print(f"Warning: Failed to create issue: {e.stderr}")
        return None
