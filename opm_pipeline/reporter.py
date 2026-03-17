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
        lines.append(f"**Rows:** {_fmt(rc['old'])} -> {_fmt(rc['new'])} ({_sign(rc['diff'])}, {_sign(rc['pct_change'])}%)")
        lines.append("")

    # Schema changes
    schema = diff.get("schema", {})
    if schema.get("added") or schema.get("removed"):
        lines.append("**Schema changes:**")
        if schema.get("added"):
            lines.append(f"- Columns added: {', '.join(f'`{c}`' for c in schema['added'])}")
        if schema.get("removed"):
            lines.append(f"- Columns removed: {', '.join(f'`{c}`' for c in schema['removed'])}")
        lines.append("")

    # Value count changes
    vc = diff.get("value_counts", {})
    if vc:
        # Overview table
        lines.append("| Column | Values Changed | New Values | Removed Values | Largest Shift |")
        lines.append("|--------|---------------|------------|----------------|---------------|")
        for col, info in vc.items():
            n_changed = info["total_values_changed"]
            n_new = len(info["new_values"])
            n_removed = len(info["removed_values"])
            biggest = info["value_changes"][0] if info["value_changes"] else None
            biggest_str = f"`{biggest['value']}` ({_sign(biggest['diff'])})" if biggest else "-"
            lines.append(f"| `{col}` | {n_changed} | {n_new} | {n_removed} | {biggest_str} |")
        lines.append("")

        # Expandable detail per column
        for col, info in vc.items():
            changes_list = info["value_changes"]
            lines.append(f"<details><summary><code>{col}</code> - {info['total_values_changed']} values changed</summary>")
            lines.append("")

            if info["new_values"]:
                lines.append(f"**New values:** {', '.join(f'`{v}`' for v in info['new_values'][:20])}")
                lines.append("")

            if info["removed_values"]:
                lines.append(f"**Removed values:** {', '.join(f'`{v}`' for v in info['removed_values'][:20])}")
                lines.append("")

            lines.append("| Value | Old Count | New Count | Change |")
            lines.append("|-------|-----------|-----------|--------|")
            for entry in changes_list:
                lines.append(
                    f"| `{entry['value']}` "
                    f"| {_fmt(entry['old_count'])} "
                    f"| {_fmt(entry['new_count'])} "
                    f"| {_sign(entry['diff'])} |"
                )
            lines.append("")
            lines.append("</details>")
            lines.append("")

    if not vc and not (schema.get("added") or schema.get("removed")) and not rc:
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


def generate_report(changes: dict, diffs: dict, new_summaries: dict, run_date: date = None) -> str:
    """Generate a markdown report of pipeline changes.

    Uses GitHub-flavored markdown with <details> for expandable sections.
    """
    if run_date is None:
        run_date = date.today()

    lines = []

    # --- Summary ---
    lines.append(f"## OPM Data Pipeline Report - {run_date.isoformat()}")
    lines.append("")

    if changes["new"] and changes["updated"]:
        lines.append(f"**{len(changes['new'])} new** and **{len(changes['updated'])} updated** files processed.")
    elif changes["new"]:
        lines.append(f"**{len(changes['new'])} new** files processed.")
    elif changes["updated"]:
        lines.append(f"**{len(changes['updated'])} updated** files processed.")
    lines.append("")

    if changes["new"]:
        lines.append(f"**New:** {', '.join(f'`{k}`' for k in changes['new'])}")
        lines.append("")
    if changes["updated"]:
        lines.append(f"**Updated:** {', '.join(f'`{k}`' for k in changes['updated'])}")
        lines.append("")

    lines.append("---")
    lines.append("")

    # --- Each file ---
    all_keys = changes["new"] + changes["updated"]
    for key in all_keys:
        lines.append(f"### `{key}`")
        lines.append("")

        if key in diffs:
            _render_diff(key, diffs[key], lines)
        elif key in new_summaries:
            _render_new_summary(key, new_summaries[key], lines)
        else:
            lines.append("No data available.")
            lines.append("")

    return "\n".join(lines)


def create_github_issue(title: str, body: str) -> str | None:
    """Create a GitHub issue using `gh` CLI. Returns issue URL or None on failure."""
    try:
        result = subprocess.run(
            ["gh", "issue", "create", "--title", title, "--body", body],
            capture_output=True, text=True, check=True,
        )
        url = result.stdout.strip()
        print(f"Created issue: {url}")
        return url
    except FileNotFoundError:
        print("Warning: `gh` CLI not found. Skipping issue creation.")
        return None
    except subprocess.CalledProcessError as e:
        print(f"Warning: Failed to create issue: {e.stderr}")
        return None
