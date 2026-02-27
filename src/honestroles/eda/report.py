from __future__ import annotations

from typing import Any, Mapping

_SEVERITY_RANK = {"P0": 0, "P1": 1, "P2": 2}


def render_report_markdown(summary: Mapping[str, Any]) -> str:
    shape = summary["shape"]
    quality = summary["quality"]
    consistency = summary["consistency"]
    temporal = summary["temporal"]

    lines = [
        "# HonestRoles EDA Report",
        "",
        "## Dataset Snapshot",
        f"- Input path: `{shape['input_path']}`",
        f"- Raw rows/columns: `{shape['raw']['rows']}` / `{shape['raw']['columns']}`",
        (
            "- Runtime rows/columns: "
            f"`{shape['runtime']['rows']}` / `{shape['runtime']['columns']}`"
        ),
        "",
        "## Quality KPIs",
        f"- Profile: `{quality['profile']}`",
        f"- Score percent: `{quality['score_percent']}`",
        f"- Weighted null percent: `{quality['weighted_null_percent']}`",
        "",
        "## Consistency Checks",
        (
            "- salary_min > salary_max: "
            f"`{consistency['salary_min_gt_salary_max']['count']}` "
            f"({consistency['salary_min_gt_salary_max']['pct']}%)"
        ),
        (
            "- title == company: "
            f"`{consistency['title_equals_company']['count']}` "
            f"({consistency['title_equals_company']['pct']}%)"
        ),
        "",
        "## Temporal Coverage",
        f"- posted_at min: `{temporal['posted_at_range']['min']}`",
        f"- posted_at max: `{temporal['posted_at_range']['max']}`",
        f"- Monthly buckets: `{len(temporal['monthly_counts'])}`",
        "",
        "## Findings",
    ]

    findings = sorted(
        list(summary.get("findings", [])),
        key=lambda item: (_SEVERITY_RANK.get(item.get("severity", "P2"), 99), item.get("title", "")),
    )

    if not findings:
        lines.append("- No prioritized findings.")
    else:
        for finding in findings:
            lines.append(
                f"- **{finding['severity']}** `{finding['title']}`: {finding['detail']}"
            )
            lines.append(f"  - Recommendation: {finding['recommendation']}")

    lines.extend(["", "## Source Findings"])
    source_findings = sorted(
        list(summary.get("findings_by_source", [])),
        key=lambda item: (
            _SEVERITY_RANK.get(item.get("severity", "P2"), 99),
            item.get("source", ""),
            item.get("title", ""),
        ),
    )
    if not source_findings:
        lines.append("- No source-attributed findings.")
    else:
        for finding in source_findings:
            lines.append(
                f"- **{finding['severity']}** source=`{finding['source']}` "
                f"`{finding['title']}`: {finding['detail']}"
            )
            lines.append(f"  - Recommendation: {finding['recommendation']}")

    lines.extend(
        [
            "",
            "## Next Steps",
            "1. Address P0/P1 findings before broader downstream modeling.",
            "2. Re-run `honestroles eda generate` after fixes and compare artifacts.",
            "3. Use `honestroles eda dashboard` for interactive review of the generated artifacts.",
            "",
        ]
    )

    return "\n".join(lines)


def write_report_markdown(summary: Mapping[str, Any], output_path) -> None:
    output_path.write_text(render_report_markdown(summary), encoding="utf-8")
