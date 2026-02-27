from __future__ import annotations

from typing import Any, Mapping

from .common import round4

_SEVERITY_RANK = {"P0": 0, "P1": 1, "P2": 2}


def build_findings(summary: Mapping[str, Any]) -> list[dict[str, Any]]:
    findings: list[dict[str, Any]] = []

    score_percent = float(summary["quality"]["score_percent"])
    if score_percent < 90.0:
        findings.append(
            {
                "severity": "P1",
                "title": "Low weighted quality score",
                "detail": f"score_percent={round4(score_percent)} is below 90.",
                "recommendation": "Prioritize high-weight fields with high null rates.",
            }
        )
    elif score_percent < 95.0:
        findings.append(
            {
                "severity": "P2",
                "title": "Quality score below target",
                "detail": f"score_percent={round4(score_percent)} is below 95.",
                "recommendation": "Review top weighted-null columns and adjust extraction.",
            }
        )

    salary_inversion_count = int(summary["consistency"]["salary_min_gt_salary_max"]["count"])
    if salary_inversion_count > 0:
        findings.append(
            {
                "severity": "P0",
                "title": "Invalid salary ranges detected",
                "detail": f"{salary_inversion_count} rows have salary_min > salary_max.",
                "recommendation": "Normalize salary parsing and enforce min<=max before scoring.",
            }
        )

    title_eq_pct = float(summary["consistency"]["title_equals_company"]["pct"])
    if title_eq_pct >= 5.0:
        findings.append(
            {
                "severity": "P1",
                "title": "Title appears to contain company names",
                "detail": f"title_equals_company={round4(title_eq_pct)}%.",
                "recommendation": "Add source-specific title cleanup or fallback title extraction.",
            }
        )

    posted_at_pct = float(
        summary["completeness"]["key_fields_runtime"].get("posted_at", {}).get(
            "non_null_pct", 0.0
        )
    )
    if posted_at_pct < 80.0:
        findings.append(
            {
                "severity": "P1",
                "title": "Low posted_at coverage",
                "detail": f"posted_at non-null coverage is {round4(posted_at_pct)}%.",
                "recommendation": "Improve date extraction or add alias/transform for source fields.",
            }
        )

    unknown_location_pct = 0.0
    for item in summary["distributions"].get("top_locations_runtime", []):
        if str(item.get("location", "")).strip().lower() == "unknown":
            unknown_location_pct = float(item.get("pct", 0.0))
            break
    if unknown_location_pct >= 30.0:
        findings.append(
            {
                "severity": "P1",
                "title": "Large unknown location share",
                "detail": f"unknown location rows account for {round4(unknown_location_pct)}%.",
                "recommendation": "Expand location extraction and source alias mapping coverage.",
            }
        )

    return sorted(
        findings,
        key=lambda item: (_SEVERITY_RANK.get(item["severity"], 99), item["title"]),
    )


def build_source_findings(summary: Mapping[str, Any]) -> list[dict[str, Any]]:
    findings: list[dict[str, Any]] = []
    quality_rows = {
        str(item["source"]): item for item in summary.get("quality", {}).get("by_source", [])
    }
    consistency_rows = {
        str(item["source"]): item
        for item in summary.get("consistency", {}).get("by_source", [])
    }

    all_sources = sorted(set(quality_rows.keys()) | set(consistency_rows.keys()))
    for source in all_sources:
        quality_row = quality_rows.get(source, {})
        consistency_row = consistency_rows.get(source, {})
        score = float(quality_row.get("score_proxy", 100.0))
        if score < 90.0:
            findings.append(
                {
                    "severity": "P1",
                    "source": source,
                    "metric": "quality.score_proxy",
                    "title": "Low source quality score",
                    "detail": f"source={source} score_proxy={round4(score)} below 90.",
                    "recommendation": "Prioritize missing high-weight fields for this source.",
                }
            )
        elif score < 95.0:
            findings.append(
                {
                    "severity": "P2",
                    "source": source,
                    "metric": "quality.score_proxy",
                    "title": "Source quality below target",
                    "detail": f"source={source} score_proxy={round4(score)} below 95.",
                    "recommendation": "Review weighted nulls and extraction quality for this source.",
                }
            )

        salary_pct = float(consistency_row.get("salary_min_gt_salary_max_pct", 0.0))
        salary_count = int(consistency_row.get("salary_min_gt_salary_max_count", 0))
        if salary_count > 0:
            findings.append(
                {
                    "severity": "P0",
                    "source": source,
                    "metric": "consistency.salary_min_gt_salary_max",
                    "title": "Invalid salary ranges in source",
                    "detail": (
                        f"source={source} has {salary_count} rows with salary_min > salary_max "
                        f"({salary_pct}%)."
                    ),
                    "recommendation": "Fix salary parsing and normalization for this source.",
                }
            )

        title_pct = float(consistency_row.get("title_equals_company_pct", 0.0))
        if title_pct >= 5.0:
            findings.append(
                {
                    "severity": "P1",
                    "source": source,
                    "metric": "consistency.title_equals_company",
                    "title": "Title/company contamination in source",
                    "detail": f"source={source} has title_equals_company={round4(title_pct)}%.",
                    "recommendation": "Apply source-specific title cleanup rules.",
                }
            )

    return sorted(
        findings,
        key=lambda item: (
            _SEVERITY_RANK.get(item["severity"], 99),
            str(item.get("source", "")),
            item["title"],
        ),
    )
