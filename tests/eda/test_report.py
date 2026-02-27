from __future__ import annotations

from honestroles.eda.report import render_report_markdown


def test_render_report_markdown_orders_findings_by_severity() -> None:
    summary = {
        "shape": {
            "input_path": "/tmp/jobs.parquet",
            "raw": {"rows": 100, "columns": 10},
            "runtime": {"rows": 100, "columns": 12},
        },
        "quality": {
            "profile": "core_fields_weighted",
            "score_percent": 93.5,
            "weighted_null_percent": 6.5,
        },
        "consistency": {
            "salary_min_gt_salary_max": {"count": 2, "pct": 2.0},
            "title_equals_company": {"count": 12, "pct": 12.0},
        },
        "temporal": {
            "posted_at_range": {"min": "2025-01-01", "max": "2025-02-01"},
            "monthly_counts": [{"month": "2025-01", "count": 50}],
        },
        "findings": [
            {
                "severity": "P2",
                "title": "Later",
                "detail": "later detail",
                "recommendation": "later rec",
            },
            {
                "severity": "P0",
                "title": "First",
                "detail": "first detail",
                "recommendation": "first rec",
            },
        ],
        "findings_by_source": [
            {
                "severity": "P1",
                "source": "lever",
                "title": "Source issue",
                "detail": "source detail",
                "recommendation": "source rec",
            }
        ],
    }

    report = render_report_markdown(summary)
    assert "# HonestRoles EDA Report" in report
    assert report.index("**P0**") < report.index("**P2**")
    assert "## Source Findings" in report
