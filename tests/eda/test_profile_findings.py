from __future__ import annotations

from honestroles.eda.profile_findings import build_findings, build_source_findings


def test_build_findings_covers_p2_branch() -> None:
    summary = {
        "quality": {"score_percent": 94.0},
        "consistency": {
            "salary_min_gt_salary_max": {"count": 0},
            "title_equals_company": {"pct": 0.0},
        },
        "completeness": {"key_fields_runtime": {"posted_at": {"non_null_pct": 90.0}}},
        "distributions": {"top_locations_runtime": []},
    }
    findings = build_findings(summary)
    assert findings
    assert findings[0]["severity"] == "P2"


def test_build_findings_covers_multiple_p1_paths() -> None:
    summary = {
        "quality": {"score_percent": 80.0},
        "consistency": {
            "salary_min_gt_salary_max": {"count": 1},
            "title_equals_company": {"pct": 10.0},
        },
        "completeness": {"key_fields_runtime": {"posted_at": {"non_null_pct": 70.0}}},
        "distributions": {
            "top_locations_runtime": [{"location": "Unknown", "pct": 50.0}],
        },
    }
    findings = build_findings(summary)
    severities = [item["severity"] for item in findings]
    assert "P0" in severities
    assert severities.count("P1") >= 3


def test_build_source_findings_p2_and_p1_and_p0() -> None:
    summary = {
        "quality": {
            "by_source": [
                {"source": "a", "score_proxy": 94.0},
                {"source": "b", "score_proxy": 80.0},
            ]
        },
        "consistency": {
            "by_source": [
                {
                    "source": "a",
                    "salary_min_gt_salary_max_count": 0,
                    "salary_min_gt_salary_max_pct": 0.0,
                    "title_equals_company_pct": 6.0,
                },
                {
                    "source": "b",
                    "salary_min_gt_salary_max_count": 2,
                    "salary_min_gt_salary_max_pct": 10.0,
                    "title_equals_company_pct": 0.0,
                },
            ]
        },
    }
    findings = build_source_findings(summary)
    assert any(item["severity"] == "P2" for item in findings)
    assert any(item["severity"] == "P1" for item in findings)
    assert any(item["severity"] == "P0" for item in findings)
