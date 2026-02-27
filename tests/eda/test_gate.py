from __future__ import annotations

from honestroles.eda.gate import _count_findings, evaluate_eda_gate
from honestroles.eda.rules import load_eda_rules


def test_gate_fails_on_p0() -> None:
    summary = {
        "findings": [{"severity": "P0"}],
        "findings_by_source": [],
    }
    payload = evaluate_eda_gate(candidate_summary=summary, rules=load_eda_rules())
    assert payload["status"] == "fail"
    assert payload["failures"]


def test_gate_warns_on_p1_by_default() -> None:
    summary = {
        "findings": [{"severity": "P1"}],
        "findings_by_source": [],
    }
    payload = evaluate_eda_gate(candidate_summary=summary, rules=load_eda_rules())
    assert payload["status"] == "pass"
    assert payload["warnings"]


def test_gate_fails_on_drift_metric() -> None:
    summary = {
        "findings": [],
        "findings_by_source": [],
    }
    diff_payload = {
        "drift": {
            "metrics": [
                {
                    "kind": "numeric",
                    "column": "salary_min",
                    "metric": "psi",
                    "value": 0.3,
                    "warn_threshold": 0.1,
                    "fail_threshold": 0.25,
                    "status": "fail",
                }
            ]
        }
    }
    payload = evaluate_eda_gate(
        candidate_summary=summary,
        rules=load_eda_rules(),
        diff_payload=diff_payload,
    )
    assert payload["status"] == "fail"
    assert any(item["type"] == "drift" for item in payload["failures"])


def test_gate_warn_path_for_drift_and_fail_warn_overlap(tmp_path) -> None:
    rules_file = tmp_path / "rules.toml"
    rules_file.write_text(
        """
[gate]
fail_on = ["P0", "P1"]
warn_on = ["P1", "P2"]
""".strip(),
        encoding="utf-8",
    )
    rules = load_eda_rules(rules_file=rules_file)
    summary = {
        "findings": [{"severity": "P1"}, {"severity": "P2"}],
        "findings_by_source": [{"severity": "P2"}],
    }
    diff_payload = {
        "drift": {
            "metrics": [
                {
                    "kind": "categorical",
                    "column": "source",
                    "metric": "jsd",
                    "value": 0.12,
                    "warn_threshold": 0.1,
                    "status": "warn",
                }
            ]
        }
    }
    payload = evaluate_eda_gate(candidate_summary=summary, rules=rules, diff_payload=diff_payload)
    assert payload["status"] == "pass"
    assert any(item["type"] == "drift" for item in payload["warnings"])
    assert payload["severity_counts"]["P2"] == 2


def test_gate_count_findings_ignores_unknown_severity() -> None:
    counts = _count_findings(
        {"findings": [{"severity": "p9"}], "findings_by_source": [{"severity": "p2"}, {"severity": "p9"}]}
    )
    assert counts["P2"] == 1
