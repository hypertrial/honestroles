from __future__ import annotations

from honestroles.eda.gate import evaluate_eda_gate
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
