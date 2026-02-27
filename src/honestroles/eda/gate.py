from __future__ import annotations

from typing import Any, Mapping

from honestroles.eda.rules import EDARules


def evaluate_eda_gate(
    *,
    candidate_summary: Mapping[str, Any],
    rules: EDARules,
    diff_payload: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    severity_counts = _count_findings(candidate_summary)
    failures: list[dict[str, Any]] = []
    warnings: list[dict[str, Any]] = []

    for severity in rules.gate.fail_on:
        count = severity_counts.get(severity, 0)
        threshold = _severity_threshold(rules, severity)
        if count > threshold:
            failures.append(
                {
                    "type": "finding_count",
                    "severity": severity,
                    "count": count,
                    "threshold": threshold,
                    "detail": f"{severity} findings count {count} exceeds threshold {threshold}.",
                }
            )

    for severity in rules.gate.warn_on:
        if severity in rules.gate.fail_on:
            continue
        count = severity_counts.get(severity, 0)
        threshold = _severity_threshold(rules, severity)
        if count > 0:
            warnings.append(
                {
                    "type": "finding_count",
                    "severity": severity,
                    "count": count,
                    "threshold": threshold,
                    "detail": f"{severity} findings count is {count}.",
                }
            )

    if diff_payload is not None:
        for metric in diff_payload.get("drift", {}).get("metrics", []):
            status = str(metric.get("status", "pass"))
            if status == "fail":
                failures.append(
                    {
                        "type": "drift",
                        "kind": metric.get("kind"),
                        "column": metric.get("column"),
                        "metric": metric.get("metric"),
                        "value": metric.get("value"),
                        "threshold": metric.get("fail_threshold"),
                        "detail": (
                            f"drift metric {metric.get('metric')} for {metric.get('column')}="
                            f"{metric.get('value')} exceeds fail threshold "
                            f"{metric.get('fail_threshold')}."
                        ),
                    }
                )
            elif status == "warn":
                warnings.append(
                    {
                        "type": "drift",
                        "kind": metric.get("kind"),
                        "column": metric.get("column"),
                        "metric": metric.get("metric"),
                        "value": metric.get("value"),
                        "threshold": metric.get("warn_threshold"),
                        "detail": (
                            f"drift metric {metric.get('metric')} for {metric.get('column')}="
                            f"{metric.get('value')} exceeds warn threshold "
                            f"{metric.get('warn_threshold')}."
                        ),
                    }
                )

    return {
        "status": "fail" if failures else "pass",
        "severity_counts": severity_counts,
        "failures": failures,
        "warnings": warnings,
        "evaluated_rules": rules.to_dict(),
    }


def _count_findings(candidate_summary: Mapping[str, Any]) -> dict[str, int]:
    counts: dict[str, int] = {"P0": 0, "P1": 0, "P2": 0}
    for finding in candidate_summary.get("findings", []):
        severity = str(finding.get("severity", "")).upper()
        if severity in counts:
            counts[severity] += 1
    for finding in candidate_summary.get("findings_by_source", []):
        severity = str(finding.get("severity", "")).upper()
        if severity in counts:
            counts[severity] += 1
    return counts


def _severity_threshold(rules: EDARules, severity: str) -> int:
    if severity == "P0":
        return rules.gate.max_p0
    if severity == "P1":
        return rules.gate.max_p1
    return 999999
