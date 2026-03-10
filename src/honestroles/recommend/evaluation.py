from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from honestroles.errors import ConfigValidationError

from .matching import match_jobs_with_profile
from .models import CandidateProfile, RelevanceEvaluationResult, SCHEMA_VERSION
from .parser import parse_candidate_profile_payload
from .policy import load_eval_thresholds


def evaluate_relevance(
    *,
    index_dir: str | Path,
    golden_set: str | Path,
    thresholds_file: str | Path | None = None,
    policy_file: str | Path | None = None,
) -> RelevanceEvaluationResult:
    thresholds, thresholds_source, thresholds_hash = load_eval_thresholds(thresholds_file)
    golden_path = Path(golden_set).expanduser().resolve()
    try:
        payload = json.loads(golden_path.read_text(encoding="utf-8"))
    except OSError as exc:
        raise ConfigValidationError(f"cannot read golden set '{golden_path}': {exc}") from exc
    except json.JSONDecodeError as exc:
        raise ConfigValidationError(f"invalid golden set JSON '{golden_path}': {exc}") from exc
    if not isinstance(payload, dict):
        raise ConfigValidationError("golden set root must be an object")

    cases = payload.get("cases")
    if not isinstance(cases, list):
        raise ConfigValidationError("golden set must include cases[]")

    ks = tuple(sorted(set(thresholds.ks)))
    if not ks:
        raise ConfigValidationError("evaluation ks must be non-empty")

    precision_sum = {k: 0.0 for k in ks}
    recall_sum = {k: 0.0 for k in ks}
    case_count = 0

    for case in cases:
        if not isinstance(case, dict):
            continue
        candidate = _parse_case_candidate(case)
        relevant_ids = {
            str(item).strip()
            for item in case.get("relevant_job_ids", [])
            if str(item).strip()
        }
        if not relevant_ids:
            continue

        result = match_jobs_with_profile(
            index_dir=index_dir,
            candidate=candidate,
            top_k=max(ks),
            policy_file=policy_file,
            include_excluded=False,
        )

        predicted = [item.job_id for item in result.results]
        case_count += 1
        for k in ks:
            top_ids = predicted[:k]
            hits = len([job_id for job_id in top_ids if job_id in relevant_ids])
            precision_sum[k] += hits / float(k)
            recall_sum[k] += hits / float(len(relevant_ids))

    if case_count == 0:
        raise ConfigValidationError("golden set has no valid cases with relevant_job_ids")

    metrics: dict[str, float] = {}
    for k in ks:
        metrics[f"precision_at_{k}"] = round(precision_sum[k] / float(case_count), 6)
        metrics[f"recall_at_{k}"] = round(recall_sum[k] / float(case_count), 6)

    threshold_payload = {
        "precision_at_10_min": thresholds.precision_at_10_min,
        "recall_at_25_min": thresholds.recall_at_25_min,
    }

    failing_checks: list[str] = []
    check_codes: list[str] = []
    if metrics.get("precision_at_10", 0.0) < thresholds.precision_at_10_min:
        failing_checks.append("precision_at_10")
        check_codes.append("EVAL_PRECISION_AT_10_BELOW_THRESHOLD")
    if metrics.get("recall_at_25", 0.0) < thresholds.recall_at_25_min:
        failing_checks.append("recall_at_25")
        check_codes.append("EVAL_RECALL_AT_25_BELOW_THRESHOLD")

    status = "pass" if not failing_checks else "fail"
    return RelevanceEvaluationResult(
        schema_version=SCHEMA_VERSION,
        status=status,
        index_dir=str(Path(index_dir).expanduser().resolve()),
        thresholds_source=thresholds_source,
        thresholds_hash=thresholds_hash,
        metrics=metrics,
        thresholds=threshold_payload,
        cases_evaluated=case_count,
        failing_checks=tuple(failing_checks),
        check_codes=tuple(check_codes),
    )


def _parse_case_candidate(case: dict[str, Any]) -> CandidateProfile:
    payload = case.get("candidate")
    if not isinstance(payload, dict):
        raise ConfigValidationError("each golden set case must include candidate object")
    return parse_candidate_profile_payload(payload)
