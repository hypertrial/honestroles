from __future__ import annotations

import hashlib
import json
from pathlib import Path
import tomllib
from typing import Any

from honestroles.errors import ConfigValidationError

from .models import EvalThresholds, RecommendationPolicy


def load_recommendation_policy(path: str | Path | None) -> tuple[RecommendationPolicy, str, str]:
    if path in (None, ""):
        policy = RecommendationPolicy()
        return policy, "builtin", _policy_hash(policy.to_dict())

    policy_path = Path(path).expanduser().resolve()
    try:
        payload = tomllib.loads(policy_path.read_text(encoding="utf-8"))
    except OSError as exc:
        raise ConfigValidationError(f"cannot read recommendation policy '{policy_path}': {exc}") from exc
    except tomllib.TOMLDecodeError as exc:
        raise ConfigValidationError(f"invalid recommendation policy '{policy_path}': {exc}") from exc
    if not isinstance(payload, dict):
        raise ConfigValidationError("recommendation policy root must be a TOML table")

    weights_raw = payload.get("weights", {})
    if weights_raw in (None, ""):
        weights_raw = {}
    if not isinstance(weights_raw, dict):
        raise ConfigValidationError("recommendation.weights must be a TOML table")

    cleaned_weights: dict[str, float] = {}
    for key, value in weights_raw.items():
        name = str(key).strip().lower()
        if not name:
            raise ConfigValidationError("recommendation.weights keys must be non-empty")
        if not isinstance(value, (int, float)) or isinstance(value, bool):
            raise ConfigValidationError(f"recommendation.weights['{name}'] must be numeric")
        if float(value) < 0:
            raise ConfigValidationError(f"recommendation.weights['{name}'] must be >= 0")
        cleaned_weights[name] = float(value)

    reason_limit_raw = payload.get("reason_limit", 3)
    if not isinstance(reason_limit_raw, int) or isinstance(reason_limit_raw, bool):
        raise ConfigValidationError("recommendation.reason_limit must be an integer")
    if reason_limit_raw < 1:
        raise ConfigValidationError("recommendation.reason_limit must be >= 1")

    policy = RecommendationPolicy(weights=cleaned_weights or RecommendationPolicy().weights, reason_limit=reason_limit_raw)
    return policy, str(policy_path), _policy_hash(policy.to_dict())


def load_eval_thresholds(path: str | Path | None) -> tuple[EvalThresholds, str, str]:
    if path in (None, ""):
        thresholds = EvalThresholds()
        return thresholds, "builtin", _policy_hash(thresholds.to_dict())

    thresholds_path = Path(path).expanduser().resolve()
    try:
        payload = tomllib.loads(thresholds_path.read_text(encoding="utf-8"))
    except OSError as exc:
        raise ConfigValidationError(f"cannot read eval thresholds '{thresholds_path}': {exc}") from exc
    except tomllib.TOMLDecodeError as exc:
        raise ConfigValidationError(f"invalid eval thresholds '{thresholds_path}': {exc}") from exc
    if not isinstance(payload, dict):
        raise ConfigValidationError("eval thresholds root must be a TOML table")

    ks_raw = payload.get("ks", [10, 25, 50])
    if not isinstance(ks_raw, list):
        raise ConfigValidationError("eval.ks must be an array of integers")
    ks: list[int] = []
    for value in ks_raw:
        if not isinstance(value, int) or isinstance(value, bool):
            raise ConfigValidationError("eval.ks entries must be integers")
        if value < 1:
            raise ConfigValidationError("eval.ks entries must be >= 1")
        if value not in ks:
            ks.append(value)

    precision_raw = payload.get("precision_at_10_min", 0.60)
    recall_raw = payload.get("recall_at_25_min", 0.70)
    precision = _parse_ratio(precision_raw, field="precision_at_10_min")
    recall = _parse_ratio(recall_raw, field="recall_at_25_min")
    thresholds = EvalThresholds(ks=tuple(ks), precision_at_10_min=precision, recall_at_25_min=recall)
    return thresholds, str(thresholds_path), _policy_hash(thresholds.to_dict())


def _parse_ratio(value: Any, *, field: str) -> float:
    if not isinstance(value, (int, float)) or isinstance(value, bool):
        raise ConfigValidationError(f"eval.{field} must be numeric")
    ratio = float(value)
    if ratio < 0.0 or ratio > 1.0:
        raise ConfigValidationError(f"eval.{field} must be between 0 and 1")
    return ratio


def _policy_hash(payload: dict[str, Any]) -> str:
    digest = hashlib.sha256()
    digest.update(json.dumps(payload, sort_keys=True).encode("utf-8"))
    return digest.hexdigest()
