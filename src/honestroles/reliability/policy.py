from __future__ import annotations

from dataclasses import dataclass
import hashlib
import json
from pathlib import Path
import tomllib
from typing import Any

from honestroles.errors import ConfigValidationError


@dataclass(frozen=True, slots=True)
class FreshnessRule:
    column: str = "posted_at"
    max_age_days: int = 30

    def to_dict(self) -> dict[str, Any]:
        return {
            "column": self.column,
            "max_age_days": self.max_age_days,
        }


@dataclass(frozen=True, slots=True)
class ReliabilityPolicy:
    min_rows: int = 1
    required_columns: tuple[str, ...] = ("title", "description_text")
    max_null_pct: dict[str, float] | None = None
    freshness: FreshnessRule = FreshnessRule()

    def __post_init__(self) -> None:
        if self.max_null_pct is None:
            object.__setattr__(
                self,
                "max_null_pct",
                {
                    "title": 10.0,
                    "description_text": 15.0,
                },
            )

    def to_dict(self) -> dict[str, Any]:
        return {
            "min_rows": self.min_rows,
            "required_columns": list(self.required_columns),
            "max_null_pct": dict(sorted((self.max_null_pct or {}).items())),
            "freshness": self.freshness.to_dict(),
        }


@dataclass(frozen=True, slots=True)
class LoadedReliabilityPolicy:
    policy: ReliabilityPolicy
    source: str
    policy_hash: str


def default_reliability_policy() -> ReliabilityPolicy:
    return ReliabilityPolicy()


def load_reliability_policy(path: str | Path | None = None) -> LoadedReliabilityPolicy:
    if path in (None, ""):
        policy = default_reliability_policy()
        return LoadedReliabilityPolicy(
            policy=policy,
            source="builtin:default",
            policy_hash=_hash_policy(policy),
        )

    policy_path = Path(path).expanduser().resolve()
    if not policy_path.exists():
        raise ConfigValidationError(f"reliability policy file does not exist: '{policy_path}'")
    if not policy_path.is_file():
        raise ConfigValidationError(f"reliability policy path is not a file: '{policy_path}'")

    try:
        payload = tomllib.loads(policy_path.read_text(encoding="utf-8"))
    except (OSError, tomllib.TOMLDecodeError) as exc:
        raise ConfigValidationError(f"invalid reliability policy '{policy_path}': {exc}") from exc

    policy = _parse_policy(payload)
    return LoadedReliabilityPolicy(
        policy=policy,
        source=str(policy_path),
        policy_hash=_hash_policy(policy),
    )


def _parse_policy(payload: dict[str, Any]) -> ReliabilityPolicy:
    if not isinstance(payload, dict):
        raise ConfigValidationError("reliability policy root must be a TOML table")

    min_rows = payload.get("min_rows", 1)
    if not isinstance(min_rows, int) or isinstance(min_rows, bool):
        raise ConfigValidationError("reliability.min_rows must be an integer")
    if min_rows < 1:
        raise ConfigValidationError("reliability.min_rows must be >= 1")

    required_columns = payload.get("required_columns", ["title", "description_text"])
    if not isinstance(required_columns, list):
        raise ConfigValidationError("reliability.required_columns must be an array of strings")
    cleaned_required: list[str] = []
    seen_required: set[str] = set()
    for raw in required_columns:
        if not isinstance(raw, str):
            raise ConfigValidationError("reliability.required_columns entries must be strings")
        column = raw.strip()
        if not column:
            raise ConfigValidationError("reliability.required_columns entries must be non-empty")
        if column not in seen_required:
            seen_required.add(column)
            cleaned_required.append(column)

    max_null_pct = payload.get("max_null_pct", {"title": 10.0, "description_text": 15.0})
    if not isinstance(max_null_pct, dict):
        raise ConfigValidationError("reliability.max_null_pct must be a table")
    cleaned_null_pct: dict[str, float] = {}
    for raw_key, raw_value in max_null_pct.items():
        if not isinstance(raw_key, str):
            raise ConfigValidationError("reliability.max_null_pct keys must be strings")
        key = raw_key.strip()
        if not key:
            raise ConfigValidationError("reliability.max_null_pct keys must be non-empty")
        if not isinstance(raw_value, (int, float)) or isinstance(raw_value, bool):
            raise ConfigValidationError(
                f"reliability.max_null_pct['{key}'] must be a number between 0 and 100"
            )
        value = float(raw_value)
        if value < 0 or value > 100:
            raise ConfigValidationError(
                f"reliability.max_null_pct['{key}'] must be between 0 and 100"
            )
        cleaned_null_pct[key] = value

    freshness = payload.get("freshness", {})
    if not isinstance(freshness, dict):
        raise ConfigValidationError("reliability.freshness must be a table")

    freshness_column = freshness.get("column", "posted_at")
    if not isinstance(freshness_column, str) or not freshness_column.strip():
        raise ConfigValidationError("reliability.freshness.column must be a non-empty string")

    freshness_age = freshness.get("max_age_days", 30)
    if not isinstance(freshness_age, int) or isinstance(freshness_age, bool):
        raise ConfigValidationError("reliability.freshness.max_age_days must be an integer")
    if freshness_age < 0:
        raise ConfigValidationError("reliability.freshness.max_age_days must be >= 0")

    return ReliabilityPolicy(
        min_rows=min_rows,
        required_columns=tuple(cleaned_required),
        max_null_pct=cleaned_null_pct,
        freshness=FreshnessRule(
            column=freshness_column.strip(),
            max_age_days=freshness_age,
        ),
    )


def _hash_policy(policy: ReliabilityPolicy) -> str:
    digest = hashlib.sha256()
    digest.update(json.dumps(policy.to_dict(), sort_keys=True).encode("utf-8"))
    return digest.hexdigest()
