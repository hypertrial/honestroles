from __future__ import annotations

from collections import defaultdict
from datetime import UTC, datetime
import hashlib
import json
from pathlib import Path
from typing import Any

from honestroles.errors import ConfigValidationError

from .models import FeedbackEventType, FeedbackResult, FeedbackSummary, SCHEMA_VERSION, SIGNAL_KEYS

_FEEDBACK_FACTORS: dict[str, dict[str, float]] = {
    "not_relevant": {
        "skills": 0.95,
        "title": 0.95,
        "location_work_mode": 0.90,
        "seniority": 0.95,
        "recency": 1.00,
        "compensation": 0.98,
    },
    "applied": {
        "skills": 1.03,
        "title": 1.03,
        "location_work_mode": 1.01,
        "seniority": 1.02,
        "recency": 1.00,
        "compensation": 1.01,
    },
    "interviewed": {
        "skills": 1.08,
        "title": 1.08,
        "location_work_mode": 1.04,
        "seniority": 1.05,
        "recency": 1.00,
        "compensation": 1.02,
    },
}


def feedback_root() -> Path:
    return (Path.cwd() / ".honestroles" / "recommend" / "feedback").resolve()


def events_file() -> Path:
    return feedback_root() / "events.jsonl"


def weights_file(profile_id: str) -> Path:
    return feedback_root() / "weights" / f"{profile_id}.json"


def load_profile_weights(profile_id: str) -> dict[str, float]:
    path = weights_file(profile_id)
    if not path.exists():
        return {key: 1.0 for key in SIGNAL_KEYS}
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {key: 1.0 for key in SIGNAL_KEYS}
    if not isinstance(payload, dict):
        return {key: 1.0 for key in SIGNAL_KEYS}
    out: dict[str, float] = {}
    for key in SIGNAL_KEYS:
        value = payload.get(key, 1.0)
        if not isinstance(value, (int, float)):
            out[key] = 1.0
            continue
        out[key] = float(max(0.5, min(1.5, value)))
    return out


def record_feedback_event(
    *,
    profile_id: str,
    job_id: str,
    event: FeedbackEventType,
    meta_json_file: str | Path | None = None,
) -> FeedbackResult:
    if event not in _FEEDBACK_FACTORS:
        raise ConfigValidationError(
            "feedback event must be one of: not_relevant, applied, interviewed"
        )
    profile = _required_text(profile_id, field="profile_id")
    job = _required_text(job_id, field="job_id")

    meta = _load_meta(meta_json_file)
    identity_payload = {
        "schema_version": SCHEMA_VERSION,
        "profile_id": profile,
        "job_id": job,
        "event": event,
        "meta": meta,
    }
    event_hash = _event_hash(identity_payload)
    event_payload = {
        **identity_payload,
        "recorded_at_utc": datetime.now(UTC).replace(microsecond=0).isoformat(),
        "event_hash": event_hash,
    }

    root = feedback_root()
    root.mkdir(parents=True, exist_ok=True)
    target_events_file = events_file()
    target_events_file.parent.mkdir(parents=True, exist_ok=True)

    duplicate = _event_hash_exists(target_events_file, event_hash)
    if not duplicate:
        with target_events_file.open("a", encoding="utf-8") as handle:
            handle.write(json.dumps(event_payload, sort_keys=True) + "\n")

    current_weights = load_profile_weights(profile)
    if not duplicate:
        factors = _FEEDBACK_FACTORS[event]
        updated_weights = {
            key: _clamp_weight(current_weights[key] * factors[key]) for key in SIGNAL_KEYS
        }
        _write_weights(profile, updated_weights)
    else:
        updated_weights = current_weights

    total_events = _count_events(target_events_file)
    return FeedbackResult(
        schema_version=SCHEMA_VERSION,
        status="pass",
        profile_id=profile,
        job_id=job,
        event=event,
        duplicate=duplicate,
        events_file=str(target_events_file),
        weights_file=str(weights_file(profile)),
        total_events=total_events,
        weights=updated_weights,
    )


def summarize_feedback(profile_id: str | None = None) -> FeedbackSummary:
    target_file = events_file()
    all_counts: dict[str, int] = defaultdict(int)
    per_profile: dict[str, dict[str, int]] = defaultdict(lambda: defaultdict(int))
    total = 0

    if target_file.exists():
        for line in target_file.read_text(encoding="utf-8").splitlines():
            if not line.strip():
                continue
            payload = json.loads(line)
            if not isinstance(payload, dict):
                continue
            profile = str(payload.get("profile_id", "")).strip().lower()
            event = str(payload.get("event", "")).strip().lower()
            if not profile or not event:
                continue
            total += 1
            all_counts[event] += 1
            per_profile[profile][event] += 1

    resolved_profile = (
        _required_text(profile_id, field="profile_id") if profile_id not in (None, "") else None
    )

    if resolved_profile is None:
        current_weights = None
    else:
        current_weights = load_profile_weights(resolved_profile)

    return FeedbackSummary(
        schema_version=SCHEMA_VERSION,
        status="pass",
        profile_id=resolved_profile,
        events_file=str(target_file),
        total_events=total,
        counts=dict(all_counts),
        profile_counts={profile: dict(counter) for profile, counter in per_profile.items()},
        weights=current_weights,
    )


def _load_meta(meta_json_file: str | Path | None) -> dict[str, Any]:
    if meta_json_file in (None, ""):
        return {}
    meta_path = Path(meta_json_file).expanduser().resolve()
    try:
        payload = json.loads(meta_path.read_text(encoding="utf-8"))
    except OSError as exc:
        raise ConfigValidationError(f"cannot read feedback meta JSON '{meta_path}': {exc}") from exc
    except json.JSONDecodeError as exc:
        raise ConfigValidationError(f"invalid feedback meta JSON '{meta_path}': {exc}") from exc
    if not isinstance(payload, dict):
        raise ConfigValidationError("feedback meta JSON root must be an object")
    return payload


def _write_weights(profile_id: str, payload: dict[str, float]) -> None:
    path = weights_file(profile_id)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")


def _event_hash_exists(path: Path, event_hash: str) -> bool:
    if not path.exists():
        return False
    lines = path.read_text(encoding="utf-8").splitlines()
    # Bound check to keep this deterministic and cheap for long histories.
    for line in lines[-1000:]:
        if not line.strip():
            continue
        payload = json.loads(line)
        if not isinstance(payload, dict):
            continue
        if str(payload.get("event_hash", "")) == event_hash:
            return True
    return False


def _count_events(path: Path) -> int:
    if not path.exists():
        return 0
    count = 0
    for line in path.read_text(encoding="utf-8").splitlines():
        if line.strip():
            count += 1
    return count


def _event_hash(payload: dict[str, Any]) -> str:
    digest = hashlib.sha256()
    digest.update(json.dumps(payload, sort_keys=True).encode("utf-8"))
    return digest.hexdigest()


def _required_text(value: str, *, field: str) -> str:
    text = str(value).strip().lower()
    if not text:
        raise ConfigValidationError(f"feedback.{field} must be a non-empty string")
    return text


def _clamp_weight(value: float) -> float:
    return float(max(0.5, min(1.5, value)))
