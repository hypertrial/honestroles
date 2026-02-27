from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Mapping


def _sorted_dict(values: Mapping[str, Any]) -> dict[str, Any]:
    return {key: values[key] for key in sorted(values)}


@dataclass(frozen=True, slots=True)
class StageRowCounts:
    counts: dict[str, int] = field(default_factory=dict)

    def record(self, stage: str, count: int) -> "StageRowCounts":
        updated = dict(self.counts)
        updated[stage] = int(count)
        return StageRowCounts(counts=updated)

    def to_dict(self) -> dict[str, int]:
        return {key: int(value) for key, value in _sorted_dict(self.counts).items()}


@dataclass(frozen=True, slots=True)
class PluginExecutionCounts:
    filter: int = 0
    label: int = 0
    rate: int = 0

    def to_dict(self) -> dict[str, int]:
        return {
            "filter": int(self.filter),
            "label": int(self.label),
            "rate": int(self.rate),
        }


@dataclass(frozen=True, slots=True)
class RuntimeSettingsSnapshot:
    fail_fast: bool
    random_seed: int

    def to_dict(self) -> dict[str, Any]:
        return {
            "fail_fast": self.fail_fast,
            "random_seed": int(self.random_seed),
        }


@dataclass(frozen=True, slots=True)
class InputAdapterErrorSample:
    field: str
    source: str
    value: str
    reason: str

    def to_dict(self) -> dict[str, str]:
        return {
            "field": self.field,
            "source": self.source,
            "value": self.value,
            "reason": self.reason,
        }


@dataclass(frozen=True, slots=True)
class InputAdapterDiagnostics:
    enabled: bool = True
    applied: dict[str, str] = field(default_factory=dict)
    conflicts: dict[str, int] = field(default_factory=dict)
    coercion_errors: dict[str, int] = field(default_factory=dict)
    null_like_hits: dict[str, int] = field(default_factory=dict)
    unresolved: tuple[str, ...] = ()
    error_samples: tuple[InputAdapterErrorSample, ...] = ()

    @classmethod
    def from_mapping(cls, payload: Mapping[str, Any]) -> "InputAdapterDiagnostics":
        samples = tuple(
            InputAdapterErrorSample(
                field=str(item.get("field", "")),
                source=str(item.get("source", "")),
                value=str(item.get("value", "")),
                reason=str(item.get("reason", "")),
            )
            for item in payload.get("error_samples", ())
        )
        return cls(
            enabled=bool(payload.get("enabled", True)),
            applied={str(k): str(v) for k, v in dict(payload.get("applied", {})).items()},
            conflicts={str(k): int(v) for k, v in dict(payload.get("conflicts", {})).items()},
            coercion_errors={
                str(k): int(v) for k, v in dict(payload.get("coercion_errors", {})).items()
            },
            null_like_hits={
                str(k): int(v) for k, v in dict(payload.get("null_like_hits", {})).items()
            },
            unresolved=tuple(str(item) for item in payload.get("unresolved", ())),
            error_samples=samples,
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "enabled": self.enabled,
            "applied": _sorted_dict(self.applied),
            "conflicts": {k: int(v) for k, v in _sorted_dict(self.conflicts).items()},
            "coercion_errors": {k: int(v) for k, v in _sorted_dict(self.coercion_errors).items()},
            "null_like_hits": {k: int(v) for k, v in _sorted_dict(self.null_like_hits).items()},
            "unresolved": list(self.unresolved),
            "error_samples": [item.to_dict() for item in self.error_samples],
        }


@dataclass(frozen=True, slots=True)
class InputAliasingDiagnostics:
    applied: dict[str, str] = field(default_factory=dict)
    conflicts: dict[str, int] = field(default_factory=dict)
    unresolved: tuple[str, ...] = ()

    @classmethod
    def from_mapping(cls, payload: Mapping[str, Any]) -> "InputAliasingDiagnostics":
        return cls(
            applied={str(k): str(v) for k, v in dict(payload.get("applied", {})).items()},
            conflicts={str(k): int(v) for k, v in dict(payload.get("conflicts", {})).items()},
            unresolved=tuple(str(item) for item in payload.get("unresolved", ())),
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "applied": _sorted_dict(self.applied),
            "conflicts": {k: int(v) for k, v in _sorted_dict(self.conflicts).items()},
            "unresolved": list(self.unresolved),
        }


@dataclass(frozen=True, slots=True)
class NonFatalStageError:
    stage: str
    error_type: str
    detail: str

    def to_dict(self) -> dict[str, str]:
        return {
            "stage": self.stage,
            "error_type": self.error_type,
            "detail": self.detail,
        }


@dataclass(frozen=True, slots=True)
class RuntimeDiagnostics:
    input_path: str
    stage_rows: StageRowCounts
    plugin_counts: PluginExecutionCounts
    runtime: RuntimeSettingsSnapshot
    input_adapter: InputAdapterDiagnostics
    input_aliasing: InputAliasingDiagnostics
    output_path: str | None = None
    final_rows: int = 0
    non_fatal_errors: tuple[NonFatalStageError, ...] = ()

    def to_dict(self) -> dict[str, Any]:
        payload: dict[str, Any] = {
            "input_path": self.input_path,
            "stage_rows": self.stage_rows.to_dict(),
            "plugin_counts": self.plugin_counts.to_dict(),
            "runtime": self.runtime.to_dict(),
            "input_adapter": self.input_adapter.to_dict(),
            "input_aliasing": self.input_aliasing.to_dict(),
            "final_rows": int(self.final_rows),
        }
        if self.output_path is not None:
            payload["output_path"] = self.output_path
        if self.non_fatal_errors:
            payload["non_fatal_errors"] = [item.to_dict() for item in self.non_fatal_errors]
        return payload
