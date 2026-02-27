from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
import tomllib
from typing import Any

from honestroles.errors import ConfigValidationError

_ALLOWED_SEVERITIES = {"P0", "P1", "P2"}
_DEFAULT_FAIL_ON = ("P0",)
_DEFAULT_WARN_ON = ("P1",)


@dataclass(frozen=True, slots=True)
class GateRules:
    fail_on: tuple[str, ...] = _DEFAULT_FAIL_ON
    warn_on: tuple[str, ...] = _DEFAULT_WARN_ON
    max_p0: int = 0
    max_p1: int = 999999

    def to_dict(self) -> dict[str, Any]:
        return {
            "fail_on": list(self.fail_on),
            "warn_on": list(self.warn_on),
            "max_p0": self.max_p0,
            "max_p1": self.max_p1,
        }


@dataclass(frozen=True, slots=True)
class DriftRules:
    numeric_warn_psi: float = 0.10
    numeric_fail_psi: float = 0.25
    categorical_warn_jsd: float = 0.10
    categorical_fail_jsd: float = 0.20
    columns_numeric: tuple[str, ...] = ("salary_min", "salary_max")
    columns_categorical: tuple[str, ...] = (
        "source",
        "remote",
        "location",
        "company",
    )

    def to_dict(self) -> dict[str, Any]:
        return {
            "numeric_warn_psi": self.numeric_warn_psi,
            "numeric_fail_psi": self.numeric_fail_psi,
            "categorical_warn_jsd": self.categorical_warn_jsd,
            "categorical_fail_jsd": self.categorical_fail_jsd,
            "columns_numeric": list(self.columns_numeric),
            "columns_categorical": list(self.columns_categorical),
        }


@dataclass(frozen=True, slots=True)
class EDARules:
    gate: GateRules = GateRules()
    drift: DriftRules = DriftRules()

    def to_dict(self) -> dict[str, Any]:
        return {
            "gate": self.gate.to_dict(),
            "drift": self.drift.to_dict(),
        }


def load_eda_rules(
    *,
    rules_file: str | Path | None = None,
    fail_on: str | None = None,
    warn_on: str | None = None,
) -> EDARules:
    payload: dict[str, Any] = {}
    effective_rules_file = rules_file
    if effective_rules_file is None:
        default_rules = Path("eda-rules.toml").expanduser().resolve()
        if default_rules.exists():
            effective_rules_file = default_rules

    if effective_rules_file is not None:
        path = Path(effective_rules_file).expanduser().resolve()
        if not path.exists():
            raise ConfigValidationError(f"EDA rules file does not exist: '{path}'")
        if not path.is_file():
            raise ConfigValidationError(f"EDA rules path is not a file: '{path}'")
        try:
            payload = tomllib.loads(path.read_text(encoding="utf-8"))
        except (OSError, tomllib.TOMLDecodeError) as exc:
            raise ConfigValidationError(f"invalid EDA rules file '{path}': {exc}") from exc

    gate_payload = payload.get("gate", {})
    drift_payload = payload.get("drift", {})
    if not isinstance(gate_payload, dict):
        raise ConfigValidationError("EDA rules [gate] must be a table")
    if not isinstance(drift_payload, dict):
        raise ConfigValidationError("EDA rules [drift] must be a table")

    gate = GateRules(
        fail_on=_parse_severity_list(gate_payload.get("fail_on", _DEFAULT_FAIL_ON), "gate.fail_on"),
        warn_on=_parse_severity_list(gate_payload.get("warn_on", _DEFAULT_WARN_ON), "gate.warn_on"),
        max_p0=_parse_int(gate_payload.get("max_p0", 0), "gate.max_p0"),
        max_p1=_parse_int(gate_payload.get("max_p1", 999999), "gate.max_p1"),
    )

    if fail_on is not None:
        gate = GateRules(
            fail_on=_parse_severity_csv(fail_on, "--fail-on"),
            warn_on=gate.warn_on,
            max_p0=gate.max_p0,
            max_p1=gate.max_p1,
        )
    if warn_on is not None:
        gate = GateRules(
            fail_on=gate.fail_on,
            warn_on=_parse_severity_csv(warn_on, "--warn-on"),
            max_p0=gate.max_p0,
            max_p1=gate.max_p1,
        )

    drift = DriftRules(
        numeric_warn_psi=_parse_float(
            drift_payload.get("numeric_warn_psi", 0.10),
            "drift.numeric_warn_psi",
        ),
        numeric_fail_psi=_parse_float(
            drift_payload.get("numeric_fail_psi", 0.25),
            "drift.numeric_fail_psi",
        ),
        categorical_warn_jsd=_parse_float(
            drift_payload.get("categorical_warn_jsd", 0.10),
            "drift.categorical_warn_jsd",
        ),
        categorical_fail_jsd=_parse_float(
            drift_payload.get("categorical_fail_jsd", 0.20),
            "drift.categorical_fail_jsd",
        ),
        columns_numeric=_parse_column_list(
            drift_payload.get("columns_numeric", ("salary_min", "salary_max")),
            "drift.columns_numeric",
        ),
        columns_categorical=_parse_column_list(
            drift_payload.get(
                "columns_categorical",
                ("source", "remote", "location", "company"),
            ),
            "drift.columns_categorical",
        ),
    )

    _validate_gate_rules(gate)
    _validate_drift_rules(drift)
    return EDARules(gate=gate, drift=drift)


def _validate_gate_rules(rules: GateRules) -> None:
    if rules.max_p0 < 0:
        raise ConfigValidationError("gate.max_p0 must be >= 0")
    if rules.max_p1 < 0:
        raise ConfigValidationError("gate.max_p1 must be >= 0")


def _validate_drift_rules(rules: DriftRules) -> None:
    if rules.numeric_warn_psi < 0 or rules.numeric_fail_psi < 0:
        raise ConfigValidationError("drift numeric thresholds must be >= 0")
    if rules.categorical_warn_jsd < 0 or rules.categorical_fail_jsd < 0:
        raise ConfigValidationError("drift categorical thresholds must be >= 0")
    if rules.numeric_fail_psi < rules.numeric_warn_psi:
        raise ConfigValidationError("drift.numeric_fail_psi must be >= drift.numeric_warn_psi")
    if rules.categorical_fail_jsd < rules.categorical_warn_jsd:
        raise ConfigValidationError(
            "drift.categorical_fail_jsd must be >= drift.categorical_warn_jsd"
        )


def _parse_severity_list(value: object, label: str) -> tuple[str, ...]:
    if isinstance(value, tuple):
        values = list(value)
    elif isinstance(value, list):
        values = value
    else:
        raise ConfigValidationError(f"{label} must be an array")

    out: list[str] = []
    seen: set[str] = set()
    for raw in values:
        if not isinstance(raw, str):
            raise ConfigValidationError(f"{label} entries must be strings")
        severity = raw.strip().upper()
        if severity not in _ALLOWED_SEVERITIES:
            raise ConfigValidationError(
                f"{label} includes unsupported severity '{severity}'"
            )
        if severity not in seen:
            seen.add(severity)
            out.append(severity)
    return tuple(out)


def _parse_severity_csv(value: str, label: str) -> tuple[str, ...]:
    parts = [item.strip().upper() for item in value.split(",") if item.strip()]
    if not parts:
        raise ConfigValidationError(f"{label} must include at least one severity")
    return _parse_severity_list(parts, label)


def _parse_column_list(value: object, label: str) -> tuple[str, ...]:
    if isinstance(value, tuple):
        values = list(value)
    elif isinstance(value, list):
        values = value
    else:
        raise ConfigValidationError(f"{label} must be an array")

    out: list[str] = []
    seen: set[str] = set()
    for raw in values:
        if not isinstance(raw, str):
            raise ConfigValidationError(f"{label} entries must be strings")
        column = raw.strip()
        if not column:
            raise ConfigValidationError(f"{label} entries must be non-empty")
        if column not in seen:
            seen.add(column)
            out.append(column)
    return tuple(out)


def _parse_int(value: object, label: str) -> int:
    if not isinstance(value, int):
        raise ConfigValidationError(f"{label} must be an integer")
    return value


def _parse_float(value: object, label: str) -> float:
    if not isinstance(value, (int, float)):
        raise ConfigValidationError(f"{label} must be a number")
    return float(value)
