from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Iterable, Mapping

import polars as pl

from honestroles.config.models import CANONICAL_SOURCE_FIELDS


_CANONICAL_FLOAT_FIELDS = {"salary_min", "salary_max"}
_CANONICAL_BOOL_FIELDS = {"remote"}
_CANONICAL_TUPLE_FIELDS = {"skills"}


@dataclass(frozen=True, slots=True)
class CanonicalJobRecord:
    id: str | None = None
    title: str | None = None
    company: str | None = None
    location: str | None = None
    remote: bool | None = None
    description_text: str | None = None
    description_html: str | None = None
    skills: tuple[str, ...] = ()
    salary_min: float | None = None
    salary_max: float | None = None
    apply_url: str | None = None
    posted_at: str | None = None

    def __post_init__(self) -> None:
        for field_name in (
            "id",
            "title",
            "company",
            "location",
            "description_text",
            "description_html",
            "apply_url",
            "posted_at",
        ):
            value = getattr(self, field_name)
            if value is not None and not isinstance(value, str):
                raise TypeError(f"{field_name} must be str | None")
        if self.remote is not None and not isinstance(self.remote, bool):
            raise TypeError("remote must be bool | None")
        for field_name in ("salary_min", "salary_max"):
            value = getattr(self, field_name)
            if value is not None and not isinstance(value, (int, float)):
                raise TypeError(f"{field_name} must be float | None")
        if not isinstance(self.skills, tuple):
            raise TypeError("skills must be a tuple[str, ...]")
        for item in self.skills:
            if not isinstance(item, str):
                raise TypeError("skills must contain only strings")

    @classmethod
    def from_mapping(cls, mapping: Mapping[str, Any]) -> "CanonicalJobRecord":
        payload = {name: _coerce_canonical_value(name, mapping.get(name)) for name in CANONICAL_SOURCE_FIELDS}
        return cls(**payload)

    def to_dict(self) -> dict[str, Any]:
        return {
            "id": self.id,
            "title": self.title,
            "company": self.company,
            "location": self.location,
            "remote": self.remote,
            "description_text": self.description_text,
            "description_html": self.description_html,
            "skills": list(self.skills),
            "salary_min": self.salary_min,
            "salary_max": self.salary_max,
            "apply_url": self.apply_url,
            "posted_at": self.posted_at,
        }


@dataclass(frozen=True, slots=True)
class JobDataset:
    frame: pl.DataFrame
    schema_version: str = "1.0"
    canonical_fields: tuple[str, ...] = field(default_factory=lambda: CANONICAL_SOURCE_FIELDS)

    def __post_init__(self) -> None:
        if not isinstance(self.frame, pl.DataFrame):
            raise TypeError("frame must be a polars.DataFrame")
        if not isinstance(self.schema_version, str) or not self.schema_version.strip():
            raise TypeError("schema_version must be a non-empty string")
        if not isinstance(self.canonical_fields, tuple):
            raise TypeError("canonical_fields must be a tuple[str, ...]")
        for name in self.canonical_fields:
            if not isinstance(name, str):
                raise TypeError("canonical_fields must contain only strings")

    @classmethod
    def from_polars(
        cls,
        df: pl.DataFrame,
        *,
        schema_version: str = "1.0",
        canonical_fields: tuple[str, ...] = CANONICAL_SOURCE_FIELDS,
    ) -> "JobDataset":
        return cls(frame=df, schema_version=schema_version, canonical_fields=canonical_fields)

    def to_polars(self) -> pl.DataFrame:
        return self.frame

    def row_count(self) -> int:
        return self.frame.height

    def columns(self) -> tuple[str, ...]:
        return tuple(self.frame.columns)

    def missing_canonical_fields(self) -> tuple[str, ...]:
        return tuple(name for name in self.canonical_fields if name not in self.frame.columns)

    def validate_canonical_schema(self) -> None:
        missing = self.missing_canonical_fields()
        if missing:
            raise ValueError(
                "dataset is missing canonical fields: " + ", ".join(missing)
            )

    def rows(self) -> list[CanonicalJobRecord]:
        selected = self.frame
        missing = list(self.missing_canonical_fields())
        if missing:
            selected = selected.with_columns(pl.lit(None).alias(name) for name in missing)
        selected = selected.select(list(self.canonical_fields))
        return [CanonicalJobRecord.from_mapping(row) for row in selected.iter_rows(named=True)]

    def select(self, *columns: str) -> "JobDataset":
        return JobDataset.from_polars(
            self.frame.select(list(columns)),
            schema_version=self.schema_version,
            canonical_fields=self.canonical_fields,
        )

    def with_frame(self, frame: pl.DataFrame) -> "JobDataset":
        return JobDataset.from_polars(
            frame,
            schema_version=self.schema_version,
            canonical_fields=self.canonical_fields,
        )


@dataclass(frozen=True, slots=True)
class ApplicationPlanEntry:
    fit_rank: int
    title: str | None
    company: str | None
    apply_url: str | None
    fit_score: float
    estimated_effort_minutes: int

    def __post_init__(self) -> None:
        if self.fit_rank < 1:
            raise ValueError("fit_rank must be >= 1")
        if self.estimated_effort_minutes < 0:
            raise ValueError("estimated_effort_minutes must be >= 0")
        if not isinstance(self.fit_score, (int, float)):
            raise TypeError("fit_score must be a float")
        for field_name in ("title", "company", "apply_url"):
            value = getattr(self, field_name)
            if value is not None and not isinstance(value, str):
                raise TypeError(f"{field_name} must be str | None")

    def to_dict(self) -> dict[str, Any]:
        return {
            "fit_rank": int(self.fit_rank),
            "title": self.title,
            "company": self.company,
            "apply_url": self.apply_url,
            "fit_score": float(self.fit_score),
            "estimated_effort_minutes": int(self.estimated_effort_minutes),
        }


def _coerce_canonical_value(field_name: str, value: Any) -> Any:
    if value is None:
        return () if field_name in _CANONICAL_TUPLE_FIELDS else None
    if field_name in _CANONICAL_TUPLE_FIELDS:
        if isinstance(value, str):
            items = [item.strip() for item in value.split(",") if item.strip()]
            return tuple(items)
        if isinstance(value, Iterable) and not isinstance(value, (bytes, bytearray, Mapping)):
            return tuple(str(item).strip() for item in value if item is not None and str(item).strip())
        raise TypeError(f"{field_name} must be iterable[str] | str | None")
    if field_name in _CANONICAL_BOOL_FIELDS:
        if isinstance(value, bool):
            return value
        if isinstance(value, str):
            lowered = value.strip().lower()
            if lowered in {"true", "1", "yes", "y", "remote"}:
                return True
            if lowered in {"false", "0", "no", "n", "onsite", "on-site"}:
                return False
        raise TypeError(f"{field_name} must be bool | None")
    if field_name in _CANONICAL_FLOAT_FIELDS:
        if isinstance(value, (int, float)):
            return float(value)
        if isinstance(value, str) and value.strip():
            return float(value.strip())
        raise TypeError(f"{field_name} must be float | None")
    if isinstance(value, str):
        return value
    raise TypeError(f"{field_name} must be str | None")
