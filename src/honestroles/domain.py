from __future__ import annotations

from collections.abc import Callable, Iterable, Iterator, Mapping
from dataclasses import dataclass
from itertools import islice
from typing import Any

import polars as pl

from honestroles.schema import CANONICAL_JOB_SCHEMA, CANONICAL_SOURCE_FIELDS


_CANONICAL_FLOAT_FIELDS = {"salary_min", "salary_max"}
_CANONICAL_BOOL_FIELDS = {"remote"}
_CANONICAL_TUPLE_FIELDS = {"skills"}
_CANONICAL_STRING_FIELDS = tuple(
    name for name, spec in CANONICAL_JOB_SCHEMA.items() if spec.logical_type == "string"
)


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
        for field_name in _CANONICAL_STRING_FIELDS:
            value = getattr(self, field_name)
            if value is not None and not isinstance(value, str):
                raise TypeError(f"{field_name} must be str | None")
        if self.remote is not None and not isinstance(self.remote, bool):
            raise TypeError("remote must be bool | None")
        for field_name in _CANONICAL_FLOAT_FIELDS:
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
    _frame: pl.DataFrame
    canonical_fields: tuple[str, ...] = CANONICAL_SOURCE_FIELDS

    def __post_init__(self) -> None:
        if not isinstance(self._frame, pl.DataFrame):
            raise TypeError("_frame must be a polars.DataFrame")
        if not isinstance(self.canonical_fields, tuple):
            raise TypeError("canonical_fields must be a tuple[str, ...]")
        for name in self.canonical_fields:
            if not isinstance(name, str):
                raise TypeError("canonical_fields must contain only strings")
        if len(set(self.canonical_fields)) != len(self.canonical_fields):
            raise TypeError("canonical_fields must not contain duplicates")
        if self.canonical_fields != CANONICAL_SOURCE_FIELDS:
            raise TypeError("canonical_fields must exactly match the canonical source schema")

    @classmethod
    def from_polars(cls, df: pl.DataFrame) -> "JobDataset":
        dataset = cls._from_polars_unchecked(df)
        dataset.validate()
        return dataset

    @classmethod
    def _from_polars_unchecked(cls, df: pl.DataFrame) -> "JobDataset":
        return cls(_frame=df)

    def to_polars(self, *, copy: bool = True) -> pl.DataFrame:
        return self._frame.clone() if copy else self._frame

    def row_count(self) -> int:
        return self._frame.height

    def columns(self) -> tuple[str, ...]:
        return tuple(self._frame.columns)

    def missing_canonical_fields(self) -> tuple[str, ...]:
        return tuple(name for name in self.canonical_fields if name not in self._frame.columns)

    def validate_canonical_schema(self) -> None:
        missing = self.missing_canonical_fields()
        if missing:
            raise ValueError("dataset is missing canonical fields: " + ", ".join(missing))

    def validate_canonical_types(self) -> None:
        self.validate_canonical_schema()
        schema = self._frame.schema
        for name, spec in CANONICAL_JOB_SCHEMA.items():
            dtype = schema[name]
            if spec.logical_type == "string":
                if dtype not in {pl.String, pl.Null}:
                    raise TypeError(
                        f"dataset field '{name}' has invalid dtype '{dtype}', expected String or Null"
                    )
            elif spec.logical_type == "bool":
                if dtype not in {pl.Boolean, pl.Null}:
                    raise TypeError(
                        f"dataset field '{name}' has invalid dtype '{dtype}', expected Boolean or Null"
                    )
            elif spec.logical_type == "float":
                if dtype != pl.Null and not dtype.is_numeric():
                    raise TypeError(
                        f"dataset field '{name}' has invalid dtype '{dtype}', expected numeric or Null"
                    )
            elif spec.logical_type == "list_string":
                if dtype == pl.Null:
                    continue
                if not isinstance(dtype, pl.List) or dtype.inner not in {pl.String, pl.Null}:
                    raise TypeError(
                        f"dataset field '{name}' has invalid dtype '{dtype}', expected List(String) or Null"
                    )
            else:
                raise TypeError(
                    f"unsupported logical type '{spec.logical_type}' for canonical field '{name}'"
                )

    def validate(self) -> None:
        self.validate_canonical_types()

    def iter_records(self) -> Iterator[CanonicalJobRecord]:
        self.validate()
        for row in self._frame.select(list(self.canonical_fields)).iter_rows(named=True):
            yield CanonicalJobRecord.from_mapping(row)

    def materialize_records(self, limit: int | None = None) -> list[CanonicalJobRecord]:
        if limit is not None:
            if not isinstance(limit, int):
                raise TypeError("limit must be an int or None")
            if limit < 0:
                raise ValueError("limit must be >= 0")
        iterator = self.iter_records()
        if limit is None:
            return list(iterator)
        return list(islice(iterator, limit))

    def with_frame(self, frame: pl.DataFrame) -> "JobDataset":
        return JobDataset.from_polars(frame)

    def transform(self, fn: Callable[[pl.DataFrame], pl.DataFrame]) -> "JobDataset":
        frame = self.to_polars(copy=False).clone()
        result = fn(frame)
        if not isinstance(result, pl.DataFrame):
            raise TypeError("transform function must return a polars.DataFrame")
        return JobDataset.from_polars(result)


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
