from __future__ import annotations

from dataclasses import dataclass
from typing import Literal


CanonicalLogicalType = Literal["string", "bool", "float", "list_string"]


@dataclass(frozen=True, slots=True)
class CanonicalFieldSpec:
    name: str
    logical_type: CanonicalLogicalType
    nullable: bool = True


CANONICAL_JOB_SCHEMA: dict[str, CanonicalFieldSpec] = {
    "id": CanonicalFieldSpec(name="id", logical_type="string"),
    "title": CanonicalFieldSpec(name="title", logical_type="string"),
    "company": CanonicalFieldSpec(name="company", logical_type="string"),
    "location": CanonicalFieldSpec(name="location", logical_type="string"),
    "remote": CanonicalFieldSpec(name="remote", logical_type="bool"),
    "description_text": CanonicalFieldSpec(name="description_text", logical_type="string"),
    "description_html": CanonicalFieldSpec(name="description_html", logical_type="string"),
    "skills": CanonicalFieldSpec(name="skills", logical_type="list_string"),
    "salary_min": CanonicalFieldSpec(name="salary_min", logical_type="float"),
    "salary_max": CanonicalFieldSpec(name="salary_max", logical_type="float"),
    "apply_url": CanonicalFieldSpec(name="apply_url", logical_type="string"),
    "posted_at": CanonicalFieldSpec(name="posted_at", logical_type="string"),
}

CANONICAL_SOURCE_FIELDS: tuple[str, ...] = tuple(CANONICAL_JOB_SCHEMA.keys())

