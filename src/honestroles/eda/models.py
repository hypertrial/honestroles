from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any

import polars as pl


@dataclass(frozen=True, slots=True)
class EDAArtifactsManifest:
    schema_version: str
    artifact_kind: str
    generated_at_utc: str
    input_path: str
    row_count_raw: int
    row_count_runtime: int
    quality_profile: str
    files: dict[str, str]
    rules_context: dict[str, Any] | None = None


@dataclass(frozen=True, slots=True)
class EDAArtifactsBundle:
    artifacts_dir: Path
    manifest: EDAArtifactsManifest
    summary: dict[str, Any] | None = None
    diff: dict[str, Any] | None = None


@dataclass(frozen=True, slots=True)
class EDAProfileResult:
    summary: dict[str, Any]
    tables: dict[str, pl.DataFrame]
