from __future__ import annotations

from dataclasses import dataclass

from honestroles.diagnostics import RuntimeDiagnostics
from honestroles.domain import ApplicationPlanEntry, CanonicalJobRecord, JobDataset


@dataclass(frozen=True, slots=True)
class PipelineRun:
    dataset: JobDataset
    diagnostics: RuntimeDiagnostics
    application_plan: tuple[ApplicationPlanEntry, ...] = ()


__all__ = [
    "ApplicationPlanEntry",
    "CanonicalJobRecord",
    "JobDataset",
    "PipelineRun",
    "RuntimeDiagnostics",
]
