from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Mapping

from honestroles.config.models import RuntimeQualityConfig
from honestroles.errors import ConfigValidationError

from .charts import write_chart_figures
from .models import EDAArtifactsBundle, EDAArtifactsManifest
from .profile import build_eda_profile
from .report import write_report_markdown
from .rules import load_eda_rules

_SCHEMA_VERSION = "1.1"


def generate_eda_artifacts(
    *,
    input_parquet: str | Path,
    output_dir: str | Path = "dist/eda/latest",
    quality_profile: str = "core_fields_weighted",
    field_weights: Mapping[str, float] | None = None,
    top_k: int = 10,
    max_rows: int | None = None,
    rules_file: str | Path | None = None,
) -> EDAArtifactsManifest:
    input_path = Path(input_parquet).expanduser().resolve()
    artifacts_dir = Path(output_dir).expanduser().resolve()

    if not input_path.exists():
        raise ConfigValidationError(f"input parquet does not exist: '{input_path}'")
    if not input_path.is_file():
        raise ConfigValidationError(f"input parquet is not a file: '{input_path}'")

    try:
        cfg = RuntimeQualityConfig(
            profile=quality_profile,
            field_weights=dict(field_weights or {}),
        )
    except Exception as exc:  # pydantic validation error
        raise ConfigValidationError(f"invalid EDA quality configuration: {exc}") from exc

    rules = load_eda_rules(rules_file=rules_file)

    profile = build_eda_profile(
        input_parquet=input_path,
        quality_profile=cfg.profile,
        field_weights=cfg.field_weights,
        top_k=top_k,
        max_rows=max_rows,
    )

    profile.summary["rules_context"] = rules.to_dict()

    tables_dir = artifacts_dir / "tables"
    figures_dir = artifacts_dir / "figures"
    artifacts_dir.mkdir(parents=True, exist_ok=True)
    tables_dir.mkdir(parents=True, exist_ok=True)
    figures_dir.mkdir(parents=True, exist_ok=True)

    summary_path = artifacts_dir / "summary.json"
    summary_path.write_text(
        json.dumps(profile.summary, indent=2, sort_keys=True), encoding="utf-8"
    )

    table_file_map = {
        "null_percentages": "tables/null_percentages.parquet",
        "column_profile": "tables/column_profile.parquet",
        "source_profile": "tables/source_profile.parquet",
        "top_values_source": "tables/top_values_source.parquet",
        "top_values_company": "tables/top_values_company.parquet",
        "top_values_title": "tables/top_values_title.parquet",
        "top_values_location": "tables/top_values_location.parquet",
        "numeric_quantiles": "tables/numeric_quantiles.parquet",
        "categorical_distribution": "tables/categorical_distribution.parquet",
    }
    for key, relative_path in table_file_map.items():
        table = profile.tables.get(key)
        if table is None:
            continue
        table.write_parquet(artifacts_dir / relative_path)

    figure_file_map = write_chart_figures(profile.summary, figures_dir)

    report_path = artifacts_dir / "report.md"
    write_report_markdown(profile.summary, report_path)

    files: dict[str, str] = {
        "summary_json": "summary.json",
        "report_md": "report.md",
    }
    files.update(table_file_map)
    files.update({key: f"figures/{filename}" for key, filename in figure_file_map.items()})

    manifest = EDAArtifactsManifest(
        schema_version=_SCHEMA_VERSION,
        artifact_kind="profile",
        generated_at_utc=datetime.now(timezone.utc).isoformat(),
        input_path=str(input_path),
        row_count_raw=int(profile.summary["shape"]["raw"]["rows"]),
        row_count_runtime=int(profile.summary["shape"]["runtime"]["rows"]),
        quality_profile=str(profile.summary["quality"]["profile"]),
        files=files,
        rules_context=rules.to_dict(),
    )

    _write_manifest(artifacts_dir=artifacts_dir, manifest=manifest)
    return manifest


def generate_eda_diff_artifacts(
    *,
    baseline_dir: str | Path,
    candidate_dir: str | Path,
    output_dir: str | Path = "dist/eda/diff",
    rules_file: str | Path | None = None,
    fail_on: str | None = None,
    warn_on: str | None = None,
) -> EDAArtifactsManifest:
    from .diff import build_eda_diff

    rules = load_eda_rules(rules_file=rules_file, fail_on=fail_on, warn_on=warn_on)
    diff_payload, tables, _, candidate_summary = build_eda_diff(
        baseline_dir=baseline_dir,
        candidate_dir=candidate_dir,
        rules=rules,
    )

    artifacts_dir = Path(output_dir).expanduser().resolve()
    tables_dir = artifacts_dir / "tables"
    artifacts_dir.mkdir(parents=True, exist_ok=True)
    tables_dir.mkdir(parents=True, exist_ok=True)

    diff_json_path = artifacts_dir / "diff.json"
    diff_json_path.write_text(json.dumps(diff_payload, indent=2, sort_keys=True), encoding="utf-8")

    table_file_map = {
        "diff_null_percentages": "tables/diff_null_percentages.parquet",
        "diff_source_profile": "tables/diff_source_profile.parquet",
        "drift_metrics": "tables/drift_metrics.parquet",
        "findings_delta": "tables/findings_delta.parquet",
    }
    for key, relative_path in table_file_map.items():
        table = tables.get(key)
        if table is None:
            continue
        table.write_parquet(artifacts_dir / relative_path)

    files: dict[str, str] = {"diff_json": "diff.json"}
    files.update(table_file_map)

    manifest = EDAArtifactsManifest(
        schema_version=_SCHEMA_VERSION,
        artifact_kind="diff",
        generated_at_utc=datetime.now(timezone.utc).isoformat(),
        input_path=str(candidate_summary["shape"]["input_path"]),
        row_count_raw=int(candidate_summary["shape"]["raw"]["rows"]),
        row_count_runtime=int(candidate_summary["shape"]["runtime"]["rows"]),
        quality_profile=str(candidate_summary["quality"]["profile"]),
        files=files,
        rules_context=rules.to_dict(),
    )
    _write_manifest(artifacts_dir=artifacts_dir, manifest=manifest)
    return manifest


def load_eda_artifacts(artifacts_dir: str | Path) -> EDAArtifactsBundle:
    root = Path(artifacts_dir).expanduser().resolve()
    manifest_path = root / "manifest.json"
    if not manifest_path.exists():
        raise ConfigValidationError(
            f"artifacts manifest missing: '{manifest_path}'"
        )

    try:
        payload = json.loads(manifest_path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:
        raise ConfigValidationError(
            f"invalid artifacts manifest JSON: '{manifest_path}'"
        ) from exc

    for key in [
        "schema_version",
        "artifact_kind",
        "generated_at_utc",
        "input_path",
        "row_count_raw",
        "row_count_runtime",
        "quality_profile",
        "files",
    ]:
        if key not in payload:
            raise ConfigValidationError(f"artifacts manifest missing key '{key}'")

    files = payload["files"]
    if not isinstance(files, dict):
        raise ConfigValidationError("artifacts manifest 'files' must be an object")

    rules_context = payload.get("rules_context")
    if rules_context is not None and not isinstance(rules_context, dict):
        raise ConfigValidationError("artifacts manifest 'rules_context' must be an object")

    manifest = EDAArtifactsManifest(
        schema_version=str(payload["schema_version"]),
        artifact_kind=str(payload["artifact_kind"]),
        generated_at_utc=str(payload["generated_at_utc"]),
        input_path=str(payload["input_path"]),
        row_count_raw=int(payload["row_count_raw"]),
        row_count_runtime=int(payload["row_count_runtime"]),
        quality_profile=str(payload["quality_profile"]),
        files={str(k): str(v) for k, v in files.items()},
        rules_context={str(k): v for k, v in rules_context.items()} if rules_context else None,
    )

    summary: dict[str, object] | None = None
    diff: dict[str, object] | None = None

    if manifest.artifact_kind == "profile":
        for key in ["summary_json", "report_md"]:
            if key not in manifest.files:
                raise ConfigValidationError(
                    f"artifacts manifest missing file mapping '{key}'"
                )
        summary_path = root / manifest.files["summary_json"]
        if not summary_path.exists():
            raise ConfigValidationError(f"artifacts summary missing: '{summary_path}'")
        try:
            summary = json.loads(summary_path.read_text(encoding="utf-8"))
        except json.JSONDecodeError as exc:
            raise ConfigValidationError(f"invalid summary JSON: '{summary_path}'") from exc
    elif manifest.artifact_kind == "diff":
        if "diff_json" not in manifest.files:
            raise ConfigValidationError("artifacts manifest missing file mapping 'diff_json'")
        diff_path = root / manifest.files["diff_json"]
        if not diff_path.exists():
            raise ConfigValidationError(f"artifacts diff JSON missing: '{diff_path}'")
        try:
            diff = json.loads(diff_path.read_text(encoding="utf-8"))
        except json.JSONDecodeError as exc:
            raise ConfigValidationError(f"invalid diff JSON: '{diff_path}'") from exc
    else:
        raise ConfigValidationError(
            f"unsupported artifact_kind '{manifest.artifact_kind}' in manifest"
        )

    for relative_path in manifest.files.values():
        path = root / relative_path
        if not path.exists():
            raise ConfigValidationError(f"artifacts file missing: '{path}'")

    return EDAArtifactsBundle(artifacts_dir=root, manifest=manifest, summary=summary, diff=diff)


def _write_manifest(*, artifacts_dir: Path, manifest: EDAArtifactsManifest) -> None:
    manifest_path = artifacts_dir / "manifest.json"
    manifest_path.write_text(
        json.dumps(
            {
                "schema_version": manifest.schema_version,
                "artifact_kind": manifest.artifact_kind,
                "generated_at_utc": manifest.generated_at_utc,
                "input_path": manifest.input_path,
                "row_count_raw": manifest.row_count_raw,
                "row_count_runtime": manifest.row_count_runtime,
                "quality_profile": manifest.quality_profile,
                "files": manifest.files,
                "rules_context": manifest.rules_context,
            },
            indent=2,
            sort_keys=True,
        ),
        encoding="utf-8",
    )
