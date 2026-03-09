from __future__ import annotations

import argparse
from dataclasses import dataclass
import json
import os
from pathlib import Path
import shutil
import sys
from typing import Any

from honestroles.config import load_pipeline_config, load_plugin_manifest
from honestroles.eda import (
    build_eda_diff,
    evaluate_eda_gate,
    generate_eda_artifacts,
    generate_eda_diff_artifacts,
    load_eda_artifacts,
    load_eda_rules,
    parse_quality_weight_overrides,
)
from honestroles.errors import ConfigValidationError
from honestroles.io import (
    apply_source_adapter,
    build_data_quality_report,
    infer_source_adapter,
    normalize_source_data_contract,
    read_parquet,
    resolve_source_aliases,
    validate_source_data_contract,
)
from honestroles.plugins.registry import PluginRegistry
from honestroles.runtime import HonestRolesRuntime

from .lineage import list_records, load_record

_EXIT_OK = 0
_EXIT_GENERIC = 1
_EXIT_CONFIG = 2


@dataclass(frozen=True, slots=True)
class CommandResult:
    payload: dict[str, Any]
    exit_code: int = _EXIT_OK


def _build_pipeline_toml(*, input_path: Path, output_path: Path, adapter_fragment: str | None) -> str:
    sections = [
        "[input]",
        'kind = "parquet"',
        f'path = "{input_path}"',
        "",
    ]
    if adapter_fragment:
        sections.extend([adapter_fragment.strip(), ""])
    sections.extend(
        [
            "[output]",
            f'path = "{output_path}"',
            "",
            "[stages.clean]",
            "enabled = true",
            "",
            "[stages.filter]",
            "enabled = true",
            "remote_only = false",
            "",
            "[stages.label]",
            "enabled = true",
            "",
            "[stages.rate]",
            "enabled = true",
            "",
            "[stages.match]",
            "enabled = true",
            "top_k = 50",
            "",
            "[runtime]",
            "fail_fast = true",
            "random_seed = 0",
        ]
    )
    return "\n".join(sections).strip() + "\n"


def _build_plugins_toml() -> str:
    return (
        "# Add plugin entries using [[plugins]] tables.\n"
        "# Example:\n"
        "# [[plugins]]\n"
        '# name = "label_note"\n'
        '# kind = "label"\n'
        '# callable = "my_pkg.plugins:label_note"\n'
        "# enabled = true\n"
        "# order = 1\n"
    )


def handle_init(args: argparse.Namespace) -> CommandResult:
    input_path = Path(args.input_parquet).expanduser().resolve()
    if not input_path.exists():
        raise ConfigValidationError(f"input parquet does not exist: '{input_path}'")
    if args.sample_rows < 1:
        raise ConfigValidationError("sample-rows must be >= 1")

    pipeline_path = Path(args.pipeline_config).expanduser().resolve()
    plugins_path = Path(args.plugins_manifest).expanduser().resolve()
    output_path = Path(args.output_parquet).expanduser().resolve()

    for path in (pipeline_path, plugins_path):
        if path.exists() and not args.force:
            raise ConfigValidationError(
                f"target file already exists: '{path}'. Re-run with --force to overwrite."
            )

    sample = read_parquet(input_path).head(args.sample_rows)
    inferred = infer_source_adapter(sample, sample_rows=args.sample_rows)
    adapter_fragment = inferred.toml_fragment if inferred.field_suggestions > 0 else None

    pipeline_path.parent.mkdir(parents=True, exist_ok=True)
    plugins_path.parent.mkdir(parents=True, exist_ok=True)
    pipeline_path.write_text(
        _build_pipeline_toml(
            input_path=input_path,
            output_path=output_path,
            adapter_fragment=adapter_fragment,
        ),
        encoding="utf-8",
    )
    plugins_path.write_text(_build_plugins_toml(), encoding="utf-8")

    return CommandResult(
        payload={
            "input_parquet": str(input_path),
            "pipeline_config": str(pipeline_path),
            "plugins_manifest": str(plugins_path),
            "output_parquet": str(output_path),
            "adapter_field_suggestions": inferred.field_suggestions,
            "overwrite": bool(args.force),
        }
    )


def _append_check(
    checks: list[dict[str, str]],
    *,
    check_id: str,
    status: str,
    message: str,
    fix: str,
) -> None:
    checks.append(
        {
            "id": check_id,
            "status": status,
            "message": message,
            "fix": fix,
        }
    )


def _nearest_existing_parent(path: Path) -> Path:
    current = path
    while not current.exists() and current != current.parent:
        current = current.parent
    return current


def _doctor_status_summary(checks: list[dict[str, str]]) -> tuple[str, dict[str, int]]:
    summary = {"pass": 0, "warn": 0, "fail": 0}
    for item in checks:
        state = str(item["status"])
        summary[state] = summary.get(state, 0) + 1
    if summary["fail"] > 0:
        return "fail", summary
    if summary["warn"] > 0:
        return "warn", summary
    return "pass", summary


def handle_doctor(args: argparse.Namespace) -> CommandResult:
    if args.sample_rows < 1:
        raise ConfigValidationError("sample-rows must be >= 1")

    checks: list[dict[str, str]] = []
    has_config_input_error = False
    _append_check(
        checks,
        check_id="python_version",
        status="pass" if sys.version_info >= (3, 11) else "fail",
        message=f"Python {sys.version_info.major}.{sys.version_info.minor}.{sys.version_info.micro}",
        fix="Use Python >= 3.11",
    )

    missing_imports: list[str] = []
    for module in ("polars", "pydantic"):
        try:
            __import__(module)
        except Exception:
            missing_imports.append(module)
    if missing_imports:
        _append_check(
            checks,
            check_id="required_imports",
            status="fail",
            message=f"Missing imports: {', '.join(sorted(missing_imports))}",
            fix="Install package dependencies (e.g. pip install honestroles)",
        )
    else:
        _append_check(
            checks,
            check_id="required_imports",
            status="pass",
            message="Required runtime imports are available",
            fix="-",
        )

    cfg = None
    try:
        cfg = load_pipeline_config(args.pipeline_config)
    except ConfigValidationError as exc:
        has_config_input_error = True
        _append_check(
            checks,
            check_id="pipeline_config",
            status="fail",
            message=str(exc),
            fix="Run: honestroles config validate --pipeline <pipeline.toml>",
        )
    else:
        _append_check(
            checks,
            check_id="pipeline_config",
            status="pass",
            message=f"Loaded pipeline config: {cfg.input.path}",
            fix="-",
        )

    if args.plugin_manifest:
        try:
            load_plugin_manifest(args.plugin_manifest)
        except ConfigValidationError as exc:
            has_config_input_error = True
            _append_check(
                checks,
                check_id="plugin_manifest",
                status="fail",
                message=str(exc),
                fix="Run: honestroles plugins validate --manifest <plugins.toml>",
            )
        else:
            _append_check(
                checks,
                check_id="plugin_manifest",
                status="pass",
                message="Plugin manifest is valid",
                fix="-",
            )

    if cfg is not None:
        input_path = cfg.input.path
        if not input_path.exists():
            _append_check(
                checks,
                check_id="input_exists",
                status="fail",
                message=f"Input parquet missing: {input_path}",
                fix="Set [input].path to an existing parquet file",
            )
        else:
            _append_check(
                checks,
                check_id="input_exists",
                status="pass",
                message=f"Input parquet exists: {input_path}",
                fix="-",
            )
            sample = read_parquet(input_path).head(args.sample_rows)
            _append_check(
                checks,
                check_id="input_sample_read",
                status="pass",
                message=f"Read sample rows: {sample.height}",
                fix="-",
            )
            try:
                adapted, _ = apply_source_adapter(sample, cfg.input.adapter)
                aliased, _ = resolve_source_aliases(adapted, cfg.input.aliases)
                normalized = normalize_source_data_contract(aliased)
                validate_source_data_contract(normalized)
            except ConfigValidationError as exc:
                _append_check(
                    checks,
                    check_id="canonical_contract",
                    status="fail",
                    message=str(exc),
                    fix="Update input aliases/adapter mappings to populate canonical fields",
                )
            else:
                _append_check(
                    checks,
                    check_id="canonical_contract",
                    status="pass",
                    message="Canonical contract validation passed",
                    fix="-",
                )

                if normalized.height == 0:
                    _append_check(
                        checks,
                        check_id="content_readiness",
                        status="warn",
                        message="Input sample is empty",
                        fix="Verify source extraction returns rows",
                    )
                elif normalized["title"].null_count() == normalized.height:
                    _append_check(
                        checks,
                        check_id="content_readiness",
                        status="warn",
                        message="All sampled rows have null title",
                        fix="Map title via [input.aliases] or [input.adapter]",
                    )
                else:
                    _append_check(
                        checks,
                        check_id="content_readiness",
                        status="pass",
                        message="Sample contains required content signals",
                        fix="-",
                    )

        if cfg.output is None:
            _append_check(
                checks,
                check_id="output_path",
                status="warn",
                message="No [output] path configured",
                fix="Add [output].path to persist pipeline results",
            )
        else:
            parent = cfg.output.path.parent
            if parent.exists():
                writable = parent.is_dir() and os.access(parent, os.W_OK)
                _append_check(
                    checks,
                    check_id="output_path",
                    status="pass" if writable else "fail",
                    message=f"Output parent directory: {parent}",
                    fix="Ensure output directory exists and is writable",
                )
            else:
                ancestor = _nearest_existing_parent(parent)
                _append_check(
                    checks,
                    check_id="output_path",
                    status="warn" if ancestor.exists() else "fail",
                    message=f"Output parent directory does not exist: {parent}",
                    fix=f"Create directory '{parent}' before running pipeline",
                )

    status, summary = _doctor_status_summary(checks)
    return CommandResult(
        payload={
            "status": status,
            "pipeline_config": str(Path(args.pipeline_config).expanduser().resolve()),
            "plugin_manifest": (
                str(Path(args.plugin_manifest).expanduser().resolve())
                if args.plugin_manifest
                else None
            ),
            "summary": summary,
            "checks": checks,
        },
        exit_code=(
            _EXIT_CONFIG
            if has_config_input_error
            else (_EXIT_GENERIC if status == "fail" else _EXIT_OK)
        ),
    )


def handle_run(args: argparse.Namespace) -> CommandResult:
    runtime = HonestRolesRuntime.from_configs(args.pipeline_config, args.plugin_manifest)
    result = runtime.run()
    return CommandResult(payload=result.diagnostics.to_dict())


def handle_plugins_validate(args: argparse.Namespace) -> CommandResult:
    registry = PluginRegistry.from_manifest(args.manifest)
    payload = {
        "manifest": str(Path(args.manifest).expanduser().resolve()),
        "plugins": {
            "filter": list(registry.list("filter")),
            "label": list(registry.list("label")),
            "rate": list(registry.list("rate")),
        },
    }
    return CommandResult(payload=payload)


def handle_config_validate(args: argparse.Namespace) -> CommandResult:
    cfg = load_pipeline_config(args.pipeline)
    return CommandResult(payload=cfg.model_dump(mode="json"))


def handle_report_quality(args: argparse.Namespace) -> CommandResult:
    runtime = HonestRolesRuntime.from_configs(args.pipeline_config, args.plugin_manifest)
    result = runtime.run()
    report = build_data_quality_report(
        result.dataset,
        quality=runtime.pipeline_spec.runtime.quality,
    )
    return CommandResult(
        payload={
            "row_count": report.row_count,
            "score_percent": report.score_percent,
            "null_percentages": report.null_percentages,
            "profile": report.profile,
            "weighted_null_percent": report.weighted_null_percent,
            "effective_weights": report.effective_weights,
        }
    )


def handle_eda_generate(args: argparse.Namespace) -> CommandResult:
    manifest = generate_eda_artifacts(
        input_parquet=args.input_parquet,
        output_dir=args.output_dir,
        quality_profile=args.quality_profile,
        field_weights=parse_quality_weight_overrides(args.quality_weight),
        top_k=args.top_k,
        max_rows=args.max_rows,
        rules_file=args.rules_file,
    )
    artifacts_dir = Path(args.output_dir).expanduser().resolve()
    return CommandResult(
        payload={
            "artifacts_dir": str(artifacts_dir),
            "manifest": str(artifacts_dir / "manifest.json"),
            "summary": str(artifacts_dir / manifest.files["summary_json"]),
            "report": str(artifacts_dir / manifest.files["report_md"]),
        }
    )


def handle_eda_diff(args: argparse.Namespace) -> CommandResult:
    manifest = generate_eda_diff_artifacts(
        baseline_dir=args.baseline_dir,
        candidate_dir=args.candidate_dir,
        output_dir=args.output_dir,
        rules_file=args.rules_file,
    )
    artifacts_dir = Path(args.output_dir).expanduser().resolve()
    return CommandResult(
        payload={
            "diff_dir": str(artifacts_dir),
            "manifest": str(artifacts_dir / "manifest.json"),
            "diff_json": str(artifacts_dir / manifest.files["diff_json"]),
        }
    )


def handle_eda_gate(args: argparse.Namespace) -> CommandResult:
    candidate_bundle = load_eda_artifacts(args.candidate_dir)
    if candidate_bundle.summary is None:
        raise ConfigValidationError(
            "eda gate --candidate-dir must reference profile artifacts"
        )
    rules = load_eda_rules(
        rules_file=args.rules_file,
        fail_on=args.fail_on,
        warn_on=args.warn_on,
    )

    if args.baseline_dir:
        diff_payload, _, _, _ = build_eda_diff(
            baseline_dir=args.baseline_dir,
            candidate_dir=args.candidate_dir,
            rules=rules,
        )
        gate_payload = diff_payload["gate_evaluation"]
    else:
        gate_payload = evaluate_eda_gate(
            candidate_summary=candidate_bundle.summary,
            rules=rules,
            diff_payload=None,
        )

    return CommandResult(
        payload=gate_payload,
        exit_code=_EXIT_OK if gate_payload["status"] == "pass" else _EXIT_GENERIC,
    )


def handle_adapter_infer(args: argparse.Namespace) -> CommandResult:
    input_path = Path(args.input_parquet).expanduser().resolve()
    if not input_path.exists():
        raise ConfigValidationError(f"input parquet does not exist: '{input_path}'")

    if args.sample_rows < 1:
        raise ConfigValidationError("sample-rows must be >= 1")
    if args.top_candidates < 1:
        raise ConfigValidationError("top-candidates must be >= 1")
    if not (0.0 <= args.min_confidence <= 1.0):
        raise ConfigValidationError("min-confidence must be between 0 and 1")

    output_file = Path(args.output_file).expanduser().resolve()
    report_file = output_file.with_suffix(".report.json")

    df = read_parquet(input_path)
    inferred = infer_source_adapter(
        df,
        sample_rows=args.sample_rows,
        top_candidates=args.top_candidates,
        min_confidence=args.min_confidence,
    )

    output_file.parent.mkdir(parents=True, exist_ok=True)
    output_file.write_text(inferred.toml_fragment, encoding="utf-8")
    report_file.write_text(
        json.dumps(inferred.report, indent=2, sort_keys=True),
        encoding="utf-8",
    )

    if args.print_fragment:
        print(inferred.toml_fragment.strip())

    return CommandResult(
        payload={
            "input_parquet": str(input_path),
            "adapter_draft": str(output_file),
            "inference_report": str(report_file),
            "field_suggestions": inferred.field_suggestions,
        }
    )


def replace_text(path: Path, needle: str, replacement: str) -> None:
    content = path.read_text(encoding="utf-8")
    path.write_text(content.replace(needle, replacement), encoding="utf-8")


def resolve_plugin_template_root() -> Path:
    repo_template = Path(__file__).resolve().parents[3] / "plugin_template"
    if repo_template.exists():
        return repo_template
    packaged_template = (
        Path(__file__).resolve().parents[1] / "_templates" / "plugin_template"
    )
    if packaged_template.exists():
        return packaged_template
    raise ConfigValidationError(
        f"plugin template directory does not exist: '{repo_template}'"
    )


def handle_scaffold_plugin(args: argparse.Namespace) -> CommandResult:
    template_root = resolve_plugin_template_root()

    output_dir = Path(args.output_dir).expanduser().resolve()
    target = output_dir / args.name
    if target.exists():
        raise ConfigValidationError(f"target scaffold path already exists: '{target}'")

    package_name = args.name.strip().lower().replace("-", "_")
    if not package_name or not package_name.replace("_", "").isalnum():
        raise ConfigValidationError(
            "plugin name must produce a valid package name using letters/numbers/underscores"
        )
    if package_name[0].isdigit():
        raise ConfigValidationError("plugin package name cannot start with a digit")

    shutil.copytree(template_root, target)

    src_root = target / "src"
    default_pkg = src_root / "honestroles_plugin_example"
    desired_pkg = src_root / package_name
    if default_pkg.exists() and desired_pkg != default_pkg:
        default_pkg.rename(desired_pkg)

    for path in target.rglob("*"):
        if path.is_file() and path.suffix in {".py", ".toml", ".md"}:
            replace_text(path, "honestroles_plugin_example", package_name)
            replace_text(path, "honestroles-plugin-example", args.name)

    return CommandResult(
        payload={
            "scaffold_path": str(target),
            "package_name": package_name,
        }
    )


def handle_runs_list(args: argparse.Namespace) -> CommandResult:
    if args.limit < 1:
        raise ConfigValidationError("limit must be >= 1")
    runs = list_records(limit=args.limit, status=args.status)
    return CommandResult(payload={"runs": runs, "count": len(runs)})


def handle_runs_show(args: argparse.Namespace) -> CommandResult:
    try:
        payload = load_record(args.run_id)
    except OSError as exc:
        raise ConfigValidationError(f"run record not found: '{args.run_id}'") from exc
    return CommandResult(payload=payload)
