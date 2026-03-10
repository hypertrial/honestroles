from __future__ import annotations

import argparse
from dataclasses import dataclass
from datetime import UTC, datetime
import json
from pathlib import Path
import shutil
from typing import Any

from honestroles.config import load_pipeline_config
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
    validate_source_data_contract,
    build_data_quality_report,
    infer_source_adapter,
    read_parquet,
)
from honestroles.ingest import (
    sync_source,
    sync_sources_from_manifest,
    validate_ingestion_source,
)
from honestroles.plugins.registry import PluginRegistry
from honestroles.recommend import (
    build_retrieval_index,
    evaluate_relevance,
    match_jobs,
    record_feedback_event,
    summarize_feedback,
)
from honestroles.reliability import evaluate_reliability
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


def _reliability_exit_code(
    *,
    status: str,
    strict: bool,
    has_config_input_error: bool,
) -> int:
    if has_config_input_error:
        return _EXIT_CONFIG
    if status == "fail":
        return _EXIT_GENERIC
    if strict and status == "warn":
        return _EXIT_GENERIC
    return _EXIT_OK


def _nearest_existing_parent(path: Path) -> Path:
    current = path
    while not current.exists() and current != current.parent:
        current = current.parent
    return current


def _doctor_status_summary(checks: list[dict[str, Any]]) -> tuple[str, dict[str, int]]:
    summary = {"pass": 0, "warn": 0, "fail": 0}
    for item in checks:
        state = str(item.get("status", ""))
        if state not in summary:
            continue
        summary[state] += 1
    if summary["fail"] > 0:
        return "fail", summary
    if summary["warn"] > 0:
        return "warn", summary
    return "pass", summary


def _reliability_payload(
    *,
    evaluation,
    pipeline_config: str,
    plugin_manifest: str | None,
    strict: bool,
) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "status": evaluation.status,
        "pipeline_config": str(Path(pipeline_config).expanduser().resolve()),
        "plugin_manifest": (
            str(Path(plugin_manifest).expanduser().resolve())
            if plugin_manifest
            else None
        ),
        "summary": evaluation.summary,
        "checks": evaluation.checks,
        "check_codes": evaluation.check_codes,
        "policy_source": evaluation.policy_source,
        "policy_hash": evaluation.policy_hash,
        "strict": bool(strict),
    }
    if strict and evaluation.status == "warn":
        payload["strict_escalated"] = True
    return payload


def handle_doctor(args: argparse.Namespace) -> CommandResult:
    strict = bool(getattr(args, "strict", False))
    evaluation = evaluate_reliability(
        pipeline_config=args.pipeline_config,
        plugin_manifest=args.plugin_manifest,
        sample_rows=args.sample_rows,
        policy_file=getattr(args, "policy_file", None),
        validate_source_data_contract_fn=validate_source_data_contract,
    )
    payload = _reliability_payload(
        evaluation=evaluation,
        pipeline_config=args.pipeline_config,
        plugin_manifest=args.plugin_manifest,
        strict=strict,
    )
    exit_code = _reliability_exit_code(
        status=evaluation.status,
        strict=strict,
        has_config_input_error=evaluation.has_config_input_error,
    )
    return CommandResult(
        payload=payload,
        exit_code=exit_code,
    )


def handle_reliability_check(args: argparse.Namespace) -> CommandResult:
    strict = bool(getattr(args, "strict", False))
    output_file = Path(args.output_file).expanduser().resolve()
    output_file.parent.mkdir(parents=True, exist_ok=True)
    try:
        evaluation = evaluate_reliability(
            pipeline_config=args.pipeline_config,
            plugin_manifest=args.plugin_manifest,
            sample_rows=args.sample_rows,
            policy_file=getattr(args, "policy_file", None),
            validate_source_data_contract_fn=validate_source_data_contract,
        )
    except ConfigValidationError as exc:
        failure_payload = {
            "status": "fail",
            "pipeline_config": str(Path(args.pipeline_config).expanduser().resolve()),
            "plugin_manifest": (
                str(Path(args.plugin_manifest).expanduser().resolve())
                if args.plugin_manifest
                else None
            ),
            "summary": {"pass": 0, "warn": 0, "fail": 1},
            "checks": [],
            "check_codes": [],
            "policy_source": "unresolved",
            "policy_hash": None,
            "strict": strict,
            "error": {
                "type": exc.__class__.__name__,
                "message": str(exc),
            },
        }
        output_file.write_text(
            json.dumps(failure_payload, indent=2, sort_keys=True),
            encoding="utf-8",
        )
        raise

    payload = _reliability_payload(
        evaluation=evaluation,
        pipeline_config=args.pipeline_config,
        plugin_manifest=args.plugin_manifest,
        strict=strict,
    )
    output_file.write_text(
        json.dumps(payload, indent=2, sort_keys=True),
        encoding="utf-8",
    )
    payload["reliability_artifact"] = str(output_file)
    exit_code = _reliability_exit_code(
        status=evaluation.status,
        strict=strict,
        has_config_input_error=evaluation.has_config_input_error,
    )
    return CommandResult(payload=payload, exit_code=exit_code)


def handle_ingest_sync(args: argparse.Namespace) -> CommandResult:
    result = sync_source(
        source=args.source,
        source_ref=args.source_ref,
        output_parquet=args.output_parquet,
        report_file=args.report_file,
        state_file=args.state_file,
        write_raw=bool(args.write_raw),
        max_pages=args.max_pages,
        max_jobs=args.max_jobs,
        full_refresh=bool(args.full_refresh),
        timeout_seconds=float(getattr(args, "timeout_seconds", 15.0)),
        max_retries=int(getattr(args, "max_retries", 3)),
        base_backoff_seconds=float(getattr(args, "base_backoff_seconds", 0.25)),
        user_agent=str(getattr(args, "user_agent", "honestroles-ingest/2.0")),
        quality_policy_file=getattr(args, "quality_policy_file", None),
        strict_quality=bool(getattr(args, "strict_quality", False)),
        merge_policy=str(getattr(args, "merge_policy", "updated_hash")),
        retain_snapshots=int(getattr(args, "retain_snapshots", 30)),
        prune_inactive_days=int(getattr(args, "prune_inactive_days", 90)),
    )
    exit_code = 0 if result.report.status in {"pass", "warn"} else 1
    return CommandResult(payload=result.to_payload(), exit_code=exit_code)


def handle_ingest_sync_all(args: argparse.Namespace) -> CommandResult:
    result = sync_sources_from_manifest(
        manifest_path=args.manifest,
        report_file=args.report_file,
        fail_fast=bool(args.fail_fast),
    )
    exit_code = 0 if result.status == "pass" else 1
    return CommandResult(payload=result.to_payload(), exit_code=exit_code)


def handle_ingest_validate(args: argparse.Namespace) -> CommandResult:
    result = validate_ingestion_source(
        source=args.source,
        source_ref=args.source_ref,
        report_file=args.report_file,
        write_raw=bool(args.write_raw),
        max_pages=args.max_pages,
        max_jobs=args.max_jobs,
        timeout_seconds=float(getattr(args, "timeout_seconds", 15.0)),
        max_retries=int(getattr(args, "max_retries", 3)),
        base_backoff_seconds=float(getattr(args, "base_backoff_seconds", 0.25)),
        user_agent=str(getattr(args, "user_agent", "honestroles-ingest/2.0")),
        quality_policy_file=getattr(args, "quality_policy_file", None),
        strict_quality=bool(getattr(args, "strict_quality", False)),
    )
    exit_code = 0 if result.report.status in {"pass", "warn"} else 1
    return CommandResult(payload=result.to_payload(), exit_code=exit_code)


def handle_recommend_build_index(args: argparse.Namespace) -> CommandResult:
    result = build_retrieval_index(
        input_parquet=args.input_parquet,
        output_dir=args.output_dir,
        policy_file=getattr(args, "policy_file", None),
    )
    return CommandResult(payload=result.to_payload(), exit_code=0)


def handle_recommend_match(args: argparse.Namespace) -> CommandResult:
    result = match_jobs(
        index_dir=args.index_dir,
        candidate_json=getattr(args, "candidate_json", None),
        resume_text=getattr(args, "resume_text", None),
        profile_id=getattr(args, "profile_id", None),
        top_k=int(getattr(args, "top_k", 25)),
        policy_file=getattr(args, "policy_file", None),
        include_excluded=bool(getattr(args, "include_excluded", False)),
    )
    return CommandResult(payload=result.to_payload(), exit_code=0)


def handle_recommend_evaluate(args: argparse.Namespace) -> CommandResult:
    result = evaluate_relevance(
        index_dir=args.index_dir,
        golden_set=args.golden_set,
        thresholds_file=getattr(args, "thresholds_file", None),
        policy_file=getattr(args, "policy_file", None),
    )
    return CommandResult(
        payload=result.to_payload(),
        exit_code=0 if result.status == "pass" else 1,
    )


def handle_recommend_feedback_add(args: argparse.Namespace) -> CommandResult:
    result = record_feedback_event(
        profile_id=args.profile_id,
        job_id=args.job_id,
        event=args.event,
        meta_json_file=getattr(args, "meta_json_file", None),
    )
    return CommandResult(payload=result.to_payload(), exit_code=0)


def handle_recommend_feedback_summarize(args: argparse.Namespace) -> CommandResult:
    result = summarize_feedback(profile_id=getattr(args, "profile_id", None))
    return CommandResult(payload=result.to_payload(), exit_code=0)


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
    since = None
    since_arg = getattr(args, "since", None)
    if since_arg not in (None, ""):
        try:
            value = str(since_arg).strip()
            if value.endswith("Z"):
                value = value[:-1] + "+00:00"
            since = datetime.fromisoformat(value)
            if since.tzinfo is None:
                since = since.replace(tzinfo=UTC)
            else:
                since = since.astimezone(UTC)
        except ValueError as exc:
            raise ConfigValidationError("since must be ISO-8601 datetime") from exc
    runs = list_records(
        limit=args.limit,
        status=args.status,
        command=getattr(args, "command_filter", None),
        since_utc=since,
        contains_code=getattr(args, "contains_code", None),
    )
    return CommandResult(payload={"runs": runs, "count": len(runs)})


def handle_runs_show(args: argparse.Namespace) -> CommandResult:
    try:
        payload = load_record(args.run_id)
    except OSError as exc:
        raise ConfigValidationError(f"run record not found: '{args.run_id}'") from exc
    return CommandResult(payload=payload)
