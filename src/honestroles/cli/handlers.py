from __future__ import annotations

import argparse
import json
import shutil
from pathlib import Path

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
from honestroles.io import build_data_quality_report
from honestroles.io import infer_source_adapter, read_parquet
from honestroles.plugins.registry import PluginRegistry
from honestroles.runtime import HonestRolesRuntime

_EXIT_OK = 0
_EXIT_GENERIC = 1


def handle_run(args: argparse.Namespace) -> int:
    runtime = HonestRolesRuntime.from_configs(args.pipeline_config, args.plugin_manifest)
    result = runtime.run()
    print(json.dumps(result.diagnostics.to_dict(), indent=2, sort_keys=True))
    return _EXIT_OK


def handle_plugins_validate(args: argparse.Namespace) -> int:
    registry = PluginRegistry.from_manifest(args.manifest)
    payload = {
        "manifest": str(Path(args.manifest).expanduser().resolve()),
        "plugins": {
            "filter": list(registry.list("filter")),
            "label": list(registry.list("label")),
            "rate": list(registry.list("rate")),
        },
    }
    print(json.dumps(payload, indent=2, sort_keys=True))
    return _EXIT_OK


def handle_config_validate(args: argparse.Namespace) -> int:
    cfg = load_pipeline_config(args.pipeline)
    print(json.dumps(cfg.model_dump(mode="json"), indent=2, sort_keys=True))
    return _EXIT_OK


def handle_report_quality(args: argparse.Namespace) -> int:
    runtime = HonestRolesRuntime.from_configs(args.pipeline_config, args.plugin_manifest)
    result = runtime.run()
    report = build_data_quality_report(
        result.dataset,
        quality=runtime.pipeline_spec.runtime.quality,
    )
    print(
        json.dumps(
            {
                "row_count": report.row_count,
                "score_percent": report.score_percent,
                "null_percentages": report.null_percentages,
                "profile": report.profile,
                "weighted_null_percent": report.weighted_null_percent,
                "effective_weights": report.effective_weights,
            },
            indent=2,
            sort_keys=True,
        )
    )
    return _EXIT_OK


def handle_eda_generate(args: argparse.Namespace) -> int:
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
    print(
        json.dumps(
            {
                "artifacts_dir": str(artifacts_dir),
                "manifest": str(artifacts_dir / "manifest.json"),
                "summary": str(artifacts_dir / manifest.files["summary_json"]),
                "report": str(artifacts_dir / manifest.files["report_md"]),
            },
            indent=2,
            sort_keys=True,
        )
    )
    return _EXIT_OK


def handle_eda_diff(args: argparse.Namespace) -> int:
    manifest = generate_eda_diff_artifacts(
        baseline_dir=args.baseline_dir,
        candidate_dir=args.candidate_dir,
        output_dir=args.output_dir,
        rules_file=args.rules_file,
    )
    artifacts_dir = Path(args.output_dir).expanduser().resolve()
    print(
        json.dumps(
            {
                "diff_dir": str(artifacts_dir),
                "manifest": str(artifacts_dir / "manifest.json"),
                "diff_json": str(artifacts_dir / manifest.files["diff_json"]),
            },
            indent=2,
            sort_keys=True,
        )
    )
    return _EXIT_OK


def handle_eda_gate(args: argparse.Namespace) -> int:
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

    print(json.dumps(gate_payload, indent=2, sort_keys=True))
    return _EXIT_OK if gate_payload["status"] == "pass" else _EXIT_GENERIC


def handle_adapter_infer(args: argparse.Namespace) -> int:
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

    print(
        json.dumps(
            {
                "input_parquet": str(input_path),
                "adapter_draft": str(output_file),
                "inference_report": str(report_file),
                "field_suggestions": inferred.field_suggestions,
            },
            indent=2,
            sort_keys=True,
        )
    )
    return _EXIT_OK


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


def handle_scaffold_plugin(args: argparse.Namespace) -> int:
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

    print(
        json.dumps(
            {
                "scaffold_path": str(target),
                "package_name": package_name,
            },
            indent=2,
            sort_keys=True,
        )
    )
    return _EXIT_OK
