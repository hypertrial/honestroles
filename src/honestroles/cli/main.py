from __future__ import annotations

import argparse
import importlib.util
import json
import subprocess
import shutil
import sys
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
from honestroles.errors import ConfigValidationError, HonestRolesError, StageExecutionError
from honestroles.io import build_data_quality_report
from honestroles.plugins.errors import (
    PluginExecutionError,
    PluginLoadError,
    PluginValidationError,
)
from honestroles.plugins.registry import PluginRegistry
from honestroles.runtime import HonestRolesRuntime

_EXIT_OK = 0
_EXIT_CONFIG = 2
_EXIT_PLUGIN = 3
_EXIT_STAGE = 4
_EXIT_GENERIC = 1


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="honestroles")
    sub = parser.add_subparsers(dest="command", required=True)

    run_parser = sub.add_parser("run", help="Run pipeline from TOML config")
    run_parser.add_argument("--pipeline-config", required=True)
    run_parser.add_argument("--plugins", dest="plugin_manifest", required=False)

    plugins_parser = sub.add_parser("plugins", help="Plugin manifest operations")
    plugins_sub = plugins_parser.add_subparsers(dest="plugins_command", required=True)
    plugins_validate = plugins_sub.add_parser("validate", help="Validate plugin manifest")
    plugins_validate.add_argument("--manifest", required=True)

    config_parser = sub.add_parser("config", help="Pipeline config operations")
    config_sub = config_parser.add_subparsers(dest="config_command", required=True)
    config_validate = config_sub.add_parser("validate", help="Validate pipeline config")
    config_validate.add_argument("--pipeline", required=True)

    report_parser = sub.add_parser(
        "report-quality",
        help="Run pipeline and emit data quality report",
    )
    report_parser.add_argument("--pipeline-config", required=True)
    report_parser.add_argument("--plugins", dest="plugin_manifest", required=False)

    scaffold_parser = sub.add_parser(
        "scaffold-plugin",
        help="Scaffold a plugin package from the bundled template",
    )
    scaffold_parser.add_argument("--name", required=True)
    scaffold_parser.add_argument("--output-dir", default=".")

    eda_parser = sub.add_parser("eda", help="EDA artifact generation and dashboard")
    eda_sub = eda_parser.add_subparsers(dest="eda_command", required=True)

    eda_generate = eda_sub.add_parser(
        "generate",
        help="Generate deterministic EDA artifacts from a parquet input",
    )
    eda_generate.add_argument("--input-parquet", required=True)
    eda_generate.add_argument("--output-dir", default="dist/eda/latest")
    eda_generate.add_argument(
        "--quality-profile",
        default="core_fields_weighted",
        choices=["core_fields_weighted", "equal_weight_all", "strict_recruiting"],
    )
    eda_generate.add_argument("--quality-weight", action="append", default=[])
    eda_generate.add_argument("--top-k", type=int, default=10)
    eda_generate.add_argument("--max-rows", type=int, default=None)
    eda_generate.add_argument("--rules-file", default=None)

    eda_diff = eda_sub.add_parser(
        "diff",
        help="Compare baseline and candidate EDA artifacts and emit diff artifacts",
    )
    eda_diff.add_argument("--baseline-dir", required=True)
    eda_diff.add_argument("--candidate-dir", required=True)
    eda_diff.add_argument("--output-dir", default="dist/eda/diff")
    eda_diff.add_argument("--rules-file", default=None)

    eda_dashboard = eda_sub.add_parser(
        "dashboard",
        help="Launch Streamlit dashboard for previously generated EDA artifacts",
    )
    eda_dashboard.add_argument("--artifacts-dir", required=True)
    eda_dashboard.add_argument("--diff-dir", default=None)
    eda_dashboard.add_argument("--host", default="127.0.0.1")
    eda_dashboard.add_argument("--port", type=int, default=8501)

    eda_gate = eda_sub.add_parser(
        "gate",
        help="Evaluate EDA gate policies for candidate artifacts",
    )
    eda_gate.add_argument("--candidate-dir", required=True)
    eda_gate.add_argument("--baseline-dir", default=None)
    eda_gate.add_argument("--rules-file", default=None)
    eda_gate.add_argument("--fail-on", default=None)
    eda_gate.add_argument("--warn-on", default=None)

    return parser


def _handle_run(args: argparse.Namespace) -> int:
    runtime = HonestRolesRuntime.from_configs(args.pipeline_config, args.plugin_manifest)
    result = runtime.run()
    print(json.dumps(result.diagnostics, indent=2, sort_keys=True))
    return _EXIT_OK


def _handle_plugins_validate(args: argparse.Namespace) -> int:
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


def _handle_config_validate(args: argparse.Namespace) -> int:
    cfg = load_pipeline_config(args.pipeline)
    print(json.dumps(cfg.model_dump(mode="json"), indent=2, sort_keys=True))
    return _EXIT_OK


def _handle_report_quality(args: argparse.Namespace) -> int:
    runtime = HonestRolesRuntime.from_configs(args.pipeline_config, args.plugin_manifest)
    result = runtime.run()
    report = build_data_quality_report(
        result.dataframe, quality=runtime.pipeline_config.runtime.quality
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


def _handle_eda_generate(args: argparse.Namespace) -> int:
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


def _handle_eda_diff(args: argparse.Namespace) -> int:
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


def _handle_eda_dashboard(args: argparse.Namespace) -> int:
    bundle = load_eda_artifacts(args.artifacts_dir)
    if bundle.summary is None:
        raise ConfigValidationError(
            "eda dashboard requires a profile artifacts directory for --artifacts-dir"
        )
    diff_bundle = None
    if args.diff_dir is not None:
        diff_bundle = load_eda_artifacts(args.diff_dir)
        if diff_bundle.diff is None:
            raise ConfigValidationError(
                "eda dashboard --diff-dir must reference diff artifacts"
            )
    if importlib.util.find_spec("streamlit") is None:
        raise ConfigValidationError(
            "streamlit is required for 'honestroles eda dashboard'; install with "
            "\"pip install 'honestroles[eda]'\""
        )

    app_path = Path(__file__).resolve().parents[1] / "eda" / "dashboard_app.py"
    cmd = [
        sys.executable,
        "-m",
        "streamlit",
        "run",
        str(app_path),
        "--server.address",
        args.host,
        "--server.port",
        str(args.port),
        "--",
        "--artifacts-dir",
        str(bundle.artifacts_dir),
    ]
    if diff_bundle is not None:
        cmd.extend(["--diff-dir", str(diff_bundle.artifacts_dir)])
    completed = subprocess.run(cmd, check=False)
    return _EXIT_OK if completed.returncode == 0 else _EXIT_GENERIC


def _handle_eda_gate(args: argparse.Namespace) -> int:
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


def _replace_text(path: Path, needle: str, replacement: str) -> None:
    content = path.read_text(encoding="utf-8")
    path.write_text(content.replace(needle, replacement), encoding="utf-8")


def _resolve_plugin_template_root() -> Path:
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


def _handle_scaffold_plugin(args: argparse.Namespace) -> int:
    template_root = _resolve_plugin_template_root()

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
            _replace_text(path, "honestroles_plugin_example", package_name)
            _replace_text(path, "honestroles-plugin-example", args.name)

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


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    try:
        if args.command == "run":
            return _handle_run(args)
        if args.command == "plugins" and args.plugins_command == "validate":
            return _handle_plugins_validate(args)
        if args.command == "config" and args.config_command == "validate":
            return _handle_config_validate(args)
        if args.command == "report-quality":
            return _handle_report_quality(args)
        if args.command == "scaffold-plugin":
            return _handle_scaffold_plugin(args)
        if args.command == "eda" and args.eda_command == "generate":
            return _handle_eda_generate(args)
        if args.command == "eda" and args.eda_command == "diff":
            return _handle_eda_diff(args)
        if args.command == "eda" and args.eda_command == "dashboard":
            return _handle_eda_dashboard(args)
        if args.command == "eda" and args.eda_command == "gate":
            return _handle_eda_gate(args)
    except ConfigValidationError as exc:
        print(str(exc), file=sys.stderr)
        return _EXIT_CONFIG
    except (PluginLoadError, PluginValidationError, PluginExecutionError) as exc:
        print(str(exc), file=sys.stderr)
        return _EXIT_PLUGIN
    except StageExecutionError as exc:
        print(str(exc), file=sys.stderr)
        return _EXIT_STAGE
    except HonestRolesError as exc:
        print(str(exc), file=sys.stderr)
        return _EXIT_GENERIC

    parser.error("unhandled command")
    return _EXIT_GENERIC


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
