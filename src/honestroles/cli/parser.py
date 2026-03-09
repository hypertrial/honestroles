from __future__ import annotations

import argparse


def _add_format_arg(parser: argparse.ArgumentParser) -> None:
    parser.add_argument(
        "--format",
        choices=["json", "table"],
        default="json",
        help="Output format",
    )


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="honestroles")
    sub = parser.add_subparsers(dest="command", required=True)

    run_parser = sub.add_parser("run", help="Run pipeline from TOML config")
    run_parser.add_argument("--pipeline-config", required=True)
    run_parser.add_argument("--plugins", dest="plugin_manifest", required=False)
    _add_format_arg(run_parser)

    plugins_parser = sub.add_parser("plugins", help="Plugin manifest operations")
    plugins_sub = plugins_parser.add_subparsers(dest="plugins_command", required=True)
    plugins_validate = plugins_sub.add_parser("validate", help="Validate plugin manifest")
    plugins_validate.add_argument("--manifest", required=True)
    _add_format_arg(plugins_validate)

    config_parser = sub.add_parser("config", help="Pipeline config operations")
    config_sub = config_parser.add_subparsers(dest="config_command", required=True)
    config_validate = config_sub.add_parser("validate", help="Validate pipeline config")
    config_validate.add_argument("--pipeline", required=True)
    _add_format_arg(config_validate)

    report_parser = sub.add_parser(
        "report-quality",
        help="Run pipeline and emit data quality report",
    )
    report_parser.add_argument("--pipeline-config", required=True)
    report_parser.add_argument("--plugins", dest="plugin_manifest", required=False)
    _add_format_arg(report_parser)

    init_parser = sub.add_parser("init", help="Scaffold pipeline and plugin manifest")
    init_parser.add_argument("--input-parquet", required=True)
    init_parser.add_argument("--pipeline-config", default="pipeline.toml")
    init_parser.add_argument("--plugins-manifest", default="plugins.toml")
    init_parser.add_argument("--output-parquet", default="dist/jobs_scored.parquet")
    init_parser.add_argument("--sample-rows", type=int, default=50000)
    init_parser.add_argument("--force", action="store_true")
    _add_format_arg(init_parser)

    doctor_parser = sub.add_parser("doctor", help="Validate environment and pipeline readiness")
    doctor_parser.add_argument("--pipeline-config", required=True)
    doctor_parser.add_argument("--plugins", dest="plugin_manifest", required=False)
    doctor_parser.add_argument("--sample-rows", type=int, default=1000)
    doctor_parser.add_argument("--policy", dest="policy_file", default=None)
    doctor_parser.add_argument("--strict", action="store_true")
    _add_format_arg(doctor_parser)

    reliability_parser = sub.add_parser("reliability", help="Reliability policy operations")
    reliability_sub = reliability_parser.add_subparsers(
        dest="reliability_command", required=True
    )
    reliability_check = reliability_sub.add_parser(
        "check",
        help="Run policy-aware reliability checks and write gate artifact",
    )
    reliability_check.add_argument("--pipeline-config", required=True)
    reliability_check.add_argument("--plugins", dest="plugin_manifest", required=False)
    reliability_check.add_argument("--sample-rows", type=int, default=1000)
    reliability_check.add_argument("--policy", dest="policy_file", default=None)
    reliability_check.add_argument(
        "--output-file", default="dist/reliability/latest/gate_result.json"
    )
    reliability_check.add_argument("--strict", action="store_true")
    _add_format_arg(reliability_check)

    scaffold_parser = sub.add_parser(
        "scaffold-plugin",
        help="Scaffold a plugin package from the bundled template",
    )
    scaffold_parser.add_argument("--name", required=True)
    scaffold_parser.add_argument("--output-dir", default=".")
    _add_format_arg(scaffold_parser)

    adapter_parser = sub.add_parser(
        "adapter", help="Source adapter utilities"
    )
    adapter_sub = adapter_parser.add_subparsers(dest="adapter_command", required=True)
    adapter_infer = adapter_sub.add_parser(
        "infer",
        help="Infer a draft [input.adapter] fragment from parquet input",
    )
    adapter_infer.add_argument("--input-parquet", required=True)
    adapter_infer.add_argument("--output-file", default="dist/adapters/adapter-draft.toml")
    adapter_infer.add_argument("--sample-rows", type=int, default=50000)
    adapter_infer.add_argument("--top-candidates", type=int, default=3)
    adapter_infer.add_argument("--min-confidence", type=float, default=0.55)
    adapter_infer.add_argument("--print", dest="print_fragment", action="store_true")
    _add_format_arg(adapter_infer)

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
    _add_format_arg(eda_generate)

    eda_diff = eda_sub.add_parser(
        "diff",
        help="Compare baseline and candidate EDA artifacts and emit diff artifacts",
    )
    eda_diff.add_argument("--baseline-dir", required=True)
    eda_diff.add_argument("--candidate-dir", required=True)
    eda_diff.add_argument("--output-dir", default="dist/eda/diff")
    eda_diff.add_argument("--rules-file", default=None)
    _add_format_arg(eda_diff)

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
    _add_format_arg(eda_gate)

    runs_parser = sub.add_parser("runs", help="Inspect run lineage records")
    runs_sub = runs_parser.add_subparsers(dest="runs_command", required=True)
    runs_list = runs_sub.add_parser("list", help="List recorded runs")
    runs_list.add_argument("--limit", type=int, default=20)
    runs_list.add_argument("--status", choices=["pass", "fail"], default=None)
    runs_list.add_argument("--command", dest="command_filter", default=None)
    runs_list.add_argument("--since", default=None)
    runs_list.add_argument("--contains-code", dest="contains_code", default=None)
    _add_format_arg(runs_list)

    runs_show = runs_sub.add_parser("show", help="Show one run by ID")
    runs_show.add_argument("--run-id", required=True)
    _add_format_arg(runs_show)

    return parser
