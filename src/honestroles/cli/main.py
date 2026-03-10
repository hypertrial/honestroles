from __future__ import annotations

import argparse
from datetime import datetime, timezone
import importlib.util
import subprocess
import sys
from typing import Any

from honestroles.eda import load_eda_artifacts
from honestroles.errors import ConfigValidationError, HonestRolesError, StageExecutionError
from honestroles.plugins.errors import (
    PluginExecutionError,
    PluginLoadError,
    PluginValidationError,
)

from .handlers import (
    CommandResult,
    handle_adapter_infer,
    handle_config_validate,
    handle_doctor,
    handle_eda_diff,
    handle_eda_gate,
    handle_eda_generate,
    handle_init,
    handle_ingest_sync,
    handle_ingest_sync_all,
    handle_ingest_validate,
    handle_plugins_validate,
    handle_reliability_check,
    handle_report_quality,
    handle_run,
    handle_runs_list,
    handle_runs_show,
    handle_scaffold_plugin,
    resolve_plugin_template_root,
)
from .lineage import create_record, should_track, write_record
from .output import emit_error, emit_payload
from .parser import build_parser as _build_parser

_EXIT_OK = 0
_EXIT_CONFIG = 2
_EXIT_PLUGIN = 3
_EXIT_STAGE = 4
_EXIT_GENERIC = 1


def build_parser() -> argparse.ArgumentParser:
    return _build_parser()


def _handle_run(args: argparse.Namespace) -> CommandResult:
    return handle_run(args)


def _handle_plugins_validate(args: argparse.Namespace) -> CommandResult:
    return handle_plugins_validate(args)


def _handle_config_validate(args: argparse.Namespace) -> CommandResult:
    return handle_config_validate(args)


def _handle_report_quality(args: argparse.Namespace) -> CommandResult:
    return handle_report_quality(args)


def _handle_init(args: argparse.Namespace) -> CommandResult:
    return handle_init(args)


def _handle_doctor(args: argparse.Namespace) -> CommandResult:
    return handle_doctor(args)


def _handle_reliability_check(args: argparse.Namespace) -> CommandResult:
    return handle_reliability_check(args)


def _handle_ingest_sync(args: argparse.Namespace) -> CommandResult:
    return handle_ingest_sync(args)


def _handle_ingest_sync_all(args: argparse.Namespace) -> CommandResult:
    return handle_ingest_sync_all(args)


def _handle_ingest_validate(args: argparse.Namespace) -> CommandResult:
    return handle_ingest_validate(args)


def _handle_adapter_infer(args: argparse.Namespace) -> CommandResult:
    return handle_adapter_infer(args)


def _handle_eda_generate(args: argparse.Namespace) -> CommandResult:
    return handle_eda_generate(args)


def _handle_eda_diff(args: argparse.Namespace) -> CommandResult:
    return handle_eda_diff(args)


def _resolve_plugin_template_root():
    return resolve_plugin_template_root()


def _handle_scaffold_plugin(args: argparse.Namespace) -> CommandResult:
    return handle_scaffold_plugin(args)


def _handle_runs_list(args: argparse.Namespace) -> CommandResult:
    return handle_runs_list(args)


def _handle_runs_show(args: argparse.Namespace) -> CommandResult:
    return handle_runs_show(args)


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

    from pathlib import Path

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


def _handle_eda_gate(args: argparse.Namespace) -> CommandResult:
    return handle_eda_gate(args)


def _dispatch(args: argparse.Namespace) -> CommandResult | int | None:
    if args.command == "run":
        return _handle_run(args)
    if args.command == "plugins" and args.plugins_command == "validate":
        return _handle_plugins_validate(args)
    if args.command == "config" and args.config_command == "validate":
        return _handle_config_validate(args)
    if args.command == "report-quality":
        return _handle_report_quality(args)
    if args.command == "init":
        return _handle_init(args)
    if args.command == "doctor":
        return _handle_doctor(args)
    if args.command == "reliability" and args.reliability_command == "check":
        return _handle_reliability_check(args)
    if args.command == "ingest" and args.ingest_command == "sync":
        return _handle_ingest_sync(args)
    if args.command == "ingest" and args.ingest_command == "sync-all":
        return _handle_ingest_sync_all(args)
    if args.command == "ingest" and args.ingest_command == "validate":
        return _handle_ingest_validate(args)
    if args.command == "adapter" and args.adapter_command == "infer":
        return _handle_adapter_infer(args)
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
    if args.command == "runs" and args.runs_command == "list":
        return _handle_runs_list(args)
    if args.command == "runs" and args.runs_command == "show":
        return _handle_runs_show(args)
    return None


def _lineage_args(args: argparse.Namespace) -> dict[str, Any]:
    payload = dict(vars(args))
    payload.pop("format", None)
    return payload


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    output_format = getattr(args, "format", "json")
    tracked = should_track(_lineage_args(args))
    started_at = datetime.now(timezone.utc)

    payload: dict[str, Any] | None = None
    exit_code = _EXIT_GENERIC
    error_info: dict[str, str] | None = None

    try:
        result = _dispatch(args)
        if result is None:
            parser.error("unhandled command")
            exit_code = _EXIT_GENERIC
            return exit_code
        if isinstance(result, CommandResult):
            payload = result.payload
            emit_payload(result.payload, output_format)
            exit_code = result.exit_code
            return exit_code
        exit_code = int(result)
        return exit_code
    except ConfigValidationError as exc:
        emit_error(exc, output_format)
        exit_code = _EXIT_CONFIG
        error_info = {"type": exc.__class__.__name__, "message": str(exc)}
        return exit_code
    except (PluginLoadError, PluginValidationError, PluginExecutionError) as exc:
        emit_error(exc, output_format)
        exit_code = _EXIT_PLUGIN
        error_info = {"type": exc.__class__.__name__, "message": str(exc)}
        return exit_code
    except StageExecutionError as exc:
        emit_error(exc, output_format)
        exit_code = _EXIT_STAGE
        error_info = {"type": exc.__class__.__name__, "message": str(exc)}
        return exit_code
    except HonestRolesError as exc:
        emit_error(exc, output_format)
        exit_code = _EXIT_GENERIC
        error_info = {"type": exc.__class__.__name__, "message": str(exc)}
        return exit_code
    finally:
        if tracked:
            finished_at = datetime.now(timezone.utc)
            try:
                record = create_record(
                    args=_lineage_args(args),
                    exit_code=exit_code,
                    started_at=started_at,
                    finished_at=finished_at,
                    payload=payload,
                    error=error_info,
                )
                write_record(record)
            except Exception:
                # Never fail command execution because lineage persistence failed.
                pass


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
