from __future__ import annotations

import argparse
import importlib.util
import subprocess
import sys

from honestroles.eda import load_eda_artifacts
from honestroles.errors import ConfigValidationError, HonestRolesError, StageExecutionError
from honestroles.plugins.errors import (
    PluginExecutionError,
    PluginLoadError,
    PluginValidationError,
)

from .handlers import (
    handle_config_validate,
    handle_eda_diff,
    handle_eda_gate,
    handle_eda_generate,
    handle_plugins_validate,
    handle_report_quality,
    handle_run,
    handle_scaffold_plugin,
    resolve_plugin_template_root,
)
from .parser import build_parser as _build_parser

_EXIT_OK = 0
_EXIT_CONFIG = 2
_EXIT_PLUGIN = 3
_EXIT_STAGE = 4
_EXIT_GENERIC = 1


def build_parser() -> argparse.ArgumentParser:
    return _build_parser()


def _handle_run(args: argparse.Namespace) -> int:
    return handle_run(args)


def _handle_plugins_validate(args: argparse.Namespace) -> int:
    return handle_plugins_validate(args)


def _handle_config_validate(args: argparse.Namespace) -> int:
    return handle_config_validate(args)


def _handle_report_quality(args: argparse.Namespace) -> int:
    return handle_report_quality(args)


def _handle_eda_generate(args: argparse.Namespace) -> int:
    return handle_eda_generate(args)


def _handle_eda_diff(args: argparse.Namespace) -> int:
    return handle_eda_diff(args)


def _resolve_plugin_template_root():
    return resolve_plugin_template_root()


def _handle_scaffold_plugin(args: argparse.Namespace) -> int:
    return handle_scaffold_plugin(args)


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


def _handle_eda_gate(args: argparse.Namespace) -> int:
    return handle_eda_gate(args)


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
