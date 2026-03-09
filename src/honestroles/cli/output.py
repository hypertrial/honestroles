from __future__ import annotations

import json
import sys
from typing import Any, Mapping


def _stringify(value: Any) -> str:
    if value is None:
        return "-"
    if isinstance(value, (str, int, float, bool)):
        return str(value)
    return json.dumps(value, sort_keys=True)


def _print_kv_rows(payload: Mapping[str, Any]) -> None:
    for key in sorted(payload):
        print(f"{key:20} {_stringify(payload[key])}")


def _print_doctor_table(payload: Mapping[str, Any]) -> None:
    summary = payload.get("summary", {})
    print(
        "SUMMARY pass={pass_count} warn={warn_count} fail={fail_count}".format(
            pass_count=summary.get("pass", 0),
            warn_count=summary.get("warn", 0),
            fail_count=summary.get("fail", 0),
        )
    )
    print("CHECK                 STATUS  MESSAGE")
    checks = payload.get("checks", [])
    if isinstance(checks, list):
        for item in checks:
            if not isinstance(item, Mapping):
                continue
            check_id = str(item.get("id", ""))
            status = str(item.get("status", ""))
            message = str(item.get("message", ""))
            print(f"{check_id:21} {status:6}  {message}")
            fix = item.get("fix")
            if fix not in (None, ""):
                print(f"{'':21} {'fix':6}  {fix}")


def _print_runs_table(payload: Mapping[str, Any]) -> None:
    runs = payload.get("runs", [])
    print("RUN_ID                           STATUS  COMMAND          STARTED_AT_UTC")
    if not isinstance(runs, list):
        return
    for item in runs:
        if not isinstance(item, Mapping):
            continue
        run_id = str(item.get("run_id", ""))[:32]
        status = str(item.get("status", ""))
        command = str(item.get("command", ""))
        started = str(item.get("started_at_utc", ""))
        print(f"{run_id:32} {status:6}  {command:15}  {started}")


def emit_payload(payload: Mapping[str, Any], output_format: str) -> None:
    if output_format == "json":
        print(json.dumps(payload, indent=2, sort_keys=True))
        return

    status = str(payload.get("status", "ok"))
    print(f"STATUS               {status}")

    if isinstance(payload.get("checks"), list) and isinstance(payload.get("summary"), Mapping):
        _print_doctor_table(payload)
        return
    if isinstance(payload.get("runs"), list):
        _print_runs_table(payload)
        return
    _print_kv_rows(payload)


def emit_error(exc: Exception, output_format: str) -> None:
    if output_format == "table":
        print(f"ERROR {exc.__class__.__name__}: {exc}", file=sys.stderr)
        return
    print(str(exc), file=sys.stderr)
