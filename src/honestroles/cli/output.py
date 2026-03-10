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
    print("CHECK                 CODE                     SEVERITY  MESSAGE")
    checks = payload.get("checks", [])
    if isinstance(checks, list):
        for item in checks:
            if not isinstance(item, Mapping):
                continue
            check_id = str(item.get("id", ""))
            code = str(item.get("code", ""))
            severity = str(item.get("severity", ""))
            message = str(item.get("message", ""))
            print(f"{check_id:21} {code:24} {severity:8}  {message}")
            fix = item.get("fix")
            if fix not in (None, ""):
                print(f"{'':21} {'fix':24} {'':8}  {fix}")
            fix_snippet = item.get("fix_snippet")
            if fix_snippet not in (None, ""):
                print(f"{'':21} {'snippet':24} {'':8}  available")


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


def _print_ingest_table(payload: Mapping[str, Any]) -> None:
    print(
        "SYNC source={source} ref={ref} fetched={fetched} normalized={normalized} rows={rows} dedup_dropped={dropped}".format(
            source=_stringify(payload.get("source")),
            ref=_stringify(payload.get("source_ref")),
            fetched=_stringify(payload.get("fetched_count")),
            normalized=_stringify(payload.get("normalized_count")),
            rows=_stringify(payload.get("rows_written")),
            dropped=_stringify(payload.get("dedup_dropped")),
        )
    )
    output_paths = payload.get("output_paths")
    if isinstance(output_paths, Mapping):
        for key in sorted(output_paths):
            print(f"{str(key):20} {_stringify(output_paths[key])}")


def emit_payload(payload: Mapping[str, Any], output_format: str) -> None:
    if output_format == "json":
        print(json.dumps(payload, indent=2, sort_keys=True))
        return

    status = str(payload.get("status", "ok"))
    print(f"STATUS               {status}")

    if isinstance(payload.get("checks"), list) and isinstance(payload.get("summary"), Mapping):
        _print_doctor_table(payload)
        reliability_artifact = payload.get("reliability_artifact")
        if isinstance(reliability_artifact, str) and reliability_artifact:
            print(f"ARTIFACT             {reliability_artifact}")
        return
    if isinstance(payload.get("runs"), list):
        _print_runs_table(payload)
        return
    if (
        payload.get("schema_version") is not None
        and payload.get("source") is not None
        and payload.get("source_ref") is not None
        and payload.get("rows_written") is not None
    ):
        _print_ingest_table(payload)
        return
    _print_kv_rows(payload)


def emit_error(exc: Exception, output_format: str) -> None:
    if output_format == "table":
        print(f"ERROR {exc.__class__.__name__}: {exc}", file=sys.stderr)
        return
    print(str(exc), file=sys.stderr)
