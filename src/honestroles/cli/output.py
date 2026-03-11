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
    rows_value = payload.get("rows_written")
    if rows_value is None:
        rows_value = payload.get("rows_evaluated")
    print(
        "SYNC source={source} ref={ref} fetched={fetched} normalized={normalized} rows={rows} dedup_dropped={dropped} quality={quality}".format(
            source=_stringify(payload.get("source")),
            ref=_stringify(payload.get("source_ref")),
            fetched=_stringify(payload.get("fetched_count")),
            normalized=_stringify(payload.get("normalized_count")),
            rows=_stringify(rows_value),
            dropped=_stringify(payload.get("dedup_dropped")),
            quality=_stringify(payload.get("quality_status")),
        )
    )
    quality_summary = payload.get("quality_summary")
    if isinstance(quality_summary, Mapping):
        print(
            "QUALITY pass={pass_count} warn={warn_count} fail={fail_count}".format(
                pass_count=_stringify(quality_summary.get("pass")),
                warn_count=_stringify(quality_summary.get("warn")),
                fail_count=_stringify(quality_summary.get("fail")),
            )
        )
    warnings = payload.get("warnings")
    if isinstance(warnings, list) and warnings:
        print(f"warnings             {', '.join(str(item) for item in warnings)}")
    output_paths = payload.get("output_paths")
    if isinstance(output_paths, Mapping):
        for key in sorted(output_paths):
            print(f"{str(key):20} {_stringify(output_paths[key])}")


def _print_ingest_batch_table(payload: Mapping[str, Any]) -> None:
    print(
        "BATCH total_sources={total} pass={passed} fail={failed} rows={rows} fetched={fetched} requests={requests}".format(
            total=_stringify(payload.get("total_sources")),
            passed=_stringify(payload.get("pass_count")),
            failed=_stringify(payload.get("fail_count")),
            rows=_stringify(payload.get("total_rows_written")),
            fetched=_stringify(payload.get("total_fetched_count")),
            requests=_stringify(payload.get("total_request_count")),
        )
    )
    print("SOURCE       REF                  STATUS  ROWS  FETCHED  REQUESTS")
    sources = payload.get("sources")
    if isinstance(sources, list):
        for item in sources:
            if not isinstance(item, Mapping):
                continue
            print(
                "{source:12} {ref:20} {status:6}  {rows:4}  {fetched:7}  {requests:8}".format(
                    source=str(item.get("source", ""))[:12],
                    ref=str(item.get("source_ref", ""))[:20],
                    status=str(item.get("status", "")),
                    rows=str(item.get("rows_written", 0)),
                    fetched=str(item.get("fetched_count", 0)),
                    requests=str(item.get("request_count", 0)),
                )
            )
    report_file = payload.get("report_file")
    if report_file not in (None, ""):
        print(f"report_file           {_stringify(report_file)}")
    quality_summary = payload.get("quality_summary")
    if isinstance(quality_summary, Mapping):
        print(
            "QUALITY pass={pass_count} warn={warn_count} fail={fail_count}".format(
                pass_count=_stringify(quality_summary.get("pass")),
                warn_count=_stringify(quality_summary.get("warn")),
                fail_count=_stringify(quality_summary.get("fail")),
            )
        )


def _print_recommend_index_table(payload: Mapping[str, Any]) -> None:
    print(
        "INDEX index_id={index_id} jobs={jobs} tokens={tokens} shards={shards}".format(
            index_id=_stringify(payload.get("index_id")),
            jobs=_stringify(payload.get("jobs_count")),
            tokens=_stringify(payload.get("token_count")),
            shards=_stringify(payload.get("shard_count")),
        )
    )
    for key in ("index_dir", "manifest_file", "jobs_file", "facets_file", "quality_summary_file"):
        if key in payload:
            print(f"{key:20} {_stringify(payload.get(key))}")


def _print_recommend_match_table(payload: Mapping[str, Any]) -> None:
    print(
        "MATCH profile={profile} total={total} eligible={eligible} excluded={excluded} top_k={top_k}".format(
            profile=_stringify(payload.get("profile", {}).get("profile_id") if isinstance(payload.get("profile"), Mapping) else None),
            total=_stringify(payload.get("total_jobs")),
            eligible=_stringify(payload.get("eligible_count")),
            excluded=_stringify(payload.get("excluded_count")),
            top_k=_stringify(payload.get("top_k")),
        )
    )
    print("JOB_ID                SCORE    SOURCE       POSTED_AT")
    results = payload.get("results", [])
    if isinstance(results, list):
        for item in results[:10]:
            if not isinstance(item, Mapping):
                continue
            print(
                "{job_id:20} {score:7}  {source:10}  {posted}".format(
                    job_id=str(item.get("job_id", ""))[:20],
                    score=f"{float(item.get('score', 0.0)):.4f}",
                    source=str(item.get("source", ""))[:10],
                    posted=str(item.get("posted_at", ""))[:26],
                )
            )


def _print_recommend_eval_table(payload: Mapping[str, Any]) -> None:
    print(
        "EVAL cases={cases} status={status}".format(
            cases=_stringify(payload.get("cases_evaluated")),
            status=_stringify(payload.get("status")),
        )
    )
    metrics = payload.get("metrics")
    if isinstance(metrics, Mapping):
        for key in sorted(metrics):
            print(f"{key:20} {_stringify(metrics[key])}")
    failing = payload.get("failing_checks")
    if isinstance(failing, list) and failing:
        print(f"failing_checks        {', '.join(str(item) for item in failing)}")


def _print_recommend_feedback_table(payload: Mapping[str, Any]) -> None:
    if payload.get("event") is not None:
        print(
            "FEEDBACK profile={profile} event={event} duplicate={duplicate} total_events={total}".format(
                profile=_stringify(payload.get("profile_id")),
                event=_stringify(payload.get("event")),
                duplicate=_stringify(payload.get("duplicate")),
                total=_stringify(payload.get("total_events")),
            )
        )
    else:
        print(
            "FEEDBACK SUMMARY profile={profile} total_events={total}".format(
                profile=_stringify(payload.get("profile_id")),
                total=_stringify(payload.get("total_events")),
            )
        )
    counts = payload.get("counts")
    if isinstance(counts, Mapping):
        for key in sorted(counts):
            print(f"count.{key:14} {_stringify(counts[key])}")
    weights = payload.get("weights")
    if isinstance(weights, Mapping):
        for key in sorted(weights):
            print(f"weight.{key:13} {_stringify(weights[key])}")


def _print_publish_table(payload: Mapping[str, Any]) -> None:
    if payload.get("migrations_total") is not None:
        print(
            "NEON MIGRATE schema={schema} applied={applied} total={total}".format(
                schema=_stringify(payload.get("schema")),
                applied=_stringify(len(payload.get("migrations_applied", []))),
                total=_stringify(payload.get("migrations_total")),
            )
        )
    elif payload.get("batch_id") is not None:
        print(
            "NEON SYNC schema={schema} batch={batch} active={active} inserted={ins} updated={upd} deactivated={deact} quality={quality}".format(
                schema=_stringify(payload.get("schema")),
                batch=_stringify(payload.get("batch_id")),
                active=_stringify(payload.get("active_jobs")),
                ins=_stringify(payload.get("inserted_count")),
                upd=_stringify(payload.get("updated_count")),
                deact=_stringify(payload.get("deactivated_count")),
                quality=_stringify(payload.get("quality_gate_status")),
            )
        )
    elif isinstance(payload.get("checks"), list):
        print(
            "NEON VERIFY schema={schema} checks={count}".format(
                schema=_stringify(payload.get("schema")),
                count=_stringify(len(payload.get("checks", []))),
            )
        )
    checks = payload.get("checks")
    if isinstance(checks, list):
        print("CODE                          STATUS  MESSAGE")
        for item in checks:
            if not isinstance(item, Mapping):
                continue
            print(
                "{code:28} {status:6}  {message}".format(
                    code=str(item.get("code", ""))[:28],
                    status=str(item.get("status", "")),
                    message=str(item.get("message", "")),
                )
            )


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
    if isinstance(payload.get("sources"), list) and payload.get("total_sources") is not None:
        _print_ingest_batch_table(payload)
        return
    if payload.get("index_id") is not None and payload.get("jobs_count") is not None:
        _print_recommend_index_table(payload)
        return
    if (
        isinstance(payload.get("results"), list)
        and payload.get("eligible_count") is not None
        and payload.get("excluded_count") is not None
    ):
        _print_recommend_match_table(payload)
        return
    if isinstance(payload.get("metrics"), Mapping) and payload.get("cases_evaluated") is not None:
        _print_recommend_eval_table(payload)
        return
    if payload.get("event") is not None or payload.get("profile_counts") is not None:
        _print_recommend_feedback_table(payload)
        return
    if payload.get("database_url_env") is not None and payload.get("schema") is not None:
        _print_publish_table(payload)
        return
    if (
        payload.get("schema_version") is not None
        and payload.get("source") is not None
        and payload.get("source_ref") is not None
        and (
            payload.get("rows_written") is not None
            or payload.get("rows_evaluated") is not None
        )
    ):
        _print_ingest_table(payload)
        return
    _print_kv_rows(payload)


def emit_error(exc: Exception, output_format: str) -> None:
    if output_format == "table":
        print(f"ERROR {exc.__class__.__name__}: {exc}", file=sys.stderr)
        return
    print(str(exc), file=sys.stderr)
