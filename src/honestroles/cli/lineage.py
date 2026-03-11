from __future__ import annotations

from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
import re
from typing import Any, Mapping
import uuid

from honestroles.config import load_pipeline_config

_SCHEMA_VERSION = "1.0"
_CHUNK_SIZE = 1024 * 1024


def runs_root() -> Path:
    return (Path.cwd() / ".honestroles" / "runs").resolve()


def _sha256_bytes(data: bytes) -> str:
    digest = hashlib.sha256()
    digest.update(data)
    return digest.hexdigest()


def _hash_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        while True:
            chunk = handle.read(_CHUNK_SIZE)
            if not chunk:
                break
            digest.update(chunk)
    return digest.hexdigest()


def _hash_path_reference(path: Path) -> str:
    return _sha256_bytes(f"path:{path.resolve()}".encode("utf-8"))


def _existing_path(path_like: str | Path | None) -> Path | None:
    if path_like in (None, ""):
        return None
    candidate = Path(path_like).expanduser().resolve()
    return candidate if candidate.exists() else None


def _hash_input_path(path: Path) -> str:
    if path.is_file():
        return _hash_file(path)
    manifest = path / "manifest.json"
    if manifest.exists() and manifest.is_file():
        return _hash_file(manifest)
    return _hash_path_reference(path)


def _args_fingerprint(args: Mapping[str, Any]) -> str:
    payload = json.dumps(args, sort_keys=True, default=str).encode("utf-8")
    return _sha256_bytes(payload)


def _command_key(args: Mapping[str, Any]) -> str:
    command = str(args.get("command", ""))
    if command == "adapter":
        return "adapter.infer"
    if command == "eda":
        return f"eda.{args.get('eda_command', '')}"
    if command == "reliability":
        return f"reliability.{args.get('reliability_command', '')}"
    if command == "ingest":
        return f"ingest.{args.get('ingest_command', '')}"
    if command == "runs":
        return f"runs.{args.get('runs_command', '')}"
    if command == "recommend":
        recommend_command = str(args.get("recommend_command", ""))
        if recommend_command == "feedback":
            feedback_command = str(args.get("recommend_feedback_command", ""))
            return f"recommend.feedback.{feedback_command}"
        return f"recommend.{recommend_command}"
    if command == "publish":
        target = str(args.get("publish_target", ""))
        subcommand = str(args.get("publish_neondb_command", ""))
        if target and subcommand:
            return f"publish.{target}.{subcommand}"
        return f"publish.{target}".rstrip(".")
    return command


def should_track(args: Mapping[str, Any]) -> bool:
    return _command_key(args) in {
        "run",
        "report-quality",
        "adapter.infer",
        "eda.generate",
        "eda.diff",
        "eda.gate",
        "reliability.check",
        "ingest.sync",
        "ingest.sync-all",
        "ingest.validate",
        "recommend.build-index",
        "recommend.match",
        "recommend.evaluate",
        "recommend.feedback.add",
        "recommend.feedback.summarize",
        "publish.neondb.migrate",
        "publish.neondb.sync",
        "publish.neondb.verify",
    }


def _pipeline_related_hashes(args: Mapping[str, Any]) -> tuple[str | None, dict[str, str], str]:
    input_hash: str | None = None
    input_hashes: dict[str, str] = {}

    pipeline_path = _existing_path(args.get("pipeline_config"))
    plugin_path = _existing_path(args.get("plugin_manifest"))
    policy_path = _existing_path(args.get("policy_file"))
    hash_sources: list[str] = []
    if pipeline_path is not None:
        hash_sources.append(_hash_file(pipeline_path))
        try:
            cfg = load_pipeline_config(pipeline_path)
            if cfg.input.path.exists():
                input_hash = _hash_input_path(cfg.input.path)
                input_hashes["input"] = input_hash
        except Exception:
            pass
    if plugin_path is not None:
        hash_sources.append(_hash_file(plugin_path))
    if policy_path is not None:
        hash_sources.append(_hash_file(policy_path))

    config_hash = _sha256_bytes("|".join(sorted(hash_sources)).encode("utf-8"))
    if not hash_sources:
        config_hash = _args_fingerprint(args)
    return input_hash, input_hashes, config_hash


def _eda_hashes(args: Mapping[str, Any]) -> tuple[str | None, dict[str, str], str]:
    input_hash: str | None = None
    input_hashes: dict[str, str] = {}
    command = _command_key(args)

    if command == "eda.generate":
        input_path = _existing_path(args.get("input_parquet"))
        if input_path is not None:
            digest = _hash_input_path(input_path)
            input_hash = digest
            input_hashes["input_parquet"] = digest
    elif command in {"eda.diff", "eda.gate"}:
        candidate = _existing_path(args.get("candidate_dir"))
        baseline = _existing_path(args.get("baseline_dir"))
        if candidate is not None:
            digest = _hash_input_path(candidate)
            input_hash = digest
            input_hashes["candidate_dir"] = digest
        if baseline is not None:
            input_hashes["baseline_dir"] = _hash_input_path(baseline)

    rules_file = _existing_path(args.get("rules_file"))
    hash_sources = [input_hashes[key] for key in sorted(input_hashes)]
    if rules_file is not None:
        hash_sources.append(_hash_file(rules_file))
    config_hash = _sha256_bytes("|".join(hash_sources).encode("utf-8"))
    if not hash_sources:
        config_hash = _args_fingerprint(args)
    return input_hash, input_hashes, config_hash


def _ingest_hashes(args: Mapping[str, Any]) -> tuple[str | None, dict[str, str], str]:
    input_hashes: dict[str, str] = {}
    manifest_path = _existing_path(args.get("manifest"))
    if manifest_path is not None:
        input_hashes["manifest"] = _hash_file(manifest_path)
    state_path = _existing_path(args.get("state_file"))
    if state_path is not None:
        input_hashes["state_file"] = _hash_file(state_path)
    args_hash = _args_fingerprint(args)
    hash_sources = [args_hash] + [input_hashes[key] for key in sorted(input_hashes)]
    config_hash = _sha256_bytes("|".join(hash_sources).encode("utf-8"))
    return None, input_hashes, config_hash


def compute_hashes(args: Mapping[str, Any]) -> tuple[str | None, dict[str, str], str]:
    command = _command_key(args)
    if command in {"run", "report-quality", "reliability.check"}:
        return _pipeline_related_hashes(args)
    if command in {"ingest.sync", "ingest.sync-all", "ingest.validate"}:
        return _ingest_hashes(args)
    if command == "adapter.infer":
        input_hashes: dict[str, str] = {}
        input_path = _existing_path(args.get("input_parquet"))
        if input_path is not None:
            digest = _hash_input_path(input_path)
            input_hashes["input_parquet"] = digest
            return digest, input_hashes, _sha256_bytes(digest.encode("utf-8"))
        return None, input_hashes, _args_fingerprint(args)
    if command.startswith("recommend."):
        input_hashes: dict[str, str] = {}
        for key in (
            "input_parquet",
            "index_dir",
            "candidate_json",
            "resume_text",
            "golden_set",
            "policy_file",
            "thresholds_file",
            "meta_json_file",
        ):
            path = _existing_path(args.get(key))
            if path is not None:
                input_hashes[key] = _hash_input_path(path)
        config_hash = _sha256_bytes(
            "|".join([_args_fingerprint(args)] + [input_hashes[key] for key in sorted(input_hashes)]).encode("utf-8")
        )
        return None, input_hashes, config_hash
    if command.startswith("publish.neondb."):
        input_hashes: dict[str, str] = {}
        for key in ("jobs_parquet", "index_dir", "sync_report"):
            path = _existing_path(args.get(key))
            if path is not None:
                input_hashes[key] = _hash_input_path(path)
        config_hash = _sha256_bytes(
            "|".join(
                [_args_fingerprint(args)]
                + [input_hashes[key] for key in sorted(input_hashes)]
            ).encode("utf-8")
        )
        return None, input_hashes, config_hash
    if command.startswith("eda."):
        return _eda_hashes(args)
    return None, {}, _args_fingerprint(args)


def build_artifact_paths(args: Mapping[str, Any], payload: Mapping[str, Any] | None) -> dict[str, str]:
    if payload is not None:
        return {
            str(key): str(value)
            for key, value in payload.items()
            if isinstance(value, str)
            and ("/" in value or value.endswith(".json") or value.endswith(".toml"))
        }

    command = _command_key(args)
    if command == "adapter.infer":
        output_file = Path(str(args.get("output_file", "dist/adapters/adapter-draft.toml")))
        output = output_file.expanduser().resolve()
        return {
            "adapter_draft": str(output),
            "inference_report": str(output.with_suffix(".report.json")),
        }
    if command == "reliability.check":
        output_file = Path(
            str(args.get("output_file", "dist/reliability/latest/gate_result.json"))
        )
        output = output_file.expanduser().resolve()
        return {"reliability_artifact": str(output)}
    if command == "ingest.sync":
        source = str(args.get("source", "unknown"))
        source_ref = str(args.get("source_ref", "unknown"))
        safe_ref = re.sub(r"[^A-Za-z0-9._-]+", "_", source_ref.strip()) or "unknown"
        default_root = Path("dist/ingest") / source / safe_ref
        output_file = Path(
            str(args.get("output_parquet", default_root / "jobs.parquet"))
        ).expanduser().resolve()
        report_file = Path(
            str(args.get("report_file", default_root / "sync_report.json"))
        ).expanduser().resolve()
        artifacts = {
            "output_parquet": str(output_file),
            "report_file": str(report_file),
        }
        if bool(args.get("write_raw", False)):
            if args.get("output_parquet") not in (None, ""):
                artifacts["raw_file"] = str(output_file.with_name("raw.jsonl"))
            else:
                artifacts["raw_file"] = str((default_root / "raw.jsonl").expanduser().resolve())
        return artifacts
    if command == "ingest.validate":
        source = str(args.get("source", "unknown"))
        source_ref = str(args.get("source_ref", "unknown"))
        safe_ref = re.sub(r"[^A-Za-z0-9._-]+", "_", source_ref.strip()) or "unknown"
        default_root = Path("dist/ingest") / source / safe_ref
        report_file = Path(
            str(args.get("report_file", default_root / "validate_report.json"))
        ).expanduser().resolve()
        artifacts = {"report_file": str(report_file)}
        if bool(args.get("write_raw", False)):
            artifacts["raw_file"] = str((default_root / "raw.jsonl").expanduser().resolve())
        return artifacts
    if command == "ingest.sync-all":
        report_file = Path(
            str(args.get("report_file", "dist/ingest/sync_all_report.json"))
        ).expanduser().resolve()
        return {"report_file": str(report_file)}
    if command == "recommend.build-index":
        output_dir = args.get("output_dir")
        if output_dir not in (None, ""):
            root = Path(str(output_dir)).expanduser().resolve()
            return {
                "index_dir": str(root),
                "manifest_file": str(root / "manifest.json"),
                "jobs_file": str(root / "jobs_latest.jsonl"),
                "facets_file": str(root / "facets.json"),
                "quality_summary_file": str(root / "quality_summary.json"),
            }
        return {}
    if command in {"recommend.match", "recommend.evaluate"}:
        return {}
    if command == "recommend.feedback.add":
        profile_id = str(args.get("profile_id", "")).strip().lower()
        root = (Path.cwd() / ".honestroles" / "recommend" / "feedback").resolve()
        artifacts = {"events_file": str(root / "events.jsonl")}
        if profile_id:
            artifacts["weights_file"] = str(root / "weights" / f"{profile_id}.json")
        return artifacts
    if command == "recommend.feedback.summarize":
        root = (Path.cwd() / ".honestroles" / "recommend" / "feedback").resolve()
        return {"events_file": str(root / "events.jsonl")}
    if command == "publish.neondb.migrate":
        return {}
    if command == "publish.neondb.sync":
        artifacts: dict[str, str] = {}
        jobs_path = args.get("jobs_parquet")
        if jobs_path not in (None, ""):
            artifacts["jobs_parquet"] = str(Path(str(jobs_path)).expanduser().resolve())
        index_dir = args.get("index_dir")
        if index_dir not in (None, ""):
            artifacts["index_dir"] = str(Path(str(index_dir)).expanduser().resolve())
        sync_report = args.get("sync_report")
        if sync_report not in (None, ""):
            artifacts["sync_report"] = str(Path(str(sync_report)).expanduser().resolve())
        return artifacts
    if command == "publish.neondb.verify":
        return {}
    return {}


def create_record(
    *,
    args: Mapping[str, Any],
    exit_code: int,
    started_at: datetime,
    finished_at: datetime,
    payload: Mapping[str, Any] | None = None,
    error: Mapping[str, str] | None = None,
) -> dict[str, Any]:
    input_hash, input_hashes, config_hash = compute_hashes(args)
    run_id = uuid.uuid4().hex
    duration_ms = int((finished_at - started_at).total_seconds() * 1000)
    check_codes: list[str] = []
    if payload is not None:
        raw_codes = payload.get("check_codes")
        if isinstance(raw_codes, list):
            check_codes = [str(code) for code in raw_codes if str(code).strip()]
    ingest_metrics = _ingest_metrics(_command_key(args), payload)
    recommend_metrics = _recommend_metrics(_command_key(args), payload)
    publish_metrics = _publish_metrics(_command_key(args), payload)
    return {
        "schema_version": _SCHEMA_VERSION,
        "run_id": run_id,
        "command": _command_key(args),
        "status": "pass" if exit_code == 0 else "fail",
        "started_at_utc": started_at.astimezone(timezone.utc).isoformat(),
        "finished_at_utc": finished_at.astimezone(timezone.utc).isoformat(),
        "duration_ms": duration_ms,
        "input_hash": input_hash,
        "input_hashes": input_hashes,
        "config_hash": config_hash,
        "artifact_paths": build_artifact_paths(args, payload),
        "check_codes": check_codes,
        "ingest_metrics": ingest_metrics,
        "recommend_metrics": recommend_metrics,
        "publish_metrics": publish_metrics,
        "error": dict(error) if error is not None else None,
    }


def _ingest_metrics(
    command: str,
    payload: Mapping[str, Any] | None,
) -> dict[str, Any] | None:
    if payload is None:
        return None
    if command == "ingest.sync":
        return {
            "request_count": _safe_int(payload.get("request_count")),
            "fetched_count": _safe_int(payload.get("fetched_count")),
            "normalized_count": _safe_int(payload.get("normalized_count")),
            "rows_written": _safe_int(payload.get("rows_written")),
            "quality_status": str(payload.get("quality_status", "pass")),
            "quality_summary": payload.get("quality_summary", {}),
            "key_field_completeness": payload.get("key_field_completeness", {}),
            "stage_timings_ms": payload.get("stage_timings_ms", {}),
            "warnings": payload.get("warnings", []),
        }
    if command == "ingest.sync-all":
        return {
            "total_sources": _safe_int(payload.get("total_sources")),
            "pass_count": _safe_int(payload.get("pass_count")),
            "fail_count": _safe_int(payload.get("fail_count")),
            "total_request_count": _safe_int(payload.get("total_request_count")),
            "total_fetched_count": _safe_int(payload.get("total_fetched_count")),
            "total_rows_written": _safe_int(payload.get("total_rows_written")),
            "quality_summary": payload.get("quality_summary", {}),
            "key_field_completeness": payload.get("key_field_completeness", {}),
            "stage_timings_ms": payload.get("stage_timings_ms", {}),
        }
    if command == "ingest.validate":
        return {
            "request_count": _safe_int(payload.get("request_count")),
            "fetched_count": _safe_int(payload.get("fetched_count")),
            "normalized_count": _safe_int(payload.get("normalized_count")),
            "rows_evaluated": _safe_int(payload.get("rows_evaluated")),
            "quality_status": str(payload.get("quality_status", "pass")),
            "quality_summary": payload.get("quality_summary", {}),
            "key_field_completeness": payload.get("key_field_completeness", {}),
            "stage_timings_ms": payload.get("stage_timings_ms", {}),
            "warnings": payload.get("warnings", []),
        }
    return None


def _recommend_metrics(
    command: str,
    payload: Mapping[str, Any] | None,
) -> dict[str, Any] | None:
    if payload is None:
        return None
    if command == "recommend.build-index":
        return {
            "index_id": str(payload.get("index_id", "")),
            "jobs_count": _safe_int(payload.get("jobs_count")),
            "token_count": _safe_int(payload.get("token_count")),
            "shard_count": _safe_int(payload.get("shard_count")),
        }
    if command == "recommend.match":
        return {
            "eligible_count": _safe_int(payload.get("eligible_count")),
            "excluded_count": _safe_int(payload.get("excluded_count")),
            "total_jobs": _safe_int(payload.get("total_jobs")),
            "top_k": _safe_int(payload.get("top_k")),
        }
    if command == "recommend.evaluate":
        return {
            "cases_evaluated": _safe_int(payload.get("cases_evaluated")),
            "metrics": payload.get("metrics", {}),
            "thresholds": payload.get("thresholds", {}),
            "failing_checks": payload.get("failing_checks", []),
        }
    if command == "recommend.feedback.add":
        return {
            "profile_id": str(payload.get("profile_id", "")),
            "event": str(payload.get("event", "")),
            "duplicate": bool(payload.get("duplicate", False)),
            "total_events": _safe_int(payload.get("total_events")),
        }
    if command == "recommend.feedback.summarize":
        return {
            "profile_id": payload.get("profile_id"),
            "total_events": _safe_int(payload.get("total_events")),
            "counts": payload.get("counts", {}),
        }
    return None


def _publish_metrics(
    command: str,
    payload: Mapping[str, Any] | None,
) -> dict[str, Any] | None:
    if payload is None:
        return None
    if command == "publish.neondb.migrate":
        return {
            "schema": payload.get("schema"),
            "migrations_total": _safe_int(payload.get("migrations_total")),
            "migrations_applied": payload.get("migrations_applied", []),
        }
    if command == "publish.neondb.sync":
        return {
            "schema": payload.get("schema"),
            "batch_id": payload.get("batch_id"),
            "inserted_count": _safe_int(payload.get("inserted_count")),
            "updated_count": _safe_int(payload.get("updated_count")),
            "deactivated_count": _safe_int(payload.get("deactivated_count")),
            "active_jobs": _safe_int(payload.get("active_jobs")),
            "quality_gate_status": payload.get("quality_gate_status"),
        }
    if command == "publish.neondb.verify":
        return {
            "schema": payload.get("schema"),
            "check_codes": payload.get("check_codes", []),
        }
    return None


def _safe_int(value: object) -> int:
    try:
        if value is None:
            return 0
        return int(value)
    except (TypeError, ValueError):
        return 0


def write_record(record: Mapping[str, Any]) -> Path:
    run_id = str(record["run_id"])
    target_dir = runs_root() / run_id
    target_dir.mkdir(parents=True, exist_ok=True)
    target = target_dir / "run.json"
    target.write_text(json.dumps(record, indent=2, sort_keys=True), encoding="utf-8")
    return target


def list_records(
    limit: int,
    status: str | None,
    command: str | None = None,
    since_utc: datetime | None = None,
    contains_code: str | None = None,
) -> list[dict[str, Any]]:
    root = runs_root()
    if not root.exists():
        return []
    records: list[dict[str, Any]] = []
    for run_file in sorted(root.glob("*/run.json")):
        payload = json.loads(run_file.read_text(encoding="utf-8"))
        if status is not None and payload.get("status") != status:
            continue
        if command is not None and payload.get("command") != command:
            continue
        if since_utc is not None:
            started_at = payload.get("started_at_utc")
            if not isinstance(started_at, str):
                continue
            try:
                started_dt = datetime.fromisoformat(started_at)
            except ValueError:
                continue
            if started_dt.tzinfo is None:
                started_dt = started_dt.replace(tzinfo=timezone.utc)
            else:
                started_dt = started_dt.astimezone(timezone.utc)
            if started_dt < since_utc:
                continue
        if contains_code:
            codes = payload.get("check_codes")
            if not isinstance(codes, list) or contains_code not in {str(v) for v in codes}:
                continue
        records.append(payload)
    records.sort(key=lambda item: str(item.get("started_at_utc", "")), reverse=True)
    return records[:limit]


def load_record(run_id: str) -> dict[str, Any]:
    path = runs_root() / run_id / "run.json"
    return json.loads(path.read_text(encoding="utf-8"))
