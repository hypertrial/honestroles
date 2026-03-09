from __future__ import annotations

from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
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
    if command == "runs":
        return f"runs.{args.get('runs_command', '')}"
    return command


def should_track(args: Mapping[str, Any]) -> bool:
    return _command_key(args) in {
        "run",
        "report-quality",
        "adapter.infer",
        "eda.generate",
        "eda.diff",
        "eda.gate",
    }


def _pipeline_related_hashes(args: Mapping[str, Any]) -> tuple[str | None, dict[str, str], str]:
    input_hash: str | None = None
    input_hashes: dict[str, str] = {}

    pipeline_path = _existing_path(args.get("pipeline_config"))
    plugin_path = _existing_path(args.get("plugin_manifest"))
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


def compute_hashes(args: Mapping[str, Any]) -> tuple[str | None, dict[str, str], str]:
    command = _command_key(args)
    if command in {"run", "report-quality"}:
        return _pipeline_related_hashes(args)
    if command == "adapter.infer":
        input_hashes: dict[str, str] = {}
        input_path = _existing_path(args.get("input_parquet"))
        if input_path is not None:
            digest = _hash_input_path(input_path)
            input_hashes["input_parquet"] = digest
            return digest, input_hashes, _sha256_bytes(digest.encode("utf-8"))
        return None, input_hashes, _args_fingerprint(args)
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
        "error": dict(error) if error is not None else None,
    }


def write_record(record: Mapping[str, Any]) -> Path:
    run_id = str(record["run_id"])
    target_dir = runs_root() / run_id
    target_dir.mkdir(parents=True, exist_ok=True)
    target = target_dir / "run.json"
    target.write_text(json.dumps(record, indent=2, sort_keys=True), encoding="utf-8")
    return target


def list_records(limit: int, status: str | None) -> list[dict[str, Any]]:
    root = runs_root()
    if not root.exists():
        return []
    records: list[dict[str, Any]] = []
    for run_file in sorted(root.glob("*/run.json")):
        payload = json.loads(run_file.read_text(encoding="utf-8"))
        if status is not None and payload.get("status") != status:
            continue
        records.append(payload)
    records.sort(key=lambda item: str(item.get("started_at_utc", "")), reverse=True)
    return records[:limit]


def load_record(run_id: str) -> dict[str, Any]:
    path = runs_root() / run_id / "run.json"
    return json.loads(path.read_text(encoding="utf-8"))
