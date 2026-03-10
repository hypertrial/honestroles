from __future__ import annotations

from collections import Counter, defaultdict
from datetime import UTC, datetime
import hashlib
import json
from pathlib import Path
from time import perf_counter
from typing import Any

from honestroles.errors import ConfigValidationError
from honestroles.io import read_parquet

from .models import RetrievalIndexResult, SCHEMA_VERSION
from .policy import load_recommendation_policy
from .scoring import normalize_job_record, tokenize_text

_SHARD_COUNT = 16


def build_retrieval_index(
    *,
    input_parquet: str | Path,
    output_dir: str | Path | None = None,
    policy_file: str | Path | None = None,
) -> RetrievalIndexResult:
    started = perf_counter()
    input_path = Path(input_parquet).expanduser().resolve()
    if not input_path.exists():
        raise ConfigValidationError(f"input parquet does not exist: '{input_path}'")

    policy, policy_source, policy_hash = load_recommendation_policy(policy_file)
    _ = policy

    frame = read_parquet(input_path)
    rows = [normalize_job_record(dict(row)) for row in frame.to_dicts()]
    rows_sorted = sorted(rows, key=lambda item: str(item.get("job_id", "")))

    input_hash = _hash_file(input_path)
    index_id = input_hash[:12]
    resolved_output_dir = _resolve_output_dir(output_dir=output_dir, index_id=index_id)
    resolved_output_dir.mkdir(parents=True, exist_ok=True)

    shard_dir = resolved_output_dir / "shards"
    shard_dir.mkdir(parents=True, exist_ok=True)

    jobs_file = resolved_output_dir / "jobs_latest.jsonl"
    facets_file = resolved_output_dir / "facets.json"
    quality_file = resolved_output_dir / "quality_summary.json"
    manifest_file = resolved_output_dir / "manifest.json"

    token_map: dict[str, set[str]] = defaultdict(set)
    facets = {
        "source": Counter(),
        "location": Counter(),
        "work_mode": Counter(),
        "seniority": Counter(),
        "employment_type": Counter(),
    }

    with jobs_file.open("w", encoding="utf-8") as handle:
        for row in rows_sorted:
            handle.write(json.dumps(row, sort_keys=True) + "\n")
            job_id = str(row.get("job_id", ""))
            for token in _job_tokens(row):
                token_map[token].add(job_id)
            _increment_facet(facets["source"], row.get("source"))
            _increment_facet(facets["location"], row.get("location"))
            _increment_facet(facets["work_mode"], row.get("work_mode"))
            _increment_facet(facets["seniority"], row.get("seniority"))
            _increment_facet(facets["employment_type"], row.get("employment_type"))

    shard_files = _write_shards(token_map, shard_dir)

    quality_summary = _quality_summary(rows_sorted)
    quality_file.write_text(json.dumps(quality_summary, indent=2, sort_keys=True), encoding="utf-8")

    facets_payload = {
        key: {name: int(count) for name, count in sorted(counter.items())}
        for key, counter in sorted(facets.items())
    }
    facets_file.write_text(json.dumps(facets_payload, indent=2, sort_keys=True), encoding="utf-8")

    built_at = datetime.now(UTC).replace(microsecond=0).isoformat()
    manifest = {
        "schema_version": SCHEMA_VERSION,
        "index_id": index_id,
        "built_at_utc": built_at,
        "input_parquet": str(input_path),
        "input_hash": input_hash,
        "policy_source": policy_source,
        "policy_hash": policy_hash,
        "counts": {
            "jobs": len(rows_sorted),
            "tokens": len(token_map),
            "shards": len(shard_files),
        },
        "files": {
            "jobs_latest": jobs_file.name,
            "facets": facets_file.name,
            "quality_summary": quality_file.name,
            "shards": [path.name for path in shard_files],
        },
    }
    manifest_file.write_text(json.dumps(manifest, indent=2, sort_keys=True), encoding="utf-8")

    duration_ms = int((perf_counter() - started) * 1000)
    return RetrievalIndexResult(
        schema_version=SCHEMA_VERSION,
        status="pass",
        index_id=index_id,
        input_parquet=str(input_path),
        index_dir=str(resolved_output_dir),
        manifest_file=str(manifest_file),
        jobs_file=str(jobs_file),
        facets_file=str(facets_file),
        quality_summary_file=str(quality_file),
        shard_dir=str(shard_dir),
        policy_source=policy_source,
        policy_hash=policy_hash,
        input_hash=input_hash,
        jobs_count=len(rows_sorted),
        token_count=len(token_map),
        shard_count=len(shard_files),
        built_at_utc=built_at,
        duration_ms=duration_ms,
    )


def load_index(index_dir: str | Path) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    root = Path(index_dir).expanduser().resolve()
    manifest_path = root / "manifest.json"
    jobs_path = root / "jobs_latest.jsonl"
    if not manifest_path.exists():
        raise ConfigValidationError(f"index manifest not found: '{manifest_path}'")
    if not jobs_path.exists():
        raise ConfigValidationError(f"index jobs file not found: '{jobs_path}'")

    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    jobs: list[dict[str, Any]] = []
    for line in jobs_path.read_text(encoding="utf-8").splitlines():
        if not line.strip():
            continue
        payload = json.loads(line)
        if isinstance(payload, dict):
            jobs.append(payload)
    return manifest, jobs


def _resolve_output_dir(*, output_dir: str | Path | None, index_id: str) -> Path:
    if output_dir in (None, ""):
        return (Path("dist/recommend/index") / index_id).expanduser().resolve()
    return Path(output_dir).expanduser().resolve()


def _job_tokens(job: dict[str, Any]) -> set[str]:
    parts = [
        str(job.get("title") or ""),
        str(job.get("company") or ""),
        str(job.get("description_text") or ""),
        " ".join(str(item) for item in job.get("skills", [])),
    ]
    out: set[str] = set()
    for part in parts:
        out.update(tokenize_text(part))
    return out


def _write_shards(token_map: dict[str, set[str]], shard_dir: Path) -> list[Path]:
    shard_payloads: dict[str, dict[str, list[str]]] = {}
    for token, job_ids in sorted(token_map.items()):
        shard = _token_shard(token)
        shard_payloads.setdefault(shard, {})[token] = sorted(job_ids)

    files: list[Path] = []
    for index in range(_SHARD_COUNT):
        shard = f"{index:02x}"
        payload = shard_payloads.get(shard, {})
        shard_path = shard_dir / f"{shard}.json"
        shard_path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")
        files.append(shard_path)
    return files


def _token_shard(token: str) -> str:
    digest = hashlib.sha256(token.encode("utf-8")).hexdigest()
    return f"{int(digest[:2], 16) % _SHARD_COUNT:02x}"


def _increment_facet(counter: Counter[str], value: object) -> None:
    if value is None:
        return
    text = str(value).strip().lower()
    if text:
        counter[text] += 1


def _quality_summary(rows: list[dict[str, Any]]) -> dict[str, Any]:
    total = len(rows)
    if total == 0:
        return {
            "row_count": 0,
            "company_non_null_pct": 0.0,
            "posted_at_non_null_pct": 0.0,
            "description_text_non_null_pct": 0.0,
        }

    def non_null_pct(field: str) -> float:
        non_null = 0
        for row in rows:
            value = row.get(field)
            if value is None:
                continue
            if isinstance(value, str) and not value.strip():
                continue
            non_null += 1
        return round((non_null / float(total)) * 100.0, 4)

    return {
        "row_count": total,
        "company_non_null_pct": non_null_pct("company"),
        "posted_at_non_null_pct": non_null_pct("posted_at"),
        "description_text_non_null_pct": non_null_pct("description_text"),
    }


def _hash_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        while True:
            chunk = handle.read(1024 * 1024)
            if not chunk:
                break
            digest.update(chunk)
    return digest.hexdigest()
