from __future__ import annotations

from pathlib import Path
from typing import Any

from honestroles.config.models import PipelineConfig, PluginManifestConfig
from honestroles.errors import ConfigValidationError

try:
    import tomllib
except ModuleNotFoundError:  # pragma: no cover
    import tomli as tomllib  # type: ignore[no-redef]


def _read_toml(path: Path) -> dict[str, Any]:
    try:
        return tomllib.loads(path.read_text(encoding="utf-8"))
    except OSError as exc:
        raise ConfigValidationError(f"cannot read config file '{path}': {exc}") from exc
    except tomllib.TOMLDecodeError as exc:
        raise ConfigValidationError(f"invalid TOML in '{path}': {exc}") from exc


def load_pipeline_config(path: str | Path) -> PipelineConfig:
    config_path = Path(path).expanduser().resolve()
    raw = _read_toml(config_path)
    try:
        config = PipelineConfig.model_validate(raw)
    except Exception as exc:  # pydantic ValidationError
        raise ConfigValidationError(
            f"invalid pipeline config '{config_path}': {exc}"
        ) from exc
    return _resolve_pipeline_paths(config, config_path.parent)


def load_plugin_manifest(path: str | Path) -> PluginManifestConfig:
    manifest_path = Path(path).expanduser().resolve()
    raw = _read_toml(manifest_path)
    raw_plugins = raw.get("plugins", [])
    if isinstance(raw_plugins, dict):
        raise ConfigValidationError("'plugins' must be an array of tables ([[plugins]])")
    try:
        manifest = PluginManifestConfig.model_validate({"plugins": raw_plugins})
    except Exception as exc:  # pydantic ValidationError
        raise ConfigValidationError(
            f"invalid plugin manifest '{manifest_path}': {exc}"
        ) from exc
    return manifest


def _resolve_pipeline_paths(config: PipelineConfig, base_dir: Path) -> PipelineConfig:
    input_path = config.input.path
    if not input_path.is_absolute():
        input_path = (base_dir / input_path).resolve()

    output = config.output
    resolved_output = None
    if output is not None:
        output_path = output.path
        if not output_path.is_absolute():
            output_path = (base_dir / output_path).resolve()
        resolved_output = output.model_copy(update={"path": output_path})

    return config.model_copy(
        update={"input": config.input.model_copy(update={"path": input_path}), "output": resolved_output}
    )
