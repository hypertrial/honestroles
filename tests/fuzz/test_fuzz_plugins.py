from __future__ import annotations

import json
import tempfile
from pathlib import Path

import pytest
from hypothesis import given

from honestroles.errors import ConfigValidationError
from honestroles.plugins.errors import PluginLoadError, PluginValidationError
from honestroles.plugins.registry import PluginRegistry
from tests.fuzz.strategies import callable_ref, plugin_kind, plugin_name


@pytest.mark.fuzz
@given(name=plugin_name, kind=plugin_kind, ref=callable_ref)
def test_plugin_manifest_load_behavior(name: str, kind: str, ref: str):
    tmp_path = Path(tempfile.mkdtemp(prefix="honestroles-plugin-fuzz-"))
    manifest = f"""
[[plugins]]
name = {json.dumps(name)}
kind = {json.dumps(kind)}
callable = {json.dumps(ref)}
enabled = true
order = 0
""".strip()
    path = tmp_path / "plugins.toml"
    path.write_text(manifest, encoding="utf-8")

    try:
        registry = PluginRegistry.from_manifest(path)
    except (ConfigValidationError, PluginLoadError, PluginValidationError):
        return

    loaded = registry.list(kind)  # type: ignore[arg-type]
    assert isinstance(loaded, tuple)
