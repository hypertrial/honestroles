from __future__ import annotations

import re
import tomllib
from pathlib import Path


def _load_plugins_index() -> dict[str, object]:
    path = Path(__file__).resolve().parents[1] / "plugins-index" / "plugins.toml"
    text = path.read_text(encoding="utf-8")
    return tomllib.loads(text)


def test_plugins_index_has_expected_structure() -> None:
    data = _load_plugins_index()
    assert "plugin" in data
    plugin_table = data["plugin"]
    assert isinstance(plugin_table, dict)
    assert plugin_table

    keys = list(plugin_table.keys())
    assert keys == sorted(keys)


def test_plugins_index_entries_validate_schema() -> None:
    data = _load_plugins_index()
    plugin_table = data["plugin"]
    assert isinstance(plugin_table, dict)

    status_values = {"reference", "active", "experimental", "deprecated"}
    api_version_pattern = re.compile(r"^\d+\.\d+$")

    for name, metadata in plugin_table.items():
        assert isinstance(name, str)
        assert name.strip() == name
        assert name
        assert isinstance(metadata, dict)

        required_keys = {"description", "repo", "api_version", "status", "maintainers"}
        assert required_keys.issubset(metadata.keys())

        assert isinstance(metadata["description"], str)
        assert metadata["description"].strip()

        assert isinstance(metadata["repo"], str)
        assert metadata["repo"].startswith("https://")

        assert isinstance(metadata["api_version"], str)
        assert api_version_pattern.match(metadata["api_version"])

        assert metadata["status"] in status_values

        maintainers = metadata["maintainers"]
        assert isinstance(maintainers, list)
        assert maintainers
        assert all(isinstance(handle, str) and handle.startswith("@") for handle in maintainers)
