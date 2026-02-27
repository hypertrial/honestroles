from __future__ import annotations

import honestroles as hr


def test_new_surface_exports_runtime_types() -> None:
    required = {
        "HonestRolesRuntime",
        "PluginRegistry",
        "PipelineSpec",
        "RuntimeConfig",
        "PluginManifestConfig",
        "JobDataset",
        "PipelineRun",
    }
    assert required.issubset(set(hr.__all__))


def test_old_global_plugin_api_not_exported() -> None:
    forbidden = {
        "register_filter_plugin",
        "register_label_plugin",
        "register_rate_plugin",
        "apply_filter_plugins",
        "apply_label_plugins",
        "apply_rate_plugins",
    }
    assert forbidden.isdisjoint(set(hr.__all__))
