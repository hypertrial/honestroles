from __future__ import annotations

from importlib import import_module

from honestroles.__about__ import __version__

__all__ = [
    "read_parquet",
    "write_parquet",
    "read_duckdb_table",
    "read_duckdb_query",
    "write_duckdb",
    "normalize_source_data_contract",
    "validate_source_data_contract",
    "clean_jobs",
    "filter_jobs",
    "FilterChain",
    "label_jobs",
    "rate_jobs",
    "CandidateProfile",
    "extract_job_signals",
    "rank_jobs",
    "build_application_plan",
    "PluginSpec",
    "PluginExport",
    "register_filter_plugin",
    "unregister_filter_plugin",
    "list_filter_plugins",
    "apply_filter_plugins",
    "register_label_plugin",
    "unregister_label_plugin",
    "list_label_plugins",
    "apply_label_plugins",
    "register_rate_plugin",
    "unregister_rate_plugin",
    "list_rate_plugins",
    "apply_rate_plugins",
    "load_plugins_from_entrypoints",
    "load_plugins_from_module",
    "__version__",
]

_LAZY_IMPORTS: dict[str, tuple[str, str]] = {
    "clean_jobs": ("honestroles.clean", "clean_jobs"),
    "FilterChain": ("honestroles.filter", "FilterChain"),
    "filter_jobs": ("honestroles.filter", "filter_jobs"),
    "normalize_source_data_contract": ("honestroles.io", "normalize_source_data_contract"),
    "read_duckdb_query": ("honestroles.io", "read_duckdb_query"),
    "read_duckdb_table": ("honestroles.io", "read_duckdb_table"),
    "read_parquet": ("honestroles.io", "read_parquet"),
    "validate_source_data_contract": ("honestroles.io", "validate_source_data_contract"),
    "write_duckdb": ("honestroles.io", "write_duckdb"),
    "write_parquet": ("honestroles.io", "write_parquet"),
    "label_jobs": ("honestroles.label", "label_jobs"),
    "CandidateProfile": ("honestroles.match", "CandidateProfile"),
    "extract_job_signals": ("honestroles.match", "extract_job_signals"),
    "rank_jobs": ("honestroles.match", "rank_jobs"),
    "build_application_plan": ("honestroles.match", "build_application_plan"),
    "PluginExport": ("honestroles.plugins", "PluginExport"),
    "PluginSpec": ("honestroles.plugins", "PluginSpec"),
    "apply_filter_plugins": ("honestroles.plugins", "apply_filter_plugins"),
    "apply_label_plugins": ("honestroles.plugins", "apply_label_plugins"),
    "apply_rate_plugins": ("honestroles.plugins", "apply_rate_plugins"),
    "list_filter_plugins": ("honestroles.plugins", "list_filter_plugins"),
    "list_label_plugins": ("honestroles.plugins", "list_label_plugins"),
    "list_rate_plugins": ("honestroles.plugins", "list_rate_plugins"),
    "load_plugins_from_entrypoints": ("honestroles.plugins", "load_plugins_from_entrypoints"),
    "load_plugins_from_module": ("honestroles.plugins", "load_plugins_from_module"),
    "register_filter_plugin": ("honestroles.plugins", "register_filter_plugin"),
    "register_label_plugin": ("honestroles.plugins", "register_label_plugin"),
    "register_rate_plugin": ("honestroles.plugins", "register_rate_plugin"),
    "unregister_filter_plugin": ("honestroles.plugins", "unregister_filter_plugin"),
    "unregister_label_plugin": ("honestroles.plugins", "unregister_label_plugin"),
    "unregister_rate_plugin": ("honestroles.plugins", "unregister_rate_plugin"),
    "rate_jobs": ("honestroles.rate", "rate_jobs"),
}

_SUBMODULES = {
    "clean",
    "filter",
    "io",
    "label",
    "llm",
    "match",
    "plugins",
    "rate",
    "schema",
}


def __getattr__(name: str) -> object:
    if name in _SUBMODULES:
        module = import_module(f"honestroles.{name}")
        globals()[name] = module
        return module

    target = _LAZY_IMPORTS.get(name)
    if target is None:
        raise AttributeError(f"module 'honestroles' has no attribute '{name}'")
    module_name, attr_name = target
    module = import_module(module_name)
    value = getattr(module, attr_name)
    globals()[name] = value
    return value


def __dir__() -> list[str]:
    return sorted(set(globals()) | set(__all__))
