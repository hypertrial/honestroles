from __future__ import annotations

from honestroles.__about__ import __version__
from honestroles.clean import clean_jobs
from honestroles.filter import FilterChain, filter_jobs
from honestroles.io import (
    normalize_source_data_contract,
    read_duckdb_query,
    read_duckdb_table,
    read_parquet,
    validate_source_data_contract,
    write_duckdb,
    write_parquet,
)
from honestroles.label import label_jobs
from honestroles.plugins import (
    PluginExport,
    PluginSpec,
    apply_filter_plugins,
    apply_label_plugins,
    apply_rate_plugins,
    list_filter_plugins,
    list_label_plugins,
    list_rate_plugins,
    load_plugins_from_entrypoints,
    load_plugins_from_module,
    register_filter_plugin,
    register_label_plugin,
    register_rate_plugin,
    unregister_filter_plugin,
    unregister_label_plugin,
    unregister_rate_plugin,
)
from honestroles.rate import rate_jobs

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
