from __future__ import annotations

from honestroles.clean import clean_jobs
from honestroles.filter import FilterChain, filter_jobs
from honestroles.io import (
    normalize_source_data_contract,
    read_duckdb,
    read_parquet,
    validate_source_data_contract,
    write_duckdb,
    write_parquet,
)
from honestroles.plugins import (
    apply_filter_plugins,
    apply_label_plugins,
    list_filter_plugins,
    list_label_plugins,
    register_filter_plugin,
    register_label_plugin,
    unregister_filter_plugin,
    unregister_label_plugin,
)
from honestroles.label import label_jobs
from honestroles.rate import rate_jobs
from honestroles._deprecation import (
    HonestrolesDeprecationWarning,
    deprecated,
    warn_deprecated,
)

__all__ = [
    "read_parquet",
    "write_parquet",
    "read_duckdb",
    "write_duckdb",
    "normalize_source_data_contract",
    "validate_source_data_contract",
    "register_filter_plugin",
    "unregister_filter_plugin",
    "list_filter_plugins",
    "apply_filter_plugins",
    "register_label_plugin",
    "unregister_label_plugin",
    "list_label_plugins",
    "apply_label_plugins",
    "warn_deprecated",
    "deprecated",
    "HonestrolesDeprecationWarning",
    "clean_jobs",
    "filter_jobs",
    "FilterChain",
    "label_jobs",
    "rate_jobs",
]

__version__ = "0.1.0"
