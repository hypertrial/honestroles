# Runtime API

Public runtime API contracts for Python usage.

## `HonestRolesRuntime`

Constructor entrypoint:

```python
from honestroles import HonestRolesRuntime

runtime = HonestRolesRuntime.from_configs(
    pipeline_config_path="pipeline.toml",
    plugin_manifest_path="plugins.toml",  # optional
)
```

- `pipeline_config_path`: `str | Path`, required
- `plugin_manifest_path`: `str | Path | None`, optional

Execution:

```python
run = runtime.run()
```

## `PipelineRun`

`run()` returns `PipelineRun` with fields:

- `dataset`: final `JobDataset`
- `diagnostics`: `RuntimeDiagnostics`
- `application_plan`: `tuple[ApplicationPlanEntry, ...]`

## `JobDataset`

`JobDataset` is the strict canonical runtime stage I/O object.

- `to_polars(copy: bool = True) -> pl.DataFrame`
- `row_count() -> int`
- `columns() -> tuple[str, ...]`
- `iter_records() -> Iterator[CanonicalJobRecord]`
- `materialize_records(limit: int | None = None) -> list[CanonicalJobRecord]`
- `validate() -> None`
- `with_frame(frame) -> JobDataset`
- `transform(fn) -> JobDataset`
- Runtime-produced and plugin-returned datasets must retain all canonical fields and canonical logical dtypes.

Notes:

- `to_polars(copy=True)` is the explicit engine boundary and returns a clone by default.
- `rows()` and `select()` are not part of the public `JobDataset` API.

## Diagnostics Contract

`RuntimeDiagnostics.to_dict()` always includes:

- `input_path`
- `stage_rows`
- `plugin_counts`
- `runtime`
- `input_adapter`
- `input_aliasing`
- `final_rows`

Diagnostics conditionally include:

- `output_path` (when `[output]` is configured)
- `non_fatal_errors` (when `fail_fast = false` and errors occur)

## Determinism

The runtime seeds Python randomness from `runtime.random_seed` at run start. Fixed inputs/spec/plugins produce stable outputs.
