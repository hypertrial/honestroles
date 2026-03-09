# FAQ

## Is plugin registration global?

No. Plugin loading is instance-scoped via `PluginRegistry` per runtime.

## Does stage order change based on config order?

No. Execution order is fixed: `clean -> filter -> label -> rate -> match`.

## Can I disable stages?

Yes. Set `enabled = false` on the stage section.

## What if plugin manifest is omitted?

Runtime uses an empty plugin registry and only built-in stage behavior.

## How do I make runs deterministic?

Use fixed inputs/config/plugins and set `[runtime].random_seed`.

## Why do I see success with errors in diagnostics?

You likely set `fail_fast = false`; check `non_fatal_errors`.

## What is the difference between `doctor` and `reliability check`?

- `doctor` validates readiness and prints checks but does not write an artifact.
- `reliability check` runs the same evaluator and writes a gate artifact (`dist/reliability/latest/gate_result.json` by default).

Use `doctor` for local debugging and `reliability check --strict` for CI gates.

## Where should reliability thresholds live?

In a separate `reliability.toml` file passed with `--policy`.

Reliability thresholds are intentionally not embedded in `pipeline.toml` in v1.
