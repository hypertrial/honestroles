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
