# Label Conventions

Use these labels for plugin and extension-related changes.

- `plugin-api`: changes to public plugin contract, compatibility, or registration signatures.
- `plugin-runtime`: loader behavior, registry behavior, error handling, or runtime wiring.
- `docs`: documentation-only change.
- `breaking`: backward-incompatible behavior change.

Recommended combinations:

- Plugin API + breaking change: `plugin-api`, `breaking`
- Loader/runtime fixes: `plugin-runtime`
- Docs-only updates for plugins: `docs`
