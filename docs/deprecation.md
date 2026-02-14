# Deprecation Policy

`honestroles` keeps public API behavior stable across minor releases and uses explicit deprecation notices for planned removals.

## Rules

1. Deprecate before removal.
   Public functions, arguments, and outputs must be deprecated in at least one minor release before removal.
2. Include explicit versions.
   Every deprecation notice must state:
   - version where deprecation started (`since`)
   - version where removal is planned (`remove_in`)
3. Provide a migration path.
   Each deprecation should name a replacement API when one exists.
4. Use standardized warnings.
   Use `HonestrolesDeprecationWarning` via:
   - `warn_deprecated(...)`
   - `@deprecated(...)`

## Helper Usage

```python
from honestroles.deprecation import deprecated

@deprecated(since="0.2.0", remove_in="0.4.0", alternative="new_api")
def old_api(...):
    ...
```

```python
from honestroles.deprecation import warn_deprecated

def shim():
    warn_deprecated(
        "old_api",
        since="0.2.0",
        remove_in="0.4.0",
        alternative="new_api",
    )
```
