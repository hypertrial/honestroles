# Ingest Source-Ref Glossary

`honestroles ingest sync` and `honestroles ingest validate` require `--source`
and `--source-ref`.
`honestroles ingest sync-all` uses the same values inside `ingest.toml` `[[sources]]`
entries.

`--source-ref` identifies the public board/account endpoint per ATS connector.

## Greenhouse

- Source: `greenhouse`
- `source-ref`: board token
- Example:

```bash
$ honestroles ingest sync --source greenhouse --source-ref stripe
```

## Lever

- Source: `lever`
- `source-ref`: site/company handle
- Example:

```bash
$ honestroles ingest sync --source lever --source-ref netflix
```

## Ashby public postings

- Source: `ashby`
- `source-ref`: job board name
- Example:

```bash
$ honestroles ingest sync --source ashby --source-ref notion
```

## Workable public endpoints

- Source: `workable`
- `source-ref`: company subdomain
- Example:

```bash
$ honestroles ingest sync --source workable --source-ref workable
```

## Validation rules

`source-ref` must be non-empty and only use:

- letters and numbers
- `.`
- `_`
- `-`

## `sync-all` Mapping Example

```toml
[[sources]]
source = "greenhouse"
source_ref = "stripe"

[[sources]]
source = "lever"
source_ref = "netflix"
```
