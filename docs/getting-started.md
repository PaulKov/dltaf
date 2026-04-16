# Getting Started

## Install

```bash
pip install dltaf
```

For local development:

```bash
git clone https://github.com/PaulKov/dltaf.git
cd dltaf
python3 -m venv .venv
source .venv/bin/activate
pip install -U pip
pip install -e .[dev]
```

## Validate a manifest

```bash
dltaf-run --manifest dltaf/examples/manifests/smoke_sql_database_catalog.yaml --validate-only
```

## Inspect plugins

```bash
dltaf plugins list
dltaf plugins inspect mongodb
dltaf plugins doctor --manifest dltaf/examples/manifests/smoke_mongodb_catalog.yaml
```

## Built-in source kinds

### `oracle_custom_sql`

Use when you want explicit SQL files and per-query metadata.

### `sql_database`

Use when you want schema and table driven ingestion through `dlt.sources.sql_database`.

### `mongodb`

Use when you want one or more MongoDB collections loaded through the bundled runtime.

## Secrets

The recommended pattern is to resolve credentials from Vault via `vault-kv-client`.

Supported reference forms:
- `vault://mount/path`
- `mount:path`
- mapping form with `mount_point`, `path`, and optional `kv_version`
