# dltaf

[![CI](https://github.com/PaulKov/dltaf/actions/workflows/ci.yml/badge.svg?branch=master)](https://github.com/PaulKov/dltaf/actions/workflows/ci.yml)
[![Docs](https://github.com/PaulKov/dltaf/actions/workflows/pages.yml/badge.svg?branch=master)](https://github.com/PaulKov/dltaf/actions/workflows/pages.yml)
[![PyPI](https://img.shields.io/pypi/v/dltaf.svg)](https://pypi.org/project/dltaf/)
[![License](https://img.shields.io/github/license/PaulKov/dltaf.svg)](LICENSE)

`dltaf` is a manifest-driven data loading framework built around three ideas:

- canonical, reviewable YAML manifests
- a stable OSS core for generic sources
- extension registries that let private integrations stay private

The public repository ships a clean stage63-based core with:

- canonical `source.kind: sqldb` for relational ingestion
- built-in `mongodb` support
- compatibility aliases for legacy SQL manifests such as `sql_database`, `oracle_custom_sql`, and `oracle`
- Airflow DAG generation helpers
- manifest linting, doctoring, scaffolding, and lineage tooling
- Vault-backed secrets resolution through [`vault-kv-client`](https://github.com/PaulKov/vault-kv-client)

Private connectors such as internal APIs, Kafka-backed flows, or company-specific uploaders are intentionally not bundled into the OSS package. They should live in your monorepo or private package index and plug into the same runner, hook, and infra-check registries.

## Why dltaf

- `Manifest-first`: pipeline behavior stays diffable and reviewable
- `Canonical SQL model`: one public SQL contract, with legacy aliases supported as migration shims
- `Plugin-first`: private integrations extend the framework without forking it
- `Airflow-friendly`: the same manifest can be linted locally, planned in CI, and executed in DAG wrappers
- `Self-service`: example manifests, template generation, and migration guidance ship with the package

## Installation

Runtime install:

```bash
pip install dltaf
```

Developer install:

```bash
git clone https://github.com/PaulKov/dltaf.git
cd dltaf
python3 -m venv .venv
source .venv/bin/activate
pip install --upgrade pip
pip install -e .[dev]
```

## Quick start

Validate the canonical SQL example:

```bash
dltaf manifest lint --manifest dltaf/examples/manifests/smoke_sqldb_catalog.yaml --allow-filename-mismatch
```

Render a safe execution plan without side effects:

```bash
dltaf manifest run \
  --manifest dltaf/examples/manifests/smoke_sqldb_catalog.yaml \
  --plan
```

Generate a new public-safe template:

```bash
dltaf manifest doctor \
  --template-kind sqldb_query \
  --pipeline-name dlt__oracle__to__clickhouse__raw
```

Generate Airflow DAG wrappers:

```bash
dltaf dags generate --manifests-dir ./manifests --output-dir ./generated_dags
```

Show lineage:

```bash
dltaf lineage show --format mermaid
```

## Canonical built-ins

### `sqldb`

`sqldb` is the canonical relational source kind.

Use `mode: catalog` when you want schema-and-table driven extraction:

- PostgreSQL, MySQL, MSSQL, or other generic SQL databases
- catalog-level table selection
- canonical shape under `source.catalog`

Use `mode: query` when you want explicit Oracle SQL queries:

- one or more named queries
- query files under `dltaf/examples/sql/` or your own repo
- Oracle-specific options under `source.dialect_options`

### `mongodb`

Use `mongodb` when you want one or more collections loaded through the bundled generic runtime:

- explicit collection selection
- optional table nesting control
- manifest-level replace/append behavior through `run.write_disposition`

## Compatibility aliases

`dltaf` still accepts older SQL source kinds as compatibility shims:

- `sql_database` -> canonicalized to `sqldb + dialect=generic + mode=catalog`
- `oracle_custom_sql` -> canonicalized to `sqldb + dialect=oracle + mode=query`
- `oracle` -> canonical alias for Oracle query mode

The public recommendation is still to write new manifests directly in canonical `sqldb` form.

## Private integrations

The OSS core uses three extension registries:

- runner plugins
- hook plugins
- infra-check plugins

You can load private modules either from the environment or directly from a manifest:

```yaml
run:
  runners:
    plugins:
      - internal.dltaf_plugins.customer_export.runner_plugin
  hooks:
    plugins:
      - internal.dltaf_plugins.shared.hooks
  online_checks:
    plugins:
      - internal.dltaf_plugins.customer_export.infra_checks
```

Or through environment variables:

```bash
export DLT_RUNNER_PLUGINS="internal.dltaf_plugins.customer_export.runner_plugin"
export DLT_HOOK_PLUGINS="internal.dltaf_plugins.shared.hooks"
export DLT_INFRA_CHECK_PLUGINS="internal.dltaf_plugins.customer_export.infra_checks"
```

This keeps the manifest contract stable even if the private catalog later moves from a monorepo to a private wheel.

The roadmap for evolving this split between OSS core and private integrations lives in [ROADMAP.md](ROADMAP.md).

## Vault integration

`dltaf` resolves manifest Vault references through `vault-kv-client`.

Supported reference forms:

- `vault://mount/path`
- `mount:path`
- mapping form with `mount_point`, `path`, and optional `kv_version`

That contract is intentionally simple and portable across local runs, CI, and Airflow.

## Shipped examples

Canonical examples live under `dltaf/examples/manifests/`:

- `smoke_sqldb_catalog.yaml`
- `smoke_sqldb_query.yaml`
- `smoke_mongodb.yaml`

Compatibility examples are also shipped for migration and search continuity:

- `smoke_sql_database_catalog.yaml`
- `smoke_oracle_custom_sql.yaml`
- `smoke_mongodb_catalog.yaml`

All examples are sanitized. Replace the sample Vault refs and connection overrides with values from your own environment.

## Documentation

Full docs live on GitHub Pages:

- Docs: https://paulkov.github.io/dltaf/
- Getting started: https://paulkov.github.io/dltaf/getting-started/
- Examples: https://paulkov.github.io/dltaf/examples/
- Plugins: https://paulkov.github.io/dltaf/plugins/
- Airflow: https://paulkov.github.io/dltaf/airflow/

## Development

Run the standard checks locally:

```bash
ruff check .
pytest
python -m build
mkdocs build --strict
```

## Roadmap

The near-term focus is:

- keep `sqldb` and `mongodb` boring, explicit, and stable
- improve self-service docs, templates, and examples
- make private registries easy to adopt from a monorepo or a private package index
- preserve compatibility aliases long enough for staged migrations without surprise breakage

## License

Apache-2.0. See [LICENSE](LICENSE).
