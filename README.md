# dltaf

`dltaf` is a manifest-driven toolkit for building repeatable data-loading pipelines with `dlt`, optional Airflow DAG generation, and a plugin-first extension model.

The public package ships a clean OSS core:
- built-in source kinds for `oracle_custom_sql`, `sql_database`, and `mongodb`
- a unified source plugin registry
- Airflow runtime helpers for local, packaged, and virtualenv execution
- a Vault integration layer powered by [`vault-kv-client`](https://github.com/PaulKov/vault-kv-client)
- documentation and examples that stay safe to publish

Private integrations are intentionally not bundled into this repository. They can live in your monorepo, a private package index, or both, while still using the same `source.kind` contract.

## Why dltaf

- `YAML-first`: manifests stay readable and reviewable
- `plugin-first`: internal connectors plug in without forking the OSS core
- `Airflow-friendly`: isolated virtualenv tasks can resolve both the core package and private plugin requirements
- `Vault-ready`: one consistent secrets contract for source and destination credentials
- `self-service`: examples, docs, CLI inspection tools, and smoke-friendly workflows are included

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
pip install -U pip
pip install -e .[dev]
```

## Quick start

Validate an example manifest:

```bash
dltaf-run --manifest dltaf/examples/manifests/smoke_sql_database_catalog.yaml --validate-only
```

Inspect available plugins:

```bash
dltaf plugins list
dltaf plugins inspect sql_database
dltaf plugins doctor --manifest dltaf/examples/manifests/smoke_mongodb_catalog.yaml
```

Generate Airflow DAG files from a manifests directory:

```bash
dltaf-generate-dags --manifests-dir ./manifests --output-dir ./generated_dags
```

Render lineage for a manifests directory:

```bash
dltaf-show-lineage --manifests-dir ./manifests --format mermaid
```

## Built-in source kinds

### `oracle_custom_sql`

Use explicit SQL files and per-query metadata:
- one manifest can drive multiple queries
- merge mode can enforce `primary_key`
- SQL files stay separate from YAML

### `sql_database`

Use `dlt.sources.sql_database` in either:
- single-schema mode with `schema` + `tables`
- multi-schema mode with `schemas: {schema_name: {tables: [...]}}`

### `mongodb`

Use the bundled MongoDB runtime for:
- one or many collections
- optional collection filters and nesting control
- replace/append behavior through the manifest `run` section

## Private plugin UX

The public core is designed so private connectors can stay private without degrading developer experience.

### Option 1: local monorepo catalog

Point `dltaf` to a local plugin catalog:

```bash
export DLTAF_PLUGIN_PATHS="/path/to/monorepo/internal/dltaf_plugins"
dltaf plugins list
```

This is the softest rollout path when your private catalog still lives inside an existing monorepo.

### Option 2: importable plugin modules

Point `dltaf` to importable module names:

```bash
export DLTAF_PLUGIN_MODULES="company_private_plugins,team_connectors"
dltaf plugins list
```

### Option 3: installed private packages

Install a private package that exposes entry points in the `dltaf.plugins` group. `dltaf` will discover them automatically.

### Plugin contract

Every plugin registers one or more `SourcePlugin` objects with:
- `kind`
- `validate(manifest)`
- `build_runtime_env(manifest)` if needed
- `run(manifest)`

Canonical recommendation for private kinds:

```text
internal.customer_export
internal.partner_events
company.some_connector
```

### Scaffold a new plugin

```bash
dltaf scaffold plugin --kind internal.customer_export --output-dir ./internal/dltaf_plugins
```

## Airflow

`dltaf` ships Airflow helpers for:
- generating DAGs from manifests
- loading `run_manifest()` inside standard or virtualenv tasks
- propagating plugin paths, plugin modules, and plugin-specific requirements to isolated runtimes

Useful runtime environment variables:
- `DLTAF_PACKAGE_ROOT`
- `DLTAF_PLUGIN_PATHS`
- `DLTAF_PLUGIN_MODULES`
- `DLTAF_PLUGIN_REQUIREMENTS`

See the full guide in [GitHub Pages](https://paulkov.github.io/dltaf/airflow/).

## Vault integration

`dltaf` resolves manifest Vault references through `vault-kv-client`.

Supported reference forms:
- `vault://mount/path`
- `mount:path`
- mapping form with `mount_point`, `path`, and optional `kv_version`

This keeps the secrets contract stable across local runs, CI, and Airflow.

## Examples

The repository ships sanitized examples under `dltaf/examples/`:
- `smoke_oracle_custom_sql.yaml`
- `smoke_sql_database_catalog.yaml`
- `smoke_mongodb_catalog.yaml`

They are intentionally generic. Replace the sample Vault refs and connection settings with your own environment before running them against a live system.

## Documentation

Full docs live on GitHub Pages:

- Docs: https://paulkov.github.io/dltaf/
- Plugin guide: https://paulkov.github.io/dltaf/plugins/
- Airflow guide: https://paulkov.github.io/dltaf/airflow/
- Examples: https://paulkov.github.io/dltaf/examples/

## Development

Run the standard checks locally:

```bash
ruff check .
pytest
python -m build
mkdocs build
```

## License

Apache-2.0. See [LICENSE](LICENSE).
