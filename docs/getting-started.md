# Getting Started

## Install

Lean core install:

```bash
pip install dltaf
```

Runtime profile examples:

```bash
pip install "dltaf[runtime]"
pip install "dltaf[clickhouse,sqldb,postgres]"
pip install "dltaf[clickhouse,sqldb,oracle]"
pip install "dltaf[clickhouse,mongodb]"
```

Editable local install:

```bash
git clone https://github.com/PaulKov/dltaf.git
cd dltaf
python3 -m venv .venv
source .venv/bin/activate
pip install --upgrade pip
pip install -e .[dev]
```

Use the lean core when you only need linting, planning, template generation, or docs.
Use a runtime profile when you want to actually execute manifests inside CI or Airflow.

## 1. Start from a shipped example

The fastest way to understand the framework is to lint and plan a shipped manifest:

```bash
dltaf manifest lint --manifest dltaf/examples/manifests/smoke_sqldb_catalog.yaml --allow-filename-mismatch
dltaf manifest run --manifest dltaf/examples/manifests/smoke_sqldb_catalog.yaml --plan
```

If you want a query-driven Oracle example:

```bash
dltaf manifest lint --manifest dltaf/examples/manifests/smoke_sqldb_query.yaml
```

If you want MongoDB:

```bash
dltaf manifest lint --manifest dltaf/examples/manifests/smoke_mongodb.yaml
```

## 2. Generate a fresh template

Use `manifest doctor` when you want a clean starting point:

```bash
dltaf manifest doctor \
  --template-kind sqldb_catalog \
  --pipeline-name dlt__postgres__to__clickhouse__raw
```

Other public-safe template kinds:

- `sqldb_catalog`
- `sqldb_query`
- `mongodb`

Legacy SQL template kinds such as `sql_database` and `oracle_custom_sql` still work, but they intentionally generate canonical `sqldb` output.

## 3. Understand the command surface

The core workflow is:

```bash
dltaf manifest lint --manifest ./manifests/my_pipeline.yaml
dltaf manifest run --manifest ./manifests/my_pipeline.yaml --plan
dltaf manifest run --manifest ./manifests/my_pipeline.yaml --dry-run
dltaf dags generate --manifests-dir ./manifests --output-dir ./generated_dags
dltaf lineage show --format mermaid
```

## 4. Configure secrets

`dltaf` resolves connection secrets through `vault-kv-client`.

Supported Vault refs:

- `vault://mount/path`
- `mount:path`
- mapping form with `mount_point`, `path`, and optional `kv_version`

Typical pattern:

```yaml
connections:
  source:
    kind: postgres
    vault: ${ENV:POSTGRES__VAULT_REF|company:postgres/example}
  destination:
    kind: clickhouse
    vault: ${ENV:CLICKHOUSE__VAULT_REF|company:clickhouse/example}
```

## 5. Add private integrations when you need them

Private integrations are loaded through extension registries. You can keep them inside your monorepo:

```yaml
run:
  runners:
    plugins:
      - internal.dltaf_plugins.customer_export.runner_plugin
```

Or via environment variables:

```bash
export DLT_RUNNER_PLUGINS="internal.dltaf_plugins.customer_export.runner_plugin"
```

See the full guide in [Plugins](plugins.md).
