# dltaf

`dltaf` is a manifest-driven orchestration layer for `dlt` pipelines. It combines:
- readable YAML manifests
- reusable runtime helpers
- Airflow-friendly task generation
- a plugin registry for private integrations

The public repository intentionally ships only generic built-ins. Private connectors should stay in a local monorepo catalog or a private package, and plug into the same registry contract.

## What you get

- built-in `oracle_custom_sql`, `sql_database`, and `mongodb` source kinds
- `dltaf plugins` CLI for inspection and diagnostics
- `dltaf-run` for manifest validation and execution
- `dltaf-generate-dags` for Airflow DAG generation
- `dltaf-show-lineage` for dependency inspection

## Recommended reading order

1. [Getting Started](getting-started.md)
2. [Plugins](plugins.md)
3. [Airflow](airflow.md)
4. [Examples](examples.md)
