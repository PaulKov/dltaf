# dltaf

`dltaf` is a manifest-driven framework for repeatable data ingestion and orchestration.

It gives you:

- a canonical public YAML contract
- built-in support for `sqldb` and `mongodb`
- compatibility shims for older SQL manifests
- Airflow DAG generation and runtime helpers
- extension registries for private runners, hooks, and infra checks

## Public vs private boundary

The OSS repository intentionally ships only the reusable core:

- canonical SQL and MongoDB integrations
- generic execution services
- templating, linting, lineage, and Airflow tooling
- a stable extension contract for private modules

Internal business-specific connectors should stay in:

- a private monorepo package
- or a private Python distribution

They plug into the same registries, so the public core stays clean while private teams keep their own domain logic.

## What is canonical today

The current public recommendation is:

- use `source.kind: sqldb` for new relational manifests
- use `source.kind: mongodb` for MongoDB ingestion
- treat `sql_database`, `oracle_custom_sql`, and `oracle` as compatibility aliases only

## Suggested reading order

1. [Getting Started](getting-started.md)
2. [Installation Profiles](installation-profiles.md)
3. [Manifest Model](manifests.md)
4. [Examples](examples.md)
5. [Plugins and Private Integrations](plugins.md)
6. [Airflow](airflow.md)
7. [Compatibility and Migration](compatibility.md)
