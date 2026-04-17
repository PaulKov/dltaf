# Changelog

## 0.2.1

Runtime packaging has been split into explicit install profiles.

Highlights:
- lean base install for linting, planning, scaffolding, and docs
- public extras for `clickhouse`, `sqldb`, `postgres`, `oracle`, `mongodb`, `vault`, and `runtime`
- clearer docs for Airflow `PythonVirtualenvOperator` package-mode usage
- slimmer E2E and stage-smoke consumer install contracts
- packaging guardrails so heavy runtime dependencies do not silently drift back into the core package

## 0.2.0

Stage63-based public rebase of `dltaf`.

Highlights:
- canonical public SQL contract centered on `source.kind: sqldb`
- built-in public runtimes for `sqldb` and `mongodb`
- compatibility aliases for legacy SQL manifests such as `sql_database`, `oracle_custom_sql`, and `oracle`
- richer example manifests and example SQL shipped inside the package
- rewritten public docs, GitHub Pages navigation, and migration guidance
- registry-first extension model for private runners, hooks, and infra checks
- Airflow runtime helpers and package-mode ergonomics aligned with the fresh framework layout
- Vault integration powered by `vault-kv-client`
- GitHub Actions CI, PyPI-ready packaging, and GitHub Pages docs on the corrected stage63 base

## 0.1.0

First public bootstrap release of `dltaf`.
