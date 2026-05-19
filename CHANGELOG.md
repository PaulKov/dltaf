# Changelog

## 0.2.8

Structured Vault references are now preserved across the connection resolver.

Highlights:
- `connections.*.vault` mappings such as `{ref: "mount:path", kv_version: "2"}` now reach `vault-kv-client` as structured refs instead of stringified Python dicts
- package-mode Airflow runtimes no longer need consumer repo-local `dlt_utils` overrides for KV v2 refs
- regression coverage pins the structured Vault ref contract at the connection-resolution boundary

## 0.2.6

Package-first consumers now import runtime result contracts directly from the public `dltaf` namespace.

Highlights:
- public `dltaf` exports now include `RunResult`, `UnitRunStats`, `build_run_result`, and related runtime helpers
- private consumer plugins no longer need legacy `dlt_utils.*` imports for framework-core result contracts
- package-first environments are more robust when legacy compatibility paths are also present on `sys.path`

## 0.2.5

API-style runners now support framework-level unit observability and queryable unit audit history.

Highlights:
- new structured `UnitRunStats` and `RunResult.unit_stats` contract for per-unit execution telemetry
- built-in runtime summary now renders unit-level tables and rollups alongside existing per-table SQL summaries
- built-in audit hook now writes detailed unit rows into ClickHouse `<dataset>._pipeline_run_units`
- new `dltaf runs units` CLI command for self-service inspection of per-unit audit history
- `run.observability` added to manifest schema with `verbosity` and `dlt_progress` controls
- `pkb_conclusion` private runner can now emit full per-BIN lifecycle, requestId/decision, timings, rows and aggregated PKB rollups

## 0.2.4

SQL catalog manifests now support framework-level partial-success execution and structured table-level runtime summaries.

Highlights:
- `run.partial_success` added for `sql_database` and `sqldb` catalog manifests
- catalog mode can now tolerate per-table failures and still succeed when policy conditions are met
- built-in runtime summary hook prints compact per-table and rollup statistics for all jobs
- ClickHouse target stats now include best-effort before/after rows and storage deltas per table
- Airflow task docs now surface partial-success mode without changing `task_id`

## 0.2.3

Airflow package-mode integration is now part of the public OSS release.

Highlights:
- `dag_builder` now includes the latest package-first `PythonVirtualenvOperator` bridge
- slim install-profile helpers are shipped publicly in `dlt_utils.install_profiles`
- generated DAG wrappers pass `manifests_dir` explicitly, so the installed wheel no longer depends on repo-local default paths
- public package now exposes optional extra `dltaf[airflow]`

## 0.2.2

Vault-backed manifest connections now support explicit KV version pinning.

Highlights:
- `connections.*.vault` accepts both string refs and structured mappings
- new public mapping form supports `ref + kv_version`
- `parse_vault_ref` now understands `{ref: "mount:path", kv_version: "..."}`
- Vault env bootstrap now keeps `VAULT_ADDR` and `VAULT_ADDRESS` aligned
- docs and Airflow guidance now show deterministic KV v2 patterns for low-privilege roles

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
