# Manifest Model

`dltaf` uses one YAML document per pipeline.

At minimum, a manifest has:

```yaml
version: 1

pipeline:
  name: dlt__example__to__clickhouse__raw
  destination: clickhouse
  dataset: raw

source:
  kind: sqldb

connections:
  source:
    kind: postgres
    vault: company:postgres/example
  destination:
    kind: clickhouse
    vault: company:clickhouse/example
```

Vault mappings also accept an explicit structured form:

```yaml
connections:
  source:
    kind: postgres
    vault:
      ref: ${ENV:POSTGRES__VAULT_REF|company:postgres/example}
      kv_version: "2"
  destination:
    kind: clickhouse
    vault:
      ref: ${ENV:CLICKHOUSE__VAULT_REF|company:clickhouse/example}
      kv_version: "2"
```

## Top-level sections

### `version`

Current public version is `1`.

### `pipeline`

Required:

- `name`
- `destination`
- `dataset`

Common optional keys:

- `progress`
- `dev_mode`

### `run`

Used for execution behavior:

- `write_disposition`
- `hooks`
- `runners.plugins`
- `online_checks`
- `replace_policy`

### `connections`

Public core understands these connection slots:

- `source`
- `destination`
- `kafka`

Connection specs can resolve values from Vault, Airflow Variables, or direct overrides.

### `source`

This is where the actual extraction contract lives.

Canonical public source kinds:

- `sqldb`
- `mongodb`

Compatibility SQL aliases:

- `sql_database`
- `oracle_custom_sql`
- `oracle`

### `airflow`

Optional section used by DAG generation:

- `dag_id`
- `schedule`
- `start_date`
- `catchup`
- `max_active_runs`
- `tags`

## Canonical SQLDB shapes

### Catalog mode

Use this for relational table discovery:

```yaml
source:
  kind: sqldb
  dialect: generic
  mode: catalog
  catalog:
    schema: public
    tables:
      - orders
      - customers
```

#### Optional partial-success policy for catalog loads

Use this when one or more catalog tables may be absent or flaky, but the run
should still succeed if the configured policy is satisfied.

```yaml
run:
  write_disposition: merge
  partial_success:
    mode: any_success
    tolerate_errors: any_per_table

source:
  kind: sqldb
  dialect: generic
  mode: catalog
  catalog:
    schema: public
    tables:
      - orders
      - customers
```

Supported modes:

- `any_success`
- `critical_tables`
- `threshold`

Supported error toleration policies:

- `missing_table_only`
- `source_only`
- `any_per_table`

## Unit Checkpoints

Long-running API-style runners can opt into unit checkpoints. This is useful
for Airflow retries: if a task times out after processing thousands of API
units, the retry can restore already successful unit payloads and still run the
normal destination cleanup/load path.

```yaml
run:
  checkpoint:
    enabled: true
    # Optional. Defaults to DLTAF_LOAD_UUID, then AIRFLOW_CTX_DAG_RUN_ID,
    # then the framework-generated run_id.
    load_uuid: monthly-b057-2026-05
    # Optional. Runners should provide a meaningful default for the selected
    # backfill window or API batch.
    batch_key: b057:2026:FULL_YEAR:2023:QUARTER_1:descending:all
```

Checkpoint identity:

- `load_uuid` identifies the logical load attempt across retries.
- `batch_key` scopes a logical window within that load.
- `resume_key` is runner-defined per unit, for example `bin:961040001237` or
  `project_period:BIN=...|projectId=...|year=...|period=...`.

Safety rule: checkpoints persist emitted rows, not only unit status. A resumed
unit contributes its stored rows to the same load path, so retries do not lose
data when the previous attempt timed out before the destination write finished.

### Query mode

Use this for Oracle query files:

```yaml
source:
  kind: sqldb
  dialect: oracle
  mode: query
  query:
    fetch_batch_size: 2000
    queries:
      - name: orders_snapshot
        table_name: orders_snapshot
        sql_file: ../sql/oracle_smoke_query.sql
        write_disposition: replace
        params:
          dt_from: "2025-01-01"
  dialect_options:
    init_sql: ""
```

## MongoDB shape

```yaml
source:
  kind: mongodb
  database: analytics
  collection_names:
    - users
    - events
  max_table_nesting: 2
```

## Private runner plugins

Private connectors keep the same manifest contract, but supply their own runner through the plugin registry:

```yaml
run:
  runners:
    plugins:
      - internal.dltaf_plugins.customer_export.runner_plugin

source:
  kind: internal.customer_export
  endpoint: /api/v1/export
```

The core does not need to know your business-specific schema in advance. It only needs a loadable runner plugin for `source.kind`.
