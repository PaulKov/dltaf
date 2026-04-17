# Examples

The package ships sanitized example manifests under `dltaf/examples/manifests/`.

## At a glance

| File | Use case | Contract style |
| --- | --- | --- |
| `smoke_sqldb_catalog.yaml` | PostgreSQL or other relational catalog extraction | Canonical |
| `smoke_sqldb_query.yaml` | Oracle query-driven extraction | Canonical |
| `smoke_mongodb.yaml` | MongoDB collections | Canonical |
| `smoke_sql_database_catalog.yaml` | Legacy SQL catalog manifest | Compatibility |
| `smoke_oracle_custom_sql.yaml` | Legacy Oracle query manifest | Compatibility |
| `smoke_mongodb_catalog.yaml` | Compatibility filename kept from earlier docs | Compatibility |

## Canonical SQL catalog example

File: `dltaf/examples/manifests/smoke_sqldb_catalog.yaml`

Use this when you want a small table list from a relational database.

```yaml
version: 1

pipeline:
  name: dlt__sqldb_catalog__to__clickhouse__raw
  destination: clickhouse
  dataset: raw
  progress: log
  dev_mode: false

run:
  write_disposition: append

connections:
  source:
    kind: postgres
    vault: ${ENV:POSTGRES__VAULT_REF|company:postgres/example}
    overrides:
      drivername: postgresql+psycopg2
      database: analytics
  destination:
    kind: clickhouse
    vault: ${ENV:CLICKHOUSE__VAULT_REF|company:clickhouse/example}
    overrides:
      database: raw
      dataset_table_separator: __

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

Recommended smoke commands:

```bash
dltaf manifest lint --manifest dltaf/examples/manifests/smoke_sqldb_catalog.yaml
dltaf manifest run --manifest dltaf/examples/manifests/smoke_sqldb_catalog.yaml --plan
```

## Canonical SQL query example

File: `dltaf/examples/manifests/smoke_sqldb_query.yaml`

Use this when you want explicit Oracle SQL files and per-query metadata.

```yaml
source:
  kind: sqldb
  dialect: oracle
  mode: query
  query:
    fetch_batch_size: 2000
    queries:
      - name: smoke_query
        table_name: smoke_query
        sql_file: ../sql/oracle_smoke_query.sql
        write_disposition: replace
        params:
          dt_from: "2025-01-01"
  dialect_options:
    init_sql: ""
```

The SQL file lives in `dltaf/examples/sql/oracle_smoke_query.sql`.

## MongoDB example

File: `dltaf/examples/manifests/smoke_mongodb.yaml`

Use this when you want to ingest one or more collections with the built-in MongoDB runtime.

```yaml
source:
  kind: mongodb
  database: analytics
  collection_names:
    - users
    - events
  max_table_nesting: 2
```

## Compatibility examples

The compatibility examples are intentionally still shipped because migration is part of the product UX.

### `smoke_sql_database_catalog.yaml`

Shows an older `sql_database` manifest. It still validates and runs, but `dltaf manifest doctor` will guide you toward canonical `sqldb`.

### `smoke_oracle_custom_sql.yaml`

Shows an older `oracle_custom_sql` manifest. It still works, but the preferred target shape is `sqldb + dialect=oracle + mode=query`.

### `smoke_mongodb_catalog.yaml`

This is a compatibility filename kept for users coming from older docs and blog posts. The runtime contract is the same as `smoke_mongodb.yaml`.

## Example generation shortcuts

If you do not want to start from a shipped example, generate a fresh one:

```bash
dltaf manifest doctor --template-kind sqldb_catalog --pipeline-name dlt__postgres__to__clickhouse__raw
dltaf manifest doctor --template-kind sqldb_query --pipeline-name dlt__oracle__to__clickhouse__raw
dltaf manifest doctor --template-kind mongodb --pipeline-name dlt__mongo__to__clickhouse__raw
```
