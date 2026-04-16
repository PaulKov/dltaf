# Examples

The package includes sanitized example manifests under `dltaf/examples/manifests`.

## SQL database catalog

`smoke_sql_database_catalog.yaml`

Use this example when you want to ingest a small table list from a relational database with the built-in `sql_database` source.

## Oracle custom SQL

`smoke_oracle_custom_sql.yaml`

Use this example when you want explicit SQL files and per-query metadata. The example query file lives in `dltaf/examples/sql/oracle_smoke_query.sql`.

## MongoDB

`smoke_mongodb_catalog.yaml`

Use this example when you want to load one or more collections with the built-in `mongodb` source.

## Important note

The examples are intentionally generic. Replace sample Vault refs and connection settings with your own environment before running them against a live system.
