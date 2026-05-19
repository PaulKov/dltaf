from __future__ import annotations


def _note(comment: str | None) -> str:
    if not comment:
        return ""
    return f"# NOTE: {comment}\n"


def _render_sqldb_catalog_template(*, pipeline_name: str, destination: str, dataset: str, note: str | None = None) -> str:
    return _note(note) + f"""version: 1
pipeline:
  name: {pipeline_name}
  destination: {destination}
  dataset: {dataset}
  progress: log
  dev_mode: false
run:
  write_disposition: merge
connections:
  source:
    kind: postgres
    vault: ${{ENV:POSTGRES__VAULT_REF|company:postgres/example}}
    overrides:
      drivername: postgresql+psycopg2
      database: example
  destination:
    kind: clickhouse
    vault: ${{ENV:CLICKHOUSE__VAULT_REF|company:clickhouse/example}}
    overrides:
      database: {dataset}
      dataset_table_separator: __
source:
  kind: sqldb
  dialect: generic
  mode: catalog
  catalog:
    schema: public
    tables:
      - table_1
      - table_2
airflow:
  dag_id: {pipeline_name}
  schedule: "0 3 * * *"
  start_date: "2025-01-01"
  catchup: false
  max_active_runs: 1
  tags:
    - dlt
    - sqldb
    - catalog
"""


def _render_sqldb_query_template(*, pipeline_name: str, destination: str, dataset: str, note: str | None = None) -> str:
    return _note(note) + f"""version: 1
pipeline:
  name: {pipeline_name}
  destination: {destination}
  dataset: {dataset}
  progress: log
  dev_mode: false
run:
  write_disposition: replace
connections:
  source:
    kind: oracle
    vault: ${{ENV:ORACLE__VAULT_REF|company:oracle/example}}
    overrides:
      drivername: oracle+oracledb
  destination:
    kind: clickhouse
    vault: ${{ENV:CLICKHOUSE__VAULT_REF|company:clickhouse/example}}
    overrides:
      database: {dataset}
      dataset_table_separator: __
source:
  kind: sqldb
  dialect: oracle
  mode: query
  query:
    fetch_batch_size: 2000
    queries:
      - name: example
        table_name: example
        sql_file: ../sql/oracle_smoke_query.sql
        write_disposition: replace
  dialect_options:
    init_sql: ""
airflow:
  dag_id: {pipeline_name}
  schedule: "0 3 * * *"
  start_date: "2025-01-01"
  catchup: false
  max_active_runs: 1
  tags:
    - dlt
    - sqldb
    - query
"""


def render_template(*, kind: str, pipeline_name: str, destination: str, dataset: str) -> str:
    kind = str(kind or "").strip()
    if kind in {"sqldb_catalog", "sql_database"}:
        note = None
        if kind == "sql_database":
            note = "requested deprecated template-kind `sql_database`; generated canonical `sqldb` catalog template instead"
        return _render_sqldb_catalog_template(pipeline_name=pipeline_name, destination=destination, dataset=dataset, note=note)
    if kind in {"sqldb_query", "oracle_custom_sql", "oracle"}:
        note = None
        if kind in {"oracle_custom_sql", "oracle"}:
            note = f"requested {'deprecated' if kind == 'oracle_custom_sql' else 'non-canonical preset'} template-kind `{kind}`; generated canonical `sqldb` query template instead"
        return _render_sqldb_query_template(pipeline_name=pipeline_name, destination=destination, dataset=dataset, note=note)
    if kind == "mongodb":
        return f"""version: 1
pipeline:
  name: {pipeline_name}
  destination: {destination}
  dataset: {dataset}
  progress: log
  dev_mode: false
run:
  write_disposition: replace
connections:
  source:
    kind: mongodb
    vault: ${{ENV:MONGODB__VAULT_REF|company:mongodb/example}}
  destination:
    kind: clickhouse
    vault: ${{ENV:CLICKHOUSE__VAULT_REF|company:clickhouse/example}}
    overrides:
      database: {dataset}
      dataset_table_separator: __
source:
  kind: mongodb
  name: mongodb
  database: example
  collection_names:
    - collection_1
  max_table_nesting: 2
airflow:
  dag_id: {pipeline_name}
  schedule: "0 3 * * *"
  start_date: "2025-01-01"
  catchup: false
  max_active_runs: 1
  tags:
    - dlt
    - mongodb
"""
    raise ValueError(
        f"Unknown public template kind: {kind!r}. "
        "Supported kinds: sqldb_catalog, sqldb_query, mongodb, sql_database, oracle_custom_sql, oracle."
    )
    supported = ["uploader_b057", "pkb_conclusion", "sqldb_catalog", "sqldb_query", "mongodb"]
    raise ValueError(
        f"Unsupported template kind: {kind!r}. Supported canonical template kinds: {', '.join(supported)}"
    )


__all__ = ["render_template"]
