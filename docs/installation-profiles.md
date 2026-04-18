# Installation Profiles

`dltaf` ships a lean core and a small set of runtime extras. The goal is simple:

- local linting and planning should stay fast
- Airflow `PythonVirtualenvOperator` tasks should install only what the manifest needs
- stage smoke jobs should use a clear, reviewable package profile

## Core only

Use the base package when you only need:

- manifest lint
- manifest plan
- template generation
- lineage rendering
- docs or static validation tooling

```bash
pip install dltaf
```

This intentionally avoids Oracle, PostgreSQL, MongoDB, Vault, and full SQL runtime dependencies.

## Runtime profiles

| Scenario | Recommended install |
| --- | --- |
| ClickHouse destination with Vault-backed private plugins | `pip install "dltaf[runtime]"` |
| PostgreSQL or generic SQLDB catalog ingestion into ClickHouse | `pip install "dltaf[clickhouse,sqldb,postgres]"` |
| Oracle query-driven ingestion into ClickHouse | `pip install "dltaf[clickhouse,sqldb,oracle]"` |
| MongoDB ingestion into ClickHouse | `pip install "dltaf[clickhouse,mongodb]"` |
| Everything for framework development or broad compatibility smoke | `pip install "dltaf[all]"` |

## What each extra means

- `clickhouse`
  adds the ClickHouse destination runtime through `dlt[clickhouse]`
- `sqldb`
  adds the generic SQLDB runtime contract and `sqlalchemy`
- `postgres`
  adds `psycopg2-binary`
- `oracle`
  adds `oracledb`
- `mongodb`
  adds `pymongo`
- `vault`
  adds `vault-kv-client`
- `runtime`
  convenience profile for `clickhouse + vault`

## Airflow package-mode guidance

For `PythonVirtualenvOperator`, pin the exact profile in `requirements`:

```yaml
airflow:
  task:
    use_virtualenv: true
    requirements:
      - dltaf[clickhouse,sqldb,postgres]==0.2.6
```

That keeps cold-start installs much smaller than a kitchen-sink runtime profile.

## Private integrations

Private runner or hook plugins still bring their own dependencies. A typical split is:

```bash
pip install "dltaf[runtime]==0.2.6"
pip install "company-private-plugin==1.2.3"
```

or, for SQL-based private flows:

```bash
pip install "dltaf[runtime,sqldb,oracle]==0.2.6"
pip install "company-private-plugin==1.2.3"
```

The important rule is that the public `dltaf` profile covers generic runtime capabilities,
while your private packages cover business-specific integrations.
