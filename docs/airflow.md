# Airflow

`dltaf` is designed so the same manifest can move cleanly across:

- local development
- CI validation
- generated Airflow DAGs

## Main workflow

1. author or generate manifests
2. lint and plan them locally
3. generate DAG wrappers
4. run the same manifest contract in Airflow

```bash
dltaf manifest lint --manifests-dir ./manifests
dltaf dags generate --manifests-dir ./manifests --output-dir ./generated_dags
```

## What DAG generation expects

The DAG generator reads manifest files and creates lightweight wrappers. Your actual pipeline behavior still comes from:

- the manifest
- the core runtime
- any plugin modules referenced by the manifest

That keeps the generated DAG layer intentionally thin.

## Package-mode install profiles

For `PythonVirtualenvOperator`, prefer explicit slim profiles instead of a kitchen-sink install:

```yaml
airflow:
  task:
    use_virtualenv: true
    requirements:
      - dltaf[clickhouse,sqldb,postgres]==0.2.2
```

Typical mappings:

- SQL catalog + PostgreSQL source -> `dltaf[clickhouse,sqldb,postgres]`
- Oracle query mode -> `dltaf[clickhouse,sqldb,oracle]`
- MongoDB -> `dltaf[clickhouse,mongodb]`
- private ClickHouse + Vault flows -> `dltaf[runtime]`

## Secrets in Airflow

The same manifest connection sections work in Airflow because `dltaf` resolves them into environment variables before execution.

Typical pattern:

```yaml
connections:
  source:
    kind: postgres
    airflow_variable_prefix: SMOKE_SQLDB__
  destination:
    kind: clickhouse
    airflow_variable_prefix: CLICKHOUSE__
```

You can also continue to use Vault refs if your runtime environment already has Vault authentication configured.

## Private plugins in Airflow

Private integrations remain module-based. The important rule is simple:

- scheduler and worker need the same importable plugin modules

That can be achieved either by:

- shipping the private modules in the same deployment image
- or installing them as pinned private packages

Useful environment variables:

- `DLT_RUNNER_PLUGINS`
- `DLT_HOOK_PLUGINS`
- `DLT_INFRA_CHECK_PLUGINS`

## Recommended promotion flow

For each new pipeline:

1. `manifest lint`
2. `manifest run --plan`
3. `manifest run --dry-run`
4. generate DAGs
5. stage smoke run
6. production rollout

That sequence catches most configuration issues before a scheduler ever picks up the pipeline.
