# Roadmap

`dltaf` is evolving as an OSS-first core for manifest-driven loading, while keeping customer-specific connectors outside the public repository.

## Near-term priorities

### 1. Harden the canonical SQL contract

- keep `sqldb` as the public default for relational ingestion
- improve migration guidance from `sql_database`, `oracle_custom_sql`, and `oracle`
- expand validation and doctor guidance around `mode`, `dialect`, and query/catalog shapes

### 2. Make private integrations ergonomic

- keep private runners, hooks, and infra checks out of the OSS repository
- improve registry discovery and diagnostics for private plugin catalogs
- document monorepo-friendly and private-package-friendly patterns equally well

### 3. Raise the self-service UX bar

- continue enriching shipped manifests and end-to-end examples
- keep CLI help, docs, and manifest doctor templates aligned
- ship more troubleshooting guidance for Airflow, Vault, and package-based execution

### 4. Keep compatibility boring and safe

- support legacy manifests long enough for staged cutovers
- provide explicit migration notes before removing compatibility shims
- prefer additive migrations over flag-day rewrites

## What stays out of scope

- customer-specific integrations in the public repository
- hardcoded internal endpoints, topics, or secret paths
- workflow assumptions that require a single company monorepo layout

## Release philosophy

- small, documented, easy-to-adopt releases
- tests, docs, and examples move together
- public APIs stay cleaner than historical internal layout
