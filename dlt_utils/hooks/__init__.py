"""Built-in hooks.

Hooks are a standardized middleware layer around pipeline execution.

This package contains hooks shipped with the framework.
They are enabled by default (unless disabled in manifest `run.hooks`).

Current built-ins:
- basic_logging: start/finish/error logs
- env_summary: how many env vars were injected (values never printed)
- explain_config: log env var *sources* when `--explain-config` is enabled
- audit_run: best-effort audit row into ClickHouse `<db>._pipeline_runs`
"""
