# Plugins

`dltaf` uses a unified source plugin registry.

## Resolution order

For every `source.kind`, `dltaf` resolves plugins in this order:

1. built-in core plugins
2. installed entry points in the `dltaf.plugins` group
3. local modules or package paths from environment configuration

## Local monorepo catalog

If your private catalog stays inside a monorepo, point `dltaf` to it:

```bash
export DLTAF_PLUGIN_PATHS="/path/to/monorepo/internal/dltaf_plugins"
dltaf plugins list
```

## Importable modules

```bash
export DLTAF_PLUGIN_MODULES="company_private_plugins,team_connectors"
dltaf plugins list
```

## Installed private packages

Private packages can register plugins with Python entry points:

```toml
[project.entry-points."dltaf.plugins"]
customer_integrations = "customer_integrations.plugins:get_plugins"
```

## Plugin contract

Each plugin should register one or more `SourcePlugin` objects with:
- `kind`
- `validate(manifest)`
- `build_runtime_env(manifest)` if extra runtime variables are needed
- `run(manifest)`

## Naming guidance

Use namespaced identifiers for private kinds:

```text
internal.customer_export
company.billing_events
team.partner_sync
```

## Scaffold

```bash
dltaf scaffold plugin --kind internal.customer_export --output-dir ./internal/dltaf_plugins
```
