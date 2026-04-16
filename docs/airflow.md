# Airflow

`dltaf` supports both regular Python operators and isolated virtualenv tasks.

## Main idea

The manifest runner stays the same across local runs, CI, and Airflow. The runtime decides how to locate the framework and any private plugins.

## Important environment variables

- `DLTAF_PACKAGE_ROOT`: fallback root for an embedded framework checkout
- `DLTAF_PLUGIN_PATHS`: local plugin package or module paths
- `DLTAF_PLUGIN_MODULES`: importable Python modules that expose plugins
- `DLTAF_PLUGIN_REQUIREMENTS`: extra requirements that isolated virtualenv tasks should install

## Virtualenv behavior

The runtime resolves the framework in this order:

1. installed importable package
2. `DLTAF_PACKAGE_ROOT`
3. repo-local embedded checkout

This makes it safe to:
- run the OSS core from PyPI
- keep private catalogs in a separate monorepo
- later move the same private plugins into private wheels without changing manifests

## DAG generation

```bash
dltaf-generate-dags --manifests-dir ./manifests --output-dir ./generated_dags
```

## Lineage inspection

```bash
dltaf-show-lineage --manifests-dir ./manifests --format mermaid
```
