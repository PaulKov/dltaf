from __future__ import annotations

from pathlib import Path

from dlt_utils.manifest_runner import (
    clear_source_registry_cache,
    get_source_registry,
    validate_manifest,
)


def _manifest_stub(
    tmp_path: Path,
    *,
    name: str,
    kind: str,
    source_overrides: dict[str, object],
) -> dict[str, object]:
    manifest_path = tmp_path / f"{name}.yaml"
    manifest_path.write_text("version: 1\n", encoding="utf-8")
    return {
        "version": 1,
        "__manifest_path__": str(manifest_path),
        "pipeline": {
            "name": name,
            "destination": "clickhouse",
            "dataset": "raw",
        },
        "source": {
            "kind": kind,
            **source_overrides,
        },
    }


def test_default_registry_discovers_builtin_plugins(monkeypatch) -> None:
    monkeypatch.delenv("DLTAF_PLUGIN_PATHS", raising=False)
    monkeypatch.delenv("DLTAF_PLUGIN_MODULES", raising=False)
    clear_source_registry_cache()

    registry = get_source_registry()

    assert registry.resolve("oracle_custom_sql").kind == "oracle_custom_sql"
    assert registry.resolve("sql_database").kind == "sql_database"
    assert registry.resolve("mongodb").kind == "mongodb"


def test_validate_manifest_accepts_builtin_sql_database(
    monkeypatch,
    tmp_path: Path,
) -> None:
    monkeypatch.delenv("DLTAF_PLUGIN_PATHS", raising=False)
    monkeypatch.delenv("DLTAF_PLUGIN_MODULES", raising=False)
    clear_source_registry_cache()

    manifest = _manifest_stub(
        tmp_path,
        name="dlt__sql_database_demo",
        kind="sql_database",
        source_overrides={"schema": "public", "tables": ["events"]},
    )

    validate_manifest(manifest)


def test_validate_manifest_accepts_builtin_mongodb(
    monkeypatch,
    tmp_path: Path,
) -> None:
    monkeypatch.delenv("DLTAF_PLUGIN_PATHS", raising=False)
    monkeypatch.delenv("DLTAF_PLUGIN_MODULES", raising=False)
    clear_source_registry_cache()

    manifest = _manifest_stub(
        tmp_path,
        name="dlt__mongodb_demo",
        kind="mongodb",
        source_overrides={"database": "sample", "collection_names": ["events"]},
    )

    validate_manifest(manifest)


def test_registry_loads_local_plugin_catalog_package_without_packaging(
    monkeypatch,
    tmp_path: Path,
) -> None:
    package_dir = tmp_path / "local_catalog"
    package_dir.mkdir()
    (package_dir / "__init__.py").write_text(
        "\n".join(
            [
                "from dltaf.plugins import SourcePlugin",
                "",
                "def validate(manifest):",
                "    source = manifest.get('source') or {}",
                "    if not source.get('token'):",
                "        raise ValueError('source.token is required')",
                "",
                "def run(manifest):",
                "    return manifest['source']['token']",
                "",
                "PLUGINS = [",
                "    SourcePlugin(",
                "        kind='community.local_catalog_demo',",
                "        aliases=('local_catalog_demo',),",
                "        validate=validate,",
                "        run=run,",
                "    )",
                "]",
            ]
        ),
        encoding="utf-8",
    )

    monkeypatch.setenv("DLTAF_PLUGIN_PATHS", str(package_dir))
    monkeypatch.delenv("DLTAF_PLUGIN_MODULES", raising=False)
    clear_source_registry_cache()

    registry = get_source_registry()

    assert registry.resolve("local_catalog_demo").kind == "community.local_catalog_demo"

    manifest = _manifest_stub(
        tmp_path,
        name="dlt__local_catalog_demo",
        kind="local_catalog_demo",
        source_overrides={"token": "ok"},
    )
    validate_manifest(manifest)


def test_registry_loads_plugin_modules_from_env(monkeypatch, tmp_path: Path) -> None:
    module_dir = tmp_path / "module_catalog"
    module_dir.mkdir()
    module_name = "company_private_plugins"
    (module_dir / f"{module_name}.py").write_text(
        "\n".join(
            [
                "from dltaf.plugins import SourcePlugin",
                "",
                "def run(manifest):",
                "    return 'ok'",
                "",
                "PLUGINS = [SourcePlugin(kind='community.module_demo', aliases=('module_demo',), run=run)]",
            ]
        ),
        encoding="utf-8",
    )

    monkeypatch.syspath_prepend(str(module_dir))
    monkeypatch.setenv("DLTAF_PLUGIN_MODULES", module_name)
    monkeypatch.delenv("DLTAF_PLUGIN_PATHS", raising=False)
    clear_source_registry_cache()

    registry = get_source_registry()

    assert registry.resolve("module_demo").kind == "community.module_demo"
