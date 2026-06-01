from __future__ import annotations

import builtins
import importlib
import inspect
import logging
import os
import sys
import types
from pathlib import Path


def _install_airflow_stubs(monkeypatch) -> None:
    airflow = types.ModuleType("airflow")
    operators = types.ModuleType("airflow.operators")
    python = types.ModuleType("airflow.operators.python")
    utils = types.ModuleType("airflow.utils")
    task_group = types.ModuleType("airflow.utils.task_group")

    class _DummyOperator:
        def __init__(self, *args, **kwargs) -> None:
            self.args = args
            self.kwargs = kwargs

    class _DummyTaskGroup:
        pass

    class _DummyDag:
        pass

    airflow.DAG = _DummyDag
    python.PythonOperator = _DummyOperator
    python.PythonVirtualenvOperator = _DummyOperator
    task_group.TaskGroup = _DummyTaskGroup

    monkeypatch.setitem(sys.modules, "airflow", airflow)
    monkeypatch.setitem(sys.modules, "airflow.operators", operators)
    monkeypatch.setitem(sys.modules, "airflow.operators.python", python)
    monkeypatch.setitem(sys.modules, "airflow.utils", utils)
    monkeypatch.setitem(sys.modules, "airflow.utils.task_group", task_group)


def _clear_dlt_utils_modules(monkeypatch) -> None:
    for name in list(sys.modules):
        if name == "dlt_utils" or name.startswith("dlt_utils."):
            monkeypatch.delitem(sys.modules, name, raising=False)


def _load_task_factory(monkeypatch):
    _install_airflow_stubs(monkeypatch)
    package_root = Path(__file__).resolve().parents[1]
    monkeypatch.syspath_prepend(str(package_root))
    monkeypatch.delitem(sys.modules, "dag_builder.task_factory", raising=False)
    return importlib.import_module("dag_builder.task_factory")


def _strip_repo_root_from_sys_path(monkeypatch) -> Path:
    package_root = Path(__file__).resolve().parents[1].resolve()
    cwd_root = Path.cwd().resolve()
    monkeypatch.setattr(
        sys,
        "path",
        [
            entry
            for entry in sys.path
            if (
                entry
                and Path(entry).resolve() not in {package_root, cwd_root}
            )
        ],
    )
    return package_root


def _write_fake_runner(root: Path, marker_name: str) -> Path:
    package_dir = root / "dlt_utils"
    package_dir.mkdir(parents=True, exist_ok=True)
    (package_dir / "__init__.py").write_text("", encoding="utf-8")
    (package_dir / "manifest_runner.py").write_text(
        "\n".join(
            [
                "from pathlib import Path",
                "import os",
                "",
                "def run_manifest(manifest_path, configure_logging=False):",
                f"    target = Path(os.environ['{marker_name}'])",
                "    target.write_text(f'{manifest_path}|{configure_logging}', encoding='utf-8')",
            ]
        ),
        encoding="utf-8",
    )
    return package_dir


def _force_fallback_import(monkeypatch, allowed_root: Path) -> None:
    """Force the first package import attempt to fail until a fallback root is added."""

    real_import = builtins.__import__
    allowed_root = allowed_root.resolve()

    def _guarded_import(name, globals=None, locals=None, fromlist=(), level=0):
        wants_manifest_runner = name == "dlt_utils.manifest_runner" or (
            name == "dlt_utils" and fromlist and "manifest_runner" in fromlist
        )
        if wants_manifest_runner:
            current_roots = {
                Path(entry).resolve()
                for entry in sys.path
                if entry and Path(entry).exists()
            }
            if allowed_root not in current_roots:
                raise ImportError("Simulated missing installed dlt_utils to exercise fallback")
        return real_import(name, globals, locals, fromlist, level)

    monkeypatch.setattr(builtins, "__import__", _guarded_import)


def test_candidate_framework_roots_prefers_env_root_then_consumer_repo(monkeypatch, tmp_path) -> None:
    task_factory = _load_task_factory(monkeypatch)

    repo_root = tmp_path / "repo"
    manifest_path = repo_root / "dags" / "dlt_manifests" / "sample.yaml"
    manifest_path.parent.mkdir(parents=True, exist_ok=True)
    manifest_path.write_text("pipeline: {}\n", encoding="utf-8")

    embedded_root = repo_root / "dags" / "dp-dlt-af"
    (embedded_root / "dlt_utils").mkdir(parents=True, exist_ok=True)
    (embedded_root / "dag_builder").mkdir(parents=True, exist_ok=True)

    env_root = tmp_path / "external-package"
    (env_root / "dlt_utils").mkdir(parents=True, exist_ok=True)
    (env_root / "dag_builder").mkdir(parents=True, exist_ok=True)

    monkeypatch.setenv("DLTAF_PACKAGE_ROOT", str(env_root))

    candidates = task_factory._candidate_framework_roots(manifest_path)

    assert candidates[0] == env_root.resolve()
    assert embedded_root.resolve() in candidates


def test_virtualenv_callable_uses_dltaf_package_root_fallback(monkeypatch, tmp_path) -> None:
    task_factory = _load_task_factory(monkeypatch)

    _strip_repo_root_from_sys_path(monkeypatch)
    _clear_dlt_utils_modules(monkeypatch)

    repo_root = tmp_path / "repo"
    manifest_path = repo_root / "dags" / "dlt_manifests" / "sample.yaml"
    manifest_path.parent.mkdir(parents=True, exist_ok=True)
    manifest_path.write_text("pipeline: {}\n", encoding="utf-8")

    external_root = tmp_path / "external-package"
    _write_fake_runner(external_root, "RUN_MANIFEST_CAPTURE")
    _force_fallback_import(monkeypatch, external_root)

    capture_path = tmp_path / "capture.txt"
    monkeypatch.setenv("DLTAF_PACKAGE_ROOT", str(external_root))
    monkeypatch.setenv("RUN_MANIFEST_CAPTURE", str(capture_path))

    run_manifest = task_factory._import_run_manifest(str(manifest_path), logging.getLogger(__name__))
    os.environ["EXTRA_RUNTIME_ENV"] = "enabled"
    run_manifest(str(manifest_path), True)

    assert capture_path.read_text(encoding="utf-8") == f"{manifest_path}|True"
    runner_file = Path(sys.modules["dlt_utils.manifest_runner"].__file__).resolve()
    assert runner_file.is_relative_to(external_root.resolve())
    assert os.environ["EXTRA_RUNTIME_ENV"] == "enabled"


def test_virtualenv_callable_uses_repo_local_dp_dlt_af_fallback(monkeypatch, tmp_path) -> None:
    task_factory = _load_task_factory(monkeypatch)

    _strip_repo_root_from_sys_path(monkeypatch)
    _clear_dlt_utils_modules(monkeypatch)
    monkeypatch.delenv("DLTAF_PACKAGE_ROOT", raising=False)

    repo_root = tmp_path / "repo"
    manifest_path = repo_root / "dags" / "dlt_manifests" / "sample.yaml"
    manifest_path.parent.mkdir(parents=True, exist_ok=True)
    manifest_path.write_text("pipeline: {}\n", encoding="utf-8")

    embedded_root = repo_root / "dags" / "dp-dlt-af"
    _write_fake_runner(embedded_root, "RUN_MANIFEST_CAPTURE_REPO")
    (embedded_root / "dag_builder").mkdir(parents=True, exist_ok=True)
    _force_fallback_import(monkeypatch, embedded_root)

    capture_path = tmp_path / "repo-capture.txt"
    monkeypatch.setenv("RUN_MANIFEST_CAPTURE_REPO", str(capture_path))

    run_manifest = task_factory._import_run_manifest(str(manifest_path), logging.getLogger(__name__))
    run_manifest(str(manifest_path), False)

    assert capture_path.read_text(encoding="utf-8") == f"{manifest_path}|False"
    runner_file = Path(sys.modules["dlt_utils.manifest_runner"].__file__).resolve()
    assert runner_file.is_relative_to(embedded_root.resolve())


def test_virtualenv_callable_is_self_contained_for_subprocess_serialization(
    monkeypatch, tmp_path
) -> None:
    task_factory = _load_task_factory(monkeypatch)

    _strip_repo_root_from_sys_path(monkeypatch)
    _clear_dlt_utils_modules(monkeypatch)

    manifest_path = tmp_path / "repo" / "dags" / "dlt_manifests" / "sample.yaml"
    manifest_path.parent.mkdir(parents=True, exist_ok=True)
    manifest_path.write_text("pipeline: {}\n", encoding="utf-8")

    framework_root = tmp_path / "external-package"
    _write_fake_runner(framework_root, "RUN_MANIFEST_CAPTURE_SELF_CONTAINED")
    (framework_root / "dag_builder").mkdir(parents=True, exist_ok=True)
    monkeypatch.syspath_prepend(str(framework_root))

    capture_path = tmp_path / "self-contained-capture.txt"
    monkeypatch.setenv("DLTAF_PACKAGE_ROOT", str(framework_root))
    monkeypatch.setenv("RUN_MANIFEST_CAPTURE_SELF_CONTAINED", str(capture_path))

    isolated_namespace = {"__builtins__": __builtins__}
    exec(inspect.getsource(task_factory._virtualenv_callable), isolated_namespace)
    isolated_callable = isolated_namespace["_virtualenv_callable"]

    isolated_callable(str(manifest_path), True, {"EXTRA_RUNTIME_ENV": "enabled"})

    assert capture_path.read_text(encoding="utf-8") == f"{manifest_path}|True"
    assert os.environ["EXTRA_RUNTIME_ENV"] == "enabled"


def test_dependency_resolver_package_mode_uses_install_spec_plus_plugin_requirements(monkeypatch) -> None:
    task_factory = _load_task_factory(monkeypatch)

    monkeypatch.setenv("DLTAF_INSTALL_SPEC", "dltaf==0.2.2")
    monkeypatch.setenv(
        "DLTAF_PLUGIN_REQUIREMENTS",
        '["company-private-plugin==1.2.3", "another-private-plugin>=4"]',
    )
    monkeypatch.setattr(
        task_factory.DependencyResolver,
        "load_dependencies",
        classmethod(lambda cls: {"core": "core-package>=1.0.0"}),
    )

    requirements = task_factory.DependencyResolver.get_all_requirements()

    assert requirements[0] == "dltaf==0.2.2"
    assert "core-package>=1.0.0" not in requirements
    assert "company-private-plugin==1.2.3" in requirements
    assert "another-private-plugin>=4" in requirements


def test_dependency_resolver_embedded_mode_uses_local_pyproject_dependencies(monkeypatch) -> None:
    task_factory = _load_task_factory(monkeypatch)

    monkeypatch.delenv("DLTAF_INSTALL_SPEC", raising=False)
    monkeypatch.delenv("DLTAF_PLUGIN_REQUIREMENTS", raising=False)
    monkeypatch.setattr(
        task_factory.DependencyResolver,
        "load_dependencies",
        classmethod(lambda cls: {"core": "core-package>=1.0.0"}),
    )

    requirements = task_factory.DependencyResolver.get_all_requirements()

    assert requirements == ["core-package>=1.0.0"]


def test_force_virtualenv_can_be_enabled_via_env(monkeypatch) -> None:
    task_factory = _load_task_factory(monkeypatch)
    monkeypatch.setenv("DLTAF_FORCE_VIRTUALENV", "true")

    factory = task_factory.TaskFactory()
    strategy = factory._select_strategy({"airflow": {}}, force_virtualenv=None)

    assert isinstance(strategy, task_factory.VirtualenvOperatorStrategy)


def test_virtualenv_operator_bridges_package_mode_runtime_env(monkeypatch, tmp_path) -> None:
    task_factory = _load_task_factory(monkeypatch)

    manifest_path = tmp_path / "sample.yaml"
    manifest_path.write_text(
        "\n".join(
            [
                "version: 1",
                "pipeline:",
                "  name: dlt__sample__to__clickhouse__raw",
                "  destination: clickhouse",
                "  dataset: raw",
                "source:",
                "  kind: mongodb",
                "  database: ${ENV:MONGO_DB|demo}",
                "airflow:",
                "  task: {}",
            ]
        ),
        encoding="utf-8",
    )

    monkeypatch.setattr(
        task_factory.DependencyResolver,
        "get_install_spec",
        classmethod(lambda cls: "dltaf==0.2.2"),
    )
    monkeypatch.setattr(
        task_factory.DependencyResolver,
        "get_plugin_requirements",
        classmethod(lambda cls: ["requests>=2.31.0"]),
    )

    operator = task_factory.VirtualenvOperatorStrategy().create_operator(
        task_id="run__sample",
        manifest_path=manifest_path,
        task_config={},
    )

    runtime_env = operator.kwargs["op_args"][2]

    assert "DLTAF_EXTRA_SYS_PATHS" in runtime_env
    assert "DLT_RUNNER_PLUGINS" in runtime_env
    assert "DLT_HOOK_PLUGINS" in runtime_env
    assert "DLT_INFRA_CHECK_PLUGINS" in runtime_env
    assert runtime_env["AIRFLOW_VAR_MONGO_DB"] == "{{ var.value.get('MONGO_DB', '') }}"
    assert operator.kwargs["requirements"][0] == "dltaf[clickhouse,mongodb]==0.2.2"
    assert "sqlalchemy>=2.0.25" not in operator.kwargs["requirements"]


def test_virtualenv_operator_supports_cached_venv_and_pip_options(monkeypatch, tmp_path) -> None:
    task_factory = _load_task_factory(monkeypatch)

    manifest_path = tmp_path / "sample.yaml"
    manifest_path.write_text(
        "\n".join(
            [
                "version: 1",
                "pipeline:",
                "  name: dlt__sample__to__clickhouse__raw",
                "  destination: clickhouse",
                "  dataset: raw",
                "source:",
                "  kind: mongodb",
                "airflow:",
                "  task:",
                "    venv_cache_path: /tmp/dltaf-venv-cache",
                "    pip_install_options:",
                "      - --no-index",
                "      - --prefer-binary",
                "    requirements:",
                "      - dltaf[clickhouse,mongodb]==0.2.2",
            ]
        ),
        encoding="utf-8",
    )

    operator = task_factory.VirtualenvOperatorStrategy().create_operator(
        task_id="run__sample",
        manifest_path=manifest_path,
        task_config={
            "requirements": ["dltaf[clickhouse,mongodb]==0.2.2"],
            "venv_cache_path": "/tmp/dltaf-venv-cache",
            "pip_install_options": ["--no-index", "--prefer-binary"],
        },
    )

    assert operator.kwargs["venv_cache_path"] == "/tmp/dltaf-venv-cache"
    assert operator.kwargs["pip_install_options"] == ["--no-index", "--prefer-binary"]
    assert operator.kwargs["requirements"] == ["dltaf[clickhouse,mongodb]==0.2.2"]


def test_virtualenv_operator_normalizes_legacy_opt_repo_cache_path(monkeypatch, tmp_path) -> None:
    monkeypatch.setenv("DLTAF_REPO_ROOT", str(tmp_path))
    task_factory = _load_task_factory(monkeypatch)
    manifest_path = tmp_path / "sample.yaml"
    manifest_path.write_text(
        "\n".join(
            [
                "version: 1",
                "pipeline:",
                "  name: dlt__sample__to__clickhouse__raw",
                "  destination: clickhouse",
                "  dataset: raw",
                "source:",
                "  kind: mongodb",
            ]
        ),
        encoding="utf-8",
    )

    operator = task_factory.VirtualenvOperatorStrategy().create_operator(
        task_id="run__sample",
        manifest_path=manifest_path,
        task_config={
            "requirements": ["dltaf[runtime]==0.2.2"],
            "venv_cache_path": "/opt/dp-metadata/tests/e2e/dlt_airflow/.venv-cache",
        },
    )

    assert operator.kwargs["venv_cache_path"] == str(
        tmp_path / "tests" / "e2e" / "dlt_airflow" / ".venv-cache"
    )


def test_vault_env_bridge_maps_vault_address_to_vault_addr(monkeypatch) -> None:
    task_factory = _load_task_factory(monkeypatch)

    bridge = task_factory._vault_env_bridge()

    assert bridge["VAULT_ADDRESS"] == "{{ var.value.get('VAULT_ADDRESS', '') }}"
    assert bridge["VAULT_ADDR"] == "{{ var.value.get('VAULT_ADDR', var.value.get('VAULT_ADDRESS', '')) }}"
    assert bridge["VAULT_TOKEN"] == "{{ var.value.get('VAULT_TOKEN', '') }}"
    assert bridge["VAULT_ROLE_ID"] == "{{ var.value.get('VAULT_ROLE_ID', '') }}"
    assert bridge["VAULT_SECRET_ID"] == "{{ var.value.get('VAULT_SECRET_ID', '') }}"
    assert bridge["VAULT_NAMESPACE"] == "{{ var.value.get('VAULT_NAMESPACE', '') }}"
    assert bridge["VAULT_VERIFY"] == "{{ var.value.get('VAULT_VERIFY', '') }}"


def test_airflow_connection_prefix_bridge_env_covers_sql_and_clickhouse(monkeypatch, tmp_path) -> None:
    task_factory = _load_task_factory(monkeypatch)

    manifest_path = tmp_path / "connection-bridge.yaml"
    manifest_path.write_text(
        "\n".join(
            [
                "version: 1",
                "pipeline:",
                "  name: dlt__sample__to__clickhouse__spr",
                "connections:",
                "  source:",
                "    kind: postgres",
                "    airflow_variable_prefix: SQL_DATABASE__",
                "    overrides:",
                "      drivername: postgresql+psycopg2",
                "      database: cabinet",
                "  destination:",
                "    kind: clickhouse",
                "    airflow_variable_prefix: CLICKHOUSE__",
                "    overrides:",
                "      database: spr",
                "      dataset_table_separator: __",
                "source:",
                "  kind: sql_database",
                "airflow:",
                "  task: {}",
            ]
        ),
        encoding="utf-8",
    )

    env_bridge = task_factory._build_airflow_var_bridge_env(manifest_path)

    assert (
        env_bridge["SOURCES__SQL_DATABASE__CREDENTIALS__DRIVERNAME"]
        == "{{ var.value.get('SQL_DATABASE__DRIVERNAME', 'postgresql+psycopg2') }}"
    )
    assert (
        env_bridge["SOURCES__SQL_DATABASE__CREDENTIALS__HOST"]
        == "{{ var.value.get('SQL_DATABASE__HOST', '') }}"
    )
    assert (
        env_bridge["DESTINATION__CLICKHOUSE__CREDENTIALS__HOST"]
        == "{{ var.value.get('CLICKHOUSE__HOST', '') }}"
    )
    assert (
        env_bridge["DESTINATION__CLICKHOUSE__CREDENTIALS__DATABASE"]
        == "{{ var.value.get('CLICKHOUSE__DATABASE', 'spr') }}"
    )
    assert (
        env_bridge["DESTINATION__CLICKHOUSE__CREDENTIALS__SECURE"]
        == "{{ var.value.get('CLICKHOUSE__SECURE', '0') }}"
    )
