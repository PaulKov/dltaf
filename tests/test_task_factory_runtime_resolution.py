from __future__ import annotations

import builtins
import importlib
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

    package_root = Path(__file__).resolve().parents[1].resolve()
    monkeypatch.setattr(
        sys,
        "path",
        [entry for entry in sys.path if Path(entry).resolve() != package_root],
    )
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

    package_root = Path(__file__).resolve().parents[1].resolve()
    monkeypatch.setattr(
        sys,
        "path",
        [entry for entry in sys.path if Path(entry).resolve() != package_root],
    )
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


def test_dependency_resolver_appends_plugin_requirements(monkeypatch) -> None:
    task_factory = _load_task_factory(monkeypatch)

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

    assert "core-package>=1.0.0" in requirements
    assert "company-private-plugin==1.2.3" in requirements
    assert "another-private-plugin>=4" in requirements
