"""Фабрика для создания Airflow задач из YAML манифестов.

Этот модуль предоставляет класс TaskFactory для создания Airflow операторов
(PythonOperator или PythonVirtualenvOperator) из манифестов с правильной 
конфигурацией и обработкой ошибок.

Architecture:
    - TaskFactory: главная фабрика (фасад)
    - BaseOperatorStrategy: базовая стратегия создания оператора (Strategy pattern)
    - PythonOperatorStrategy: стратегия для PythonOperator
    - VirtualenvOperatorStrategy: стратегия для PythonVirtualenvOperator
    - DependencyResolver: читает зависимости из pyproject.toml (Single Source of Truth)
"""

import json
import logging
import os
import re
import sys
from abc import ABC, abstractmethod
from datetime import timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional, Union

import yaml
from airflow.operators.python import PythonOperator, PythonVirtualenvOperator
from airflow.utils.task_group import TaskGroup

from dlt_utils.install_profiles import (
    expand_dltaf_requirement,
    load_manifest_mapping,
    manifest_needs_sqlalchemy_upgrade,
)
from dlt_utils.naming import normalize_token, validate_pipeline_name

logger = logging.getLogger(__name__)


_ENV_REF_PATTERN = re.compile(r"\$\{ENV:([A-Z0-9_]+)(?:\|[^}]*)?\}")


# ============================================================================
# Top-level callables для операторов (ОБЯЗАТЕЛЬНО top-level для pickle!)
# ============================================================================

def _python_callable_wrapper(manifest_path, configure_logging=False, **context):
    """Wrapper для PythonOperator.
    
    Top-level function - может быть pickle.
    """
    from dlt_utils.manifest_runner import run_manifest
    run_manifest(manifest_path=manifest_path, configure_logging=configure_logging)
    return None


def _collect_manifest_env_refs(obj: Any) -> set[str]:
    if isinstance(obj, dict):
        refs: set[str] = set()
        for value in obj.values():
            refs.update(_collect_manifest_env_refs(value))
        return refs
    if isinstance(obj, list):
        refs: set[str] = set()
        for value in obj:
            refs.update(_collect_manifest_env_refs(value))
        return refs
    if isinstance(obj, str):
        return set(_ENV_REF_PATTERN.findall(obj))
    return set()


def _build_airflow_var_bridge_env(manifest_path: Path) -> Dict[str, str]:
    """Build AIRFLOW_VAR_* env bridge for virtualenv tasks from active manifest refs."""

    raw_manifest = yaml.safe_load(manifest_path.read_text(encoding="utf-8"))
    if not isinstance(raw_manifest, dict):
        return {}

    env_names = sorted(_collect_manifest_env_refs(raw_manifest))
    env_bridge = {
        f"AIRFLOW_VAR_{name}": "{{ var.value.get('" + name + "', '') }}"
        for name in env_names
    }
    env_bridge.update(_build_airflow_connection_bridge_env(raw_manifest))
    return env_bridge


def _airflow_var_template(name: str, default: str = "") -> str:
    return "{{ var.value.get('" + name + "', '" + default + "') }}"


def _vault_env_bridge() -> Dict[str, str]:
    """Build Vault bootstrap env for virtualenv tasks.

    `vault-kv-client` uses `VAULT_ADDR`, while Airflow environments in this repo
    often publish `VAULT_ADDRESS`. We bridge both names so package-mode runtime
    works with either variable contract.
    """

    vault_address = _airflow_var_template("VAULT_ADDRESS")
    vault_addr = "{{ var.value.get('VAULT_ADDR', var.value.get('VAULT_ADDRESS', '')) }}"

    return {
        "VAULT_ADDRESS": vault_address,
        "VAULT_ADDR": vault_addr,
        "VAULT_TOKEN": _airflow_var_template("VAULT_TOKEN"),
        "VAULT_ROLE_ID": _airflow_var_template("VAULT_ROLE_ID"),
        "VAULT_SECRET_ID": _airflow_var_template("VAULT_SECRET_ID"),
        "VAULT_NAMESPACE": _airflow_var_template("VAULT_NAMESPACE"),
        "VAULT_VERIFY": _airflow_var_template("VAULT_VERIFY"),
    }


def _normalized_prefix(prefix: str) -> str:
    value = str(prefix or "").strip()
    if not value:
        return ""
    return value if value.endswith("__") else f"{value}__"


def _build_prefixed_bridge(
    prefix: str,
    suffix_to_env_key: Dict[str, Union[str, tuple[str, str]]],
) -> Dict[str, str]:
    normalized_prefix = _normalized_prefix(prefix)
    if not normalized_prefix:
        return {}

    env: Dict[str, str] = {}
    for suffix, env_key_spec in suffix_to_env_key.items():
        if isinstance(env_key_spec, tuple):
            env_key, default = env_key_spec
        else:
            env_key, default = env_key_spec, ""
        env[env_key] = _airflow_var_template(f"{normalized_prefix}{suffix}", default)
    return env


def _build_airflow_connection_bridge_env(manifest: Dict[str, Any]) -> Dict[str, str]:
    """Bridge `connections.*.airflow_variable_prefix` values into runtime env.

    This keeps connection-based manifests working inside PythonVirtualenvOperator
    where direct Airflow Variable access is not available.
    """

    sql_source_map = {
        "DRIVERNAME": ("SOURCES__SQL_DATABASE__CREDENTIALS__DRIVERNAME", ""),
        "HOST": ("SOURCES__SQL_DATABASE__CREDENTIALS__HOST", ""),
        "PORT": ("SOURCES__SQL_DATABASE__CREDENTIALS__PORT", ""),
        "USERNAME": ("SOURCES__SQL_DATABASE__CREDENTIALS__USERNAME", ""),
        "PASSWORD": ("SOURCES__SQL_DATABASE__CREDENTIALS__PASSWORD", ""),
        "DATABASE": ("SOURCES__SQL_DATABASE__CREDENTIALS__DATABASE", ""),
        "DSN": ("SOURCES__SQL_DATABASE__CREDENTIALS__DSN", ""),
    }
    mongodb_source_map = {
        "CONNECTION_URL": ("SOURCES__MONGODB__CONNECTION_URL", ""),
    }
    keycloak_source_map = {
        "TOKEN_URL": ("SOURCES__KEYCLOAK__TOKEN_URL", ""),
        "CLIENT_ID": ("SOURCES__KEYCLOAK__CLIENT_ID", ""),
        "CLIENT_SECRET": ("SOURCES__KEYCLOAK__CLIENT_SECRET", ""),
        "USERNAME": ("SOURCES__KEYCLOAK__USERNAME", ""),
        "PASSWORD": ("SOURCES__KEYCLOAK__PASSWORD", ""),
        "GRANT_TYPE": ("SOURCES__KEYCLOAK__GRANT_TYPE", ""),
        "SCOPE": ("SOURCES__KEYCLOAK__SCOPE", ""),
        "VERIFY_SSL": ("SOURCES__KEYCLOAK__VERIFY_SSL", ""),
        "TIMEOUT_SECONDS": ("SOURCES__KEYCLOAK__TIMEOUT_SECONDS", ""),
    }

    env: Dict[str, str] = {}
    connections = manifest.get("connections") or {}
    if not isinstance(connections, dict):
        return env

    source = connections.get("source")
    if isinstance(source, dict):
        source_kind = str(source.get("kind") or "").strip().lower()
        source_prefix = str(source.get("airflow_variable_prefix") or "")
        source_overrides = source.get("overrides") or {}
        if source_kind in {"postgres", "sql", "oracle"}:
            sql_source_map["DRIVERNAME"] = (
                "SOURCES__SQL_DATABASE__CREDENTIALS__DRIVERNAME",
                str(source_overrides.get("drivername") or ""),
            )
            sql_source_map["DATABASE"] = (
                "SOURCES__SQL_DATABASE__CREDENTIALS__DATABASE",
                str(source_overrides.get("database") or ""),
            )
            env.update(_build_prefixed_bridge(source_prefix, sql_source_map))
        elif source_kind in {"mongodb", "mongo"}:
            env.update(_build_prefixed_bridge(source_prefix, mongodb_source_map))
        elif source_kind in {"keycloak"}:
            env.update(_build_prefixed_bridge(source_prefix, keycloak_source_map))

    destination = connections.get("destination")
    if isinstance(destination, dict):
        destination_kind = str(destination.get("kind") or "").strip().lower()
        destination_prefix = str(destination.get("airflow_variable_prefix") or "")
        destination_overrides = destination.get("overrides") or {}
        if destination_kind == "clickhouse":
            clickhouse_destination_map = {
                "HOST": ("DESTINATION__CLICKHOUSE__CREDENTIALS__HOST", ""),
                "PORT": ("DESTINATION__CLICKHOUSE__CREDENTIALS__PORT", ""),
                "HTTP_PORT": ("DESTINATION__CLICKHOUSE__CREDENTIALS__HTTP_PORT", ""),
                "SECURE": (
                    "DESTINATION__CLICKHOUSE__CREDENTIALS__SECURE",
                    str(destination_overrides.get("secure", 0)),
                ),
                "DATABASE": (
                    "DESTINATION__CLICKHOUSE__CREDENTIALS__DATABASE",
                    str(destination_overrides.get("database") or ""),
                ),
                "USERNAME": ("DESTINATION__CLICKHOUSE__CREDENTIALS__USERNAME", ""),
                "PASSWORD": ("DESTINATION__CLICKHOUSE__CREDENTIALS__PASSWORD", ""),
                "DATASET_TABLE_SEPARATOR": (
                    "DESTINATION__CLICKHOUSE__DATASET_TABLE_SEPARATOR",
                    str(destination_overrides.get("dataset_table_separator") or "__"),
                ),
            }
            env.update(_build_prefixed_bridge(destination_prefix, clickhouse_destination_map))

    return env


def _parse_env_list(raw: str) -> List[str]:
    value = str(raw or "").strip()
    if not value:
        return []

    if value.startswith("["):
        try:
            payload = json.loads(value)
        except Exception:
            payload = None
        if isinstance(payload, list):
            return [str(item).strip() for item in payload if str(item).strip()]

    return [part.strip() for part in value.split(",") if part.strip()]


def _append_extra_sys_paths_from_env(logger_: logging.Logger) -> List[Path]:
    """Append explicit consumer-owned code paths for private plugins/business code.

    The OSS framework stays package-first. We only extend `sys.path` with
    additional consumer paths when the caller explicitly provides them through
    `DLTAF_EXTRA_SYS_PATHS`.
    """

    raw = os.getenv("DLTAF_EXTRA_SYS_PATHS", "")
    appended: List[Path] = []

    for item in _parse_env_list(raw):
        candidate = Path(item).expanduser().resolve()
        if not candidate.exists():
            logger_.warning("DLTAF_EXTRA_SYS_PATHS entry does not exist: %s", candidate)
            continue

        candidate_str = str(candidate)
        if candidate_str in sys.path:
            continue

        sys.path.append(candidate_str)
        appended.append(candidate)
        logger_.info("Appended consumer runtime path to sys.path: %s", candidate)

    return appended


def _is_framework_root(path: Path) -> bool:
    """Return True when the path looks like an embedded dltaf framework root."""

    return (path / "dlt_utils").is_dir() and (path / "dag_builder").is_dir()


def _candidate_framework_roots(manifest_path: Path) -> List[Path]:
    """Resolve framework roots for virtualenv fallback imports.

    Resolution order is intentionally package-first:
    1. installed package import is attempted before this helper is used
    2. `DLTAF_PACKAGE_ROOT`, if explicitly configured
    3. repo-local embedded `dags/dp-dlt-af` discovered from the manifest path
    """

    import os

    candidates: List[Path] = []
    seen: set[Path] = set()

    def _append(path: Path) -> None:
        resolved = path.expanduser().resolve()
        if resolved in seen:
            return
        seen.add(resolved)
        candidates.append(resolved)

    package_root = os.getenv("DLTAF_PACKAGE_ROOT", "").strip()
    if package_root:
        _append(Path(package_root))

    manifest_abs = manifest_path.expanduser().resolve()
    for parent in [manifest_abs.parent] + list(manifest_abs.parents):
        direct_candidate = parent / "dp-dlt-af"
        if _is_framework_root(direct_candidate):
            _append(direct_candidate)
        if _is_framework_root(parent):
            _append(parent)

    return candidates


def _import_run_manifest(manifest_path_arg: str, logger: logging.Logger):
    """Import `run_manifest` with package-first and embedded fallback semantics."""

    def _clear_partial_dlt_utils_modules() -> None:
        for module_name in list(sys.modules):
            if module_name == "dlt_utils" or module_name.startswith("dlt_utils."):
                sys.modules.pop(module_name, None)

    _append_extra_sys_paths_from_env(logger)

    try:
        from dlt_utils.manifest_runner import run_manifest

        logger.info("Loaded dlt_utils.manifest_runner from installed/importable package")
        return run_manifest
    except ImportError as import_error:
        manifest_path = Path(manifest_path_arg).resolve()
        last_error: ImportError = import_error
        _clear_partial_dlt_utils_modules()

        for framework_root in _candidate_framework_roots(manifest_path):
            root_str = str(framework_root)
            if root_str not in sys.path:
                sys.path.insert(0, root_str)
                logger.info(f"Added framework root to sys.path: {framework_root}")

            try:
                from dlt_utils.manifest_runner import run_manifest

                logger.info(
                    "Loaded dlt_utils.manifest_runner via fallback framework root: %s",
                    framework_root,
                )
                return run_manifest
            except ImportError as retry_error:
                last_error = retry_error
                logger.warning(
                    "Failed to import dlt_utils from fallback root %s: %s",
                    framework_root,
                    retry_error,
                )

        logger.error(f"Failed to import dlt_utils.manifest_runner: {last_error}")
        logger.error(f"Candidate framework roots: {_candidate_framework_roots(manifest_path)}")
        logger.error(f"Manifest path: {manifest_path}")
        raise last_error


def _virtualenv_callable(manifest_path_arg, configure_logging_arg, runtime_env_dict):
    """Wrapper для PythonVirtualenvOperator.
    
    КРИТИЧНО: Эта функция ДОЛЖНА быть top-level (module-level),
    иначе pickle не сможет её сериализовать.
    
    Функция НЕ принимает **context, потому что это может вызвать проблемы с pickle.
    Airflow передаст context автоматически, но мы его не используем.
    
    Args:
        manifest_path_arg: Путь к манифесту
        configure_logging_arg: Настраивать ли логирование
        runtime_env_dict: Словарь ENV для runtime (Vault + AIRFLOW_VAR bridge)
    """
    import json
    import logging
    import os
    import sys
    from pathlib import Path
    
    logger = logging.getLogger(__name__)

    def _parse_env_list_local(raw: str) -> list[str]:
        value = str(raw or "").strip()
        if not value:
            return []

        if value.startswith("["):
            try:
                payload = json.loads(value)
            except Exception:
                payload = None
            if isinstance(payload, list):
                return [str(item).strip() for item in payload if str(item).strip()]

        return [part.strip() for part in value.split(",") if part.strip()]

    def _append_extra_sys_paths_from_env_local() -> None:
        raw = os.getenv("DLTAF_EXTRA_SYS_PATHS", "")
        for item in _parse_env_list_local(raw):
            candidate = Path(item).expanduser().resolve()
            if not candidate.exists():
                logger.warning("DLTAF_EXTRA_SYS_PATHS entry does not exist: %s", candidate)
                continue

            candidate_str = str(candidate)
            if candidate_str in sys.path:
                continue

            sys.path.append(candidate_str)
            logger.info("Appended consumer runtime path to sys.path: %s", candidate)

    def _is_framework_root_local(path: Path) -> bool:
        return (path / "dlt_utils").is_dir() and (path / "dag_builder").is_dir()

    def _candidate_framework_roots_local(manifest_path: Path) -> list[Path]:
        candidates: list[Path] = []
        seen: set[Path] = set()

        def _append(path: Path) -> None:
            resolved = path.expanduser().resolve()
            if resolved in seen:
                return
            seen.add(resolved)
            candidates.append(resolved)

        package_root = os.getenv("DLTAF_PACKAGE_ROOT", "").strip()
        if package_root:
            _append(Path(package_root))

        manifest_abs = manifest_path.expanduser().resolve()
        for parent in [manifest_abs.parent] + list(manifest_abs.parents):
            direct_candidate = parent / "dp-dlt-af"
            if _is_framework_root_local(direct_candidate):
                _append(direct_candidate)
            if _is_framework_root_local(parent):
                _append(parent)

        return candidates

    def _import_run_manifest_local():
        def _clear_partial_dlt_utils_modules_local() -> None:
            for module_name in list(sys.modules):
                if module_name == "dlt_utils" or module_name.startswith("dlt_utils."):
                    sys.modules.pop(module_name, None)

        _append_extra_sys_paths_from_env_local()

        try:
            from dlt_utils.manifest_runner import run_manifest

            logger.info("Loaded dlt_utils.manifest_runner from installed/importable package")
            return run_manifest
        except ImportError as import_error:
            manifest_path = Path(manifest_path_arg).resolve()
            last_error: ImportError = import_error
            _clear_partial_dlt_utils_modules_local()

            for framework_root in _candidate_framework_roots_local(manifest_path):
                root_str = str(framework_root)
                if root_str not in sys.path:
                    sys.path.insert(0, root_str)
                    logger.info("Added framework root to sys.path: %s", framework_root)

                try:
                    from dlt_utils.manifest_runner import run_manifest

                    logger.info(
                        "Loaded dlt_utils.manifest_runner via fallback framework root: %s",
                        framework_root,
                    )
                    return run_manifest
                except ImportError as retry_error:
                    last_error = retry_error
                    logger.warning(
                        "Failed to import dlt_utils from fallback root %s: %s",
                        framework_root,
                        retry_error,
                    )

            logger.error("Failed to import dlt_utils.manifest_runner: %s", last_error)
            logger.error(
                "Candidate framework roots: %s",
                _candidate_framework_roots_local(manifest_path),
            )
            logger.error("Manifest path: %s", manifest_path)
            raise last_error
    
    if runtime_env_dict:
        for key, value in runtime_env_dict.items():
            if value:  # Устанавливаем только непустые значения
                os.environ[key] = str(value)
                logger.info(f"Set ENV variable: {key}")

    run_manifest = _import_run_manifest_local()

    logger.info(f"Starting pipeline from manifest: {manifest_path_arg}")
    
    try:
        run_manifest(
            manifest_path=manifest_path_arg,
            configure_logging=configure_logging_arg,
        )
        logger.info(f"Pipeline completed successfully: {manifest_path_arg}")
        return None
    except Exception as e:
        logger.error(f"Pipeline execution error for {manifest_path_arg}: {e}")
        raise


class TaskFactoryError(Exception):
    """Ошибка при создании задачи."""
    pass


# ============================================================================
# Dependency Resolver: читает dependencies из pyproject.toml (Single Source of Truth)
# ============================================================================

class DependencyResolver:
    """Резолвер зависимостей из pyproject.toml.
    
    Single Source of Truth: все зависимости берутся из pyproject.toml,
    нет дублирования в коде.
    
    Attributes:
        _dependencies_cache: Кеш распарсенных зависимостей из pyproject.toml
        _project_root: Путь к корню проекта (где находится pyproject.toml)
    """
    
    _dependencies_cache: Optional[Dict[str, List[str]]] = None
    _project_root: Optional[Path] = None
    _legacy_repo_root = "/opt/dp-metadata"
    
    @classmethod
    def get_project_root(cls) -> Path:
        """Найти корень проекта (где находится pyproject.toml).
        
        Returns:
            Путь к корню проекта
        """
        if cls._project_root is not None:
            return cls._project_root
        
        # Начинаем с текущего файла и идем вверх
        current = Path(__file__).resolve()
        
        for parent in [current.parent] + list(current.parents):
            pyproject_path = parent / "pyproject.toml"
            if pyproject_path.exists():
                cls._project_root = parent
                logger.debug(f"Found pyproject.toml at: {parent}")
                return parent
        
        # Fallback: используем родительскую директорию dag_builder
        fallback = Path(__file__).resolve().parent.parent
        logger.warning(f"pyproject.toml not found, using fallback: {fallback}")
        cls._project_root = fallback
        return fallback
    
    @classmethod
    def load_dependencies(cls) -> Dict[str, List[str]]:
        """Загрузить и распарсить зависимости из pyproject.toml.
        
        Returns:
            Словарь {package_name: version_spec}
            Например: {"oracledb": ">=2.0.0", "sqlalchemy": ">=2.0.0"}
        """
        if cls._dependencies_cache is not None:
            return cls._dependencies_cache
        
        try:
            # Python 3.11+ имеет встроенный tomllib
            try:
                import tomllib
            except ImportError:
                # Fallback для Python 3.10
                try:
                    import tomli as tomllib
                except ImportError:
                    logger.warning(
                        "Neither tomllib nor tomli available. "
                        "Install tomli for Python < 3.11: pip install tomli"
                    )
                    return cls._get_fallback_dependencies()
            
            pyproject_path = cls.get_project_root() / "pyproject.toml"
            
            if not pyproject_path.exists():
                logger.warning(f"pyproject.toml not found at {pyproject_path}")
                return cls._get_fallback_dependencies()
            
            # Читаем pyproject.toml
            with open(pyproject_path, "rb") as f:
                data = tomllib.load(f)
            
            # Извлекаем dependencies
            dependencies = data.get("project", {}).get("dependencies", [])
            
            # Парсим в словарь {package_name: version_spec}
            parsed = cls._parse_dependencies(dependencies)
            
            cls._dependencies_cache = parsed
            logger.info(f"Loaded {len(parsed)} dependencies from pyproject.toml")
            
            return parsed
            
        except Exception as e:
            logger.error(f"Failed to load dependencies from pyproject.toml: {e}")
            return cls._get_fallback_dependencies()
    
    @staticmethod
    def _parse_dependencies(dependencies: List[str]) -> Dict[str, str]:
        """Парсит список dependencies в словарь.
        
        Args:
            dependencies: Список строк типа ["oracledb>=2.0.0", "sqlalchemy>=2.0.0"]
            
        Returns:
            Словарь {package_name: full_spec}
        """
        parsed = {}
        
        for dep in dependencies:
            # Убираем extras: dlt[clickhouse] -> dlt
            if "[" in dep:
                package = dep.split("[")[0].strip()
            else:
                # Убираем version specs
                for op in [">=", "<=", "==", "!=", "~=", ">", "<"]:
                    if op in dep:
                        package = dep.split(op)[0].strip()
                        break
                else:
                    package = dep.strip()
            
            parsed[package.lower()] = dep
        
        return parsed
    
    @staticmethod
    def _get_fallback_dependencies() -> Dict[str, str]:
        """Fallback dependencies если не удалось прочитать pyproject.toml.
        
        Returns:
            Минимальный набор зависимостей
        """
        return {
            "dlt": "dlt[clickhouse,sql_database]>=1.18.2,<2",
            "pyyaml": "PyYAML>=6.0.1",
            "vault-kv-client": "vault-kv-client>=0.1.0",
            "oracledb": "oracledb>=2.0.0",
            "sqlalchemy": "sqlalchemy>=2.0.25",
            "psycopg2-binary": "psycopg2-binary>=2.9.9",
            "pymongo": "pymongo>=4.6.0",
            "requests": "requests>=2.31.0",
            "kafka-python": "kafka-python>=2.0.2",
        }

    @staticmethod
    def _airflow_variable_lookup_enabled() -> bool:
        return any(
            key == "AIRFLOW_HOME" or key.startswith(("AIRFLOW__", "AIRFLOW_CTX_"))
            for key in os.environ
        )

    @staticmethod
    def _get_env_or_airflow_value(name: str) -> str:
        value = os.getenv(name, "").strip()
        if value:
            return value

        value = os.getenv(f"AIRFLOW_VAR_{name}", "").strip()
        if value:
            return value

        if DependencyResolver._airflow_variable_lookup_enabled():
            try:
                from airflow.models import Variable

                value = str(Variable.get(name, default_var="")).strip()
                if value:
                    return value
            except Exception:
                pass

        return ""

    @staticmethod
    def _parse_requirement_list(raw: str) -> List[str]:
        try:
            return _parse_env_list(raw)
        except Exception:
            logger.warning("Failed to parse requirement list: %s", raw)
            return []

    @classmethod
    def get_install_spec(cls) -> Optional[str]:
        value = cls._get_env_or_airflow_value("DLTAF_INSTALL_SPEC").strip()
        return value or None

    @classmethod
    def get_force_virtualenv(cls) -> Optional[bool]:
        value = cls._get_env_or_airflow_value("DLTAF_FORCE_VIRTUALENV").strip().lower()
        if not value:
            return None
        if value in {"1", "true", "yes", "y", "on"}:
            return True
        if value in {"0", "false", "no", "n", "off"}:
            return False
        logger.warning("Ignoring invalid DLTAF_FORCE_VIRTUALENV value: %s", value)
        return None

    @classmethod
    def get_plugin_requirements(cls) -> List[str]:
        raw = cls._get_env_or_airflow_value("DLTAF_PLUGIN_REQUIREMENTS")
        return cls._parse_requirement_list(raw)

    @classmethod
    def get_pip_install_options(cls) -> List[str]:
        raw = cls._get_env_or_airflow_value("DLTAF_PIP_INSTALL_OPTIONS")
        return cls._parse_requirement_list(raw)

    @classmethod
    def get_venv_cache_path(cls) -> Optional[str]:
        value = cls._get_env_or_airflow_value("DLTAF_VENV_CACHE_PATH").strip()
        return cls.normalize_venv_cache_path(value or None)

    @classmethod
    def normalize_venv_cache_path(cls, value: Optional[str]) -> Optional[str]:
        if not value:
            return value

        raw = value.strip()
        if not raw:
            return None

        repo_root = cls._get_env_or_airflow_value("DLTAF_REPO_ROOT").strip()
        if repo_root and raw.startswith(cls._legacy_repo_root):
            suffix = raw.removeprefix(cls._legacy_repo_root).lstrip("/")
            return str(Path(repo_root) / suffix) if suffix else repo_root

        return raw
    
    @classmethod
    def get_all_requirements(cls) -> List[str]:
        """Получить ВСЕ requirements из pyproject.toml.
        
        Single Source of Truth: просто возвращаем все dependencies.
        Нет фильтрации, нет хардкода - максимально просто!
        
        Returns:
            Список всех requirements из pyproject.toml
        """
        deps: List[str] = []

        install_spec = cls.get_install_spec()
        if install_spec:
            deps.append(install_spec)
        else:
            for dependency in cls.load_dependencies().values():
                if dependency not in deps:
                    deps.append(dependency)

        for requirement in cls.get_plugin_requirements():
            if requirement not in deps:
                deps.append(requirement)
        return deps



# ============================================================================
# Strategy Pattern: Разные стратегии создания операторов
# ============================================================================

class BaseOperatorStrategy(ABC):
    """Базовая стратегия для создания Airflow оператора.
    
    Следует Open/Closed Principle: открыт для расширения, закрыт для модификации.
    """
    
    @abstractmethod
    def create_operator(
        self,
        task_id: str,
        manifest_path: Path,
        task_config: Dict[str, Any],
        task_group: Optional[TaskGroup] = None,
    ) -> Union[PythonOperator, PythonVirtualenvOperator]:
        """Создать оператор с заданной конфигурацией.
        
        Args:
            task_id: ID задачи в Airflow
            manifest_path: Путь к манифесту
            task_config: Финальная конфигурация задачи
            task_group: Опциональная TaskGroup
            
        Returns:
            Сконфигурированный Airflow оператор
        """
        pass


class PythonOperatorStrategy(BaseOperatorStrategy):
    """Стратегия для создания обычного PythonOperator.
    
    Используется когда задача выполняется в общем Airflow окружении.
    """
    
    def create_operator(
        self,
        task_id: str,
        manifest_path: Path,
        task_config: Dict[str, Any],
        task_group: Optional[TaskGroup] = None,
    ) -> PythonOperator:
        """Создать PythonOperator."""
        return PythonOperator(
            task_id=task_id,
            python_callable=_python_callable_wrapper,
            op_kwargs={
                "manifest_path": str(manifest_path),
                "configure_logging": task_config.pop("configure_logging", False),
            },
            task_group=task_group,
            **task_config,
        )


class VirtualenvOperatorStrategy(BaseOperatorStrategy):
    """Стратегия для создания PythonVirtualenvOperator.
    
    Используется когда нужна изоляция зависимостей (например, для Oracle с sqlalchemy-oracledb).
    Single Responsibility: отвечает только за создание virtualenv оператора.
    
    Dependencies читаются из pyproject.toml (Single Source of Truth) через DependencyResolver.
    """
    
    def create_operator(
        self,
        task_id: str,
        manifest_path: Path,
        task_config: Dict[str, Any],
        task_group: Optional[TaskGroup] = None,
    ) -> PythonVirtualenvOperator:
        """Создать PythonVirtualenvOperator с изолированным окружением.
        
        ВАЖНО: Использует изолированный virtualenv (system_site_packages=False)
        и ставит только manifest-aware runtime profile для конкретной задачи.
        
        Для package-mode это означает slim install spec с нужными extras.
        Для legacy embedded fallback мы точечно докидываем SQLAlchemy 2.0
        только если манифест реально использует SQL-based runtime.
        """
        # Извлекаем специфичные для virtualenv параметры
        requirements = task_config.pop("requirements", None)
        system_site_packages = task_config.pop("system_site_packages", False)
        pip_install_options = task_config.pop("pip_install_options", None)
        venv_cache_path = task_config.pop("venv_cache_path", None)
        
        # Если requirements не указаны явно, определяем автоматически
        if requirements is None:
            requirements = self._get_requirements_for_manifest(manifest_path)

        if pip_install_options is None:
            pip_install_options = DependencyResolver.get_pip_install_options()

        if venv_cache_path is None:
            venv_cache_path = DependencyResolver.get_venv_cache_path()
        else:
            venv_cache_path = DependencyResolver.normalize_venv_cache_path(venv_cache_path)

        vault_env_vars = {
            **_vault_env_bridge(),
            "DLTAF_PLUGIN_PATHS": "{{ var.value.get('DLTAF_PLUGIN_PATHS', '') }}",
            "DLTAF_PLUGIN_MODULES": "{{ var.value.get('DLTAF_PLUGIN_MODULES', '') }}",
            "DLTAF_EXTRA_SYS_PATHS": "{{ var.value.get('DLTAF_EXTRA_SYS_PATHS', '') }}",
            "DLTAF_REPO_ROOT": "{{ var.value.get('DLTAF_REPO_ROOT', '') }}",
            "DLT_RUNNER_PLUGINS": "{{ var.value.get('DLT_RUNNER_PLUGINS', '') }}",
            "DLT_HOOK_PLUGINS": "{{ var.value.get('DLT_HOOK_PLUGINS', '') }}",
            "DLT_INFRA_CHECK_PLUGINS": "{{ var.value.get('DLT_INFRA_CHECK_PLUGINS', '') }}",
        }
        airflow_var_bridge_env = _build_airflow_var_bridge_env(manifest_path)
        
        custom_env = task_config.pop("env", None) or {}
        final_runtime_env = {**vault_env_vars, **airflow_var_bridge_env, **custom_env}
        
        logger.info(
            f"Creating isolated virtualenv with manifest-aware dependency profile "
            f"({len(requirements)} total requirements)"
        )
        
        return PythonVirtualenvOperator(
            task_id=task_id,
            python_callable=_virtualenv_callable,
            requirements=requirements,
            pip_install_options=pip_install_options or None,
            venv_cache_path=venv_cache_path,
            system_site_packages=system_site_packages,  # False для изоляции!
            op_args=[
                str(manifest_path),  # manifest_path_arg
                task_config.pop("configure_logging", False),
                final_runtime_env,
            ],
            task_group=task_group,
            **task_config,
        )
    
    def _get_requirements_for_manifest(self, manifest_path: Path) -> List[str]:
        """Автоматически определить requirements для конкретного manifest runtime.
        
        Package-mode:
        - берем глобальный `DLTAF_INSTALL_SPEC`
        - расширяем его по manifest contract до минимального набора extras
        - добавляем explicit private plugin requirements
        
        Legacy embedded mode:
        - читаем локальные зависимости из `pyproject.toml`
        - добавляем SQLAlchemy 2.0 только для SQL-based манифестов
        
        Args:
            manifest_path: Путь к манифесту
            
        Returns:
            Список requirements для конкретной задачи
        """
        try:
            manifest = load_manifest_mapping(manifest_path)
            install_spec = DependencyResolver.get_install_spec()
            plugin_requirements = DependencyResolver.get_plugin_requirements()

            if install_spec:
                expanded_install_spec = expand_dltaf_requirement(install_spec, manifest)
                requirements: List[str] = [expanded_install_spec]
                for requirement in plugin_requirements:
                    if requirement not in requirements:
                        requirements.append(requirement)

                logger.info(
                    "Resolved package-mode install profile for %s: %s",
                    manifest_path.name,
                    expanded_install_spec,
                )
                return requirements

            requirements = DependencyResolver.get_all_requirements()
            if (
                manifest_needs_sqlalchemy_upgrade(manifest)
                and not any("sqlalchemy" in requirement.lower() for requirement in requirements)
            ):
                requirements.append("sqlalchemy>=2.0.25")

            logger.info(
                f"Resolved {len(requirements)} embedded requirements for virtualenv"
            )
            
            return requirements
            
        except Exception as e:
            logger.error(f"Failed to load requirements from pyproject.toml: {e}")
            # Fallback - возвращаем пустой список, используется fallback в DependencyResolver
            return DependencyResolver.get_all_requirements()


# ============================================================================
# TaskFactory: главная фабрика (Facade pattern)
# ============================================================================

class TaskFactory:
    """Фабрика для создания Airflow задач из манифестов"""
    
    def __init__(
        self,
        default_task_config: Optional[Dict[str, Any]] = None,
        use_virtualenv: bool = False,
    ):
        """Инициализирует фабрику задач.
        
        Args:
            default_task_config: Конфигурация по умолчанию для всех задач
            use_virtualenv: Использовать ли PythonVirtualenvOperator по умолчанию
        """
        self.default_task_config = default_task_config or {
            "retries": 2,
            "retry_delay": timedelta(minutes=5),
            "execution_timeout": timedelta(hours=2),
        }
        self.use_virtualenv = use_virtualenv
        
        # Strategy pattern: выбираем стратегию при создании
        self.strategy: BaseOperatorStrategy = (
            VirtualenvOperatorStrategy() if use_virtualenv else PythonOperatorStrategy()
        )
    
    def create_task(
        self,
        pipeline_name: str,
        manifest_path: Path,
        manifest: Dict[str, Any],
        task_group: Optional[TaskGroup] = None,
        force_virtualenv: Optional[bool] = None,
    ) -> Union[PythonOperator, PythonVirtualenvOperator]:
        """Создает Airflow оператор для запуска пайплайна из манифеста.
        
        Single Responsibility: отвечает только за координацию создания задачи.
        
        Args:
            pipeline_name: Имя пайплайна (используется как task_id)
            manifest_path: Путь к манифесту
            manifest: Загруженный манифест (словарь)
            task_group: Опциональная TaskGroup для группировки задач
            force_virtualenv: Принудительно использовать virtualenv (переопределяет настройки)
        
        Returns:
            Сконфигурированный Airflow оператор
        
        Raises:
            TaskFactoryError: Если не удалось создать задачу или имя невалидно
        """
        try:
            # Валидируем имя пайплайна
            self._validate_pipeline_name(pipeline_name)
            
            # Извлекаем и объединяем конфигурацию
            task_config = self._build_task_config(manifest)
            
            # Создаем task_id
            task_id = self._build_task_id(pipeline_name)
            
            # Определяем стратегию для этой задачи
            strategy = self._select_strategy(manifest, force_virtualenv)
            
            # Создаем оператор через стратегию
            task = strategy.create_operator(
                task_id=task_id,
                manifest_path=manifest_path,
                task_config=task_config,
                task_group=task_group,
            )
            
            logger.debug(
                f"Задача создана: {task_id} "
                f"(strategy={strategy.__class__.__name__})"
            )
            
            return task
            
        except TaskFactoryError:
            raise
        except Exception as e:
            raise TaskFactoryError(
                f"Ошибка создания задачи для {pipeline_name}: {e}"
            ) from e
    
    def _validate_pipeline_name(self, pipeline_name: str) -> None:
        """Валидирует имя пайплайна."""
        try:
            validate_pipeline_name(pipeline_name)
        except ValueError as e:
            raise TaskFactoryError(
                f"Имя пайплайна '{pipeline_name}' не соответствует naming convention: {e}"
            ) from e
    
    def _build_task_config(self, manifest: Dict[str, Any]) -> Dict[str, Any]:
        """Строит финальную конфигурацию задачи из манифеста и defaults.
        
        DRY: Централизованная логика обработки конфигурации.
        """
        airflow_cfg = manifest.get("airflow", {})
        task_cfg = airflow_cfg.get("task", {})
        
        # Начинаем с default конфигурации
        final_config = {**self.default_task_config}
        
        # Обрабатываем retry_delay_minutes -> retry_delay (timedelta)
        if "retry_delay_minutes" in task_cfg:
            minutes = int(task_cfg["retry_delay_minutes"])
            final_config["retry_delay"] = timedelta(minutes=minutes)
        
        # Обрабатываем retries
        if "retries" in task_cfg:
            final_config["retries"] = int(task_cfg["retries"])
        
        # Обрабатываем execution_timeout_hours
        if "execution_timeout_hours" in task_cfg:
            hours = float(task_cfg["execution_timeout_hours"])
            final_config["execution_timeout"] = timedelta(hours=hours)
        
        # Специфичные параметры
        final_config["configure_logging"] = task_cfg.get("configure_logging", False)
        
        # Для virtualenv: requirements и system_site_packages
        if "requirements" in task_cfg:
            final_config["requirements"] = task_cfg["requirements"]

        if "pip_install_options" in task_cfg:
            final_config["pip_install_options"] = task_cfg["pip_install_options"]

        if "venv_cache_path" in task_cfg:
            final_config["venv_cache_path"] = task_cfg["venv_cache_path"]
        
        if "system_site_packages" in task_cfg:
            final_config["system_site_packages"] = task_cfg["system_site_packages"]
        
        return final_config
    
    def _build_task_id(self, pipeline_name: str) -> str:
        """Создает task_id из имени пайплайна.
        
        DRY: Единое место для формирования task_id.
        """
        return f"run__{normalize_token(pipeline_name)}"
    
    def _select_strategy(
        self,
        manifest: Dict[str, Any],
        force_virtualenv: Optional[bool] = None,
    ) -> BaseOperatorStrategy:
        """Выбирает стратегию создания оператора на основе конфигурации.
        
        Strategy Selection: выбор стратегии на основе контекста.
        
        Args:
            manifest: Манифест пайплайна
            force_virtualenv: Принудительно использовать virtualenv
            
        Returns:
            Выбранная стратегия
        """
        # Если force_virtualenv задан, используем его
        if force_virtualenv is not None:
            return VirtualenvOperatorStrategy() if force_virtualenv else PythonOperatorStrategy()

        forced_runtime_choice = DependencyResolver.get_force_virtualenv()
        if forced_runtime_choice is not None:
            return (
                VirtualenvOperatorStrategy()
                if forced_runtime_choice
                else PythonOperatorStrategy()
            )

        # Проверяем настройки в манифесте
        airflow_cfg = manifest.get("airflow", {})
        task_cfg = airflow_cfg.get("task", {})
        use_virtualenv_in_manifest = task_cfg.get("use_virtualenv")
        
        if use_virtualenv_in_manifest is not None:
            return VirtualenvOperatorStrategy() if use_virtualenv_in_manifest else PythonOperatorStrategy()
        
        return self.strategy
    
    def create_task_group(
        self,
        group_id: str,
        tooltip: Optional[str] = None,
    ) -> TaskGroup:
        """Создает TaskGroup для группировки связанных задач.
        
        Args:
            group_id: Идентификатор группы (должен быть валидным токеном)
            tooltip: Подсказка для UI
        
        Returns:
            TaskGroup
        
        Raises:
            TaskFactoryError: Если group_id невалиден
        """
        try:
            # Нормализуем group_id
            normalized_id = normalize_token(group_id)
            
            return TaskGroup(
                group_id=normalized_id,
                tooltip=tooltip or f"Task group: {group_id}",
            )
        except ValueError as e:
            raise TaskFactoryError(f"Невалидный group_id '{group_id}': {e}") from e
