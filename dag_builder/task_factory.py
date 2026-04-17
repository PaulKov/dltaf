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

import logging
from abc import ABC, abstractmethod
from datetime import timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional, Union

from airflow.operators.python import PythonOperator, PythonVirtualenvOperator
from airflow.utils.task_group import TaskGroup

from dlt_utils.naming import normalize_token, validate_pipeline_name

logger = logging.getLogger(__name__)


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


def _virtualenv_callable(manifest_path_arg, configure_logging_arg, vault_env_dict):
    """Wrapper для PythonVirtualenvOperator.
    
    КРИТИЧНО: Эта функция ДОЛЖНА быть top-level (module-level),
    иначе pickle не сможет её сериализовать.
    
    Функция НЕ принимает **context, потому что это может вызвать проблемы с pickle.
    Airflow передаст context автоматически, но мы его не используем.
    
    Args:
        manifest_path_arg: Путь к манифесту
        configure_logging_arg: Настраивать ли логирование
        vault_env_dict: Словарь с Vault credentials для установки в ENV
    """
    import logging
    import os
    import sys
    from pathlib import Path
    
    logger = logging.getLogger(__name__)
    
    if vault_env_dict:
        for key, value in vault_env_dict.items():
            if value:  # Устанавливаем только непустые значения
                os.environ[key] = str(value)
                logger.info(f"Set ENV variable: {key}")
    
    # Определяем путь к корню проекта через манифест
    manifest_abs = Path(manifest_path_arg).resolve()
    
    # Манифесты обычно в dags/manifests, проект на 2 уровня выше
    project_root = manifest_abs.parent.parent.parent
    
    if str(project_root) not in sys.path:
        sys.path.insert(0, str(project_root))
        logger.info(f"Added project root to sys.path: {project_root}")
    
    # Динамический импорт внутри функции - критично для virtualenv!
    try:
        from dlt_utils.manifest_runner import run_manifest
    except ImportError as e:
        logger.error(f"Failed to import dlt_utils: {e}")
        logger.error(f"sys.path: {sys.path}")
        logger.error(f"project_root: {project_root}")
        raise
    
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
            "hvac": "hvac>=2.1.0",
            "oracledb": "oracledb>=2.0.0",
            "sqlalchemy": "sqlalchemy>=2.0.25",
            "psycopg2-binary": "psycopg2-binary>=2.9.9",
            "pymongo": "pymongo>=4.6.0",
            "requests": "requests>=2.31.0",
            "kafka-python": "kafka-python>=2.0.2",
        }
    
    @classmethod
    def get_all_requirements(cls) -> List[str]:
        """Получить ВСЕ requirements из pyproject.toml.
        
        Single Source of Truth: просто возвращаем все dependencies.
        Нет фильтрации, нет хардкода - максимально просто!
        
        Returns:
            Список всех requirements из pyproject.toml
        """
        deps = cls.load_dependencies()
        return list(deps.values())



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
        с SQLAlchemy 2.0 для поддержки oracle+oracledb:// диалекта.
        
        SQLAlchemy 1.4 (в Airflow 2.11.0) не поддерживает oracle+oracledb://,
        поэтому устанавливаем SQLAlchemy 2.0 в изолированный virtualenv.
        """
        # Извлекаем специфичные для virtualenv параметры
        requirements = task_config.pop("requirements", None)
        system_site_packages = task_config.pop("system_site_packages", False)
        
        # Если requirements не указаны явно, определяем автоматически
        if requirements is None:
            requirements = self._get_requirements_for_manifest(manifest_path)
        
        requirements_with_sqlalchemy = requirements + ["sqlalchemy>=2.0.25"]
        
        vault_env_vars = {
            "VAULT_ADDRESS": "{{ var.value.get('VAULT_ADDRESS', '') }}",
            "VAULT_TOKEN": "{{ var.value.get('VAULT_TOKEN', '') }}",
            "VAULT_ROLE_ID": "{{ var.value.get('VAULT_ROLE_ID', '') }}",
            "VAULT_SECRET_ID": "{{ var.value.get('VAULT_SECRET_ID', '') }}",
        }
        
        custom_env = task_config.pop("env", None) or {}
        final_vault_env = {**vault_env_vars, **custom_env}
        
        logger.info(
            f"Creating isolated virtualenv with SQLAlchemy 2.0 "
            f"({len(requirements_with_sqlalchemy)} total requirements)"
        )
        
        return PythonVirtualenvOperator(
            task_id=task_id,
            python_callable=_virtualenv_callable,
            requirements=requirements_with_sqlalchemy,
            system_site_packages=system_site_packages,  # False для изоляции!
            op_args=[
                str(manifest_path),  # manifest_path_arg
                task_config.pop("configure_logging", False),
                final_vault_env,
            ],
            task_group=task_group,
            **task_config,
        )
    
    def _get_requirements_for_manifest(self, manifest_path: Path) -> List[str]:
        """Автоматически определить requirements из pyproject.toml.
        
        DRY: Использует DependencyResolver для чтения из pyproject.toml.
        Single Source of Truth: ВСЕ зависимости определены только в pyproject.toml.
        
        
        Args:
            manifest_path: Путь к манифесту
            
        Returns:
            Список всех requirements из pyproject.toml
        """
        try:
            # Получаем ВСЕ requirements из pyproject.toml
            requirements = DependencyResolver.get_all_requirements()
            
            logger.info(
                f"Loaded {len(requirements)} requirements from pyproject.toml for virtualenv"
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