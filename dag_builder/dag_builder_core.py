"""Ядро построителя DAG - собирает все компоненты воедино.

Этот модуль предоставляет класс DAGBuilder, который координирует работу
всех компонентов для построения графа задач в Airflow DAG.
"""

import logging
from pathlib import Path
from typing import Any, Dict, List, Optional

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup

from dag_builder.manifest_loader import ManifestLoader
from dag_builder.dependency_resolver import DependencyResolver
from dag_builder.task_factory import TaskFactory

logger = logging.getLogger(__name__)


class DAGBuilderError(Exception):
    """Ошибка при построении DAG."""
    pass


class DAGBuilder:
    """Построитель Airflow DAG из YAML манифестов.
    
    Координирует работу всех компонентов:
    - ManifestLoader: загрузка манифестов
    - DependencyResolver: построение графа зависимостей
    - TaskFactory: создание Airflow задач
    
    Attributes:
        dag: Airflow DAG, в который добавляются задачи
        loader: Загрузчик манифестов
        resolver: Резолвер зависимостей
        factory: Фабрика задач
        _tasks: Словарь созданных задач {pipeline_name: PythonOperator}
    """
    
    def __init__(
        self,
        dag: DAG,
        manifests_dir: Optional[Path] = None,
        default_task_config: Optional[Dict[str, Any]] = None,
        use_virtualenv: bool = False,
    ):
        """Инициализирует построитель DAG.
        
        Args:
            dag: Airflow DAG, в который будут добавлены задачи
            manifests_dir: Директория с манифестами
            default_task_config: Конфигурация по умолчанию для задач
            use_virtualenv: Использовать PythonVirtualenvOperator вместо PythonOperator
        """
        self.dag = dag
        self.loader = ManifestLoader(manifests_dir)
        self.resolver = DependencyResolver(self.loader)
        self.factory = TaskFactory(default_task_config, use_virtualenv=use_virtualenv)
        
        self._tasks: Dict[str, PythonOperator] = {}
        self._task_groups: Dict[str, TaskGroup] = {}
    
    def add_manifest(self, manifest_path: Path | str) -> None:
        """Добавляет манифест для обработки.
        
        Args:
            manifest_path: Путь к манифесту (абсолютный или относительный)
        """
        self.resolver.add_manifest(manifest_path)
    
    def add_manifests(self, manifest_paths: List[Path | str]) -> None:
        """Добавляет несколько манифестов для обработки.
        
        Args:
            manifest_paths: Список путей к манифестам
        """
        for path in manifest_paths:
            self.add_manifest(path)
    
    def build(
        self,
        use_task_groups: bool = True,
        task_group_prefix: Optional[str] = None,
    ) -> None:
        """Строит граф задач в DAG.
        
        Основной метод, который:
        1. Строит граф зависимостей
        2. Получает порядок выполнения (топологическая сортировка)
        3. Создает TaskGroups (если указаны в манифестах)
        4. Создает задачи для каждого пайплайна
        5. Выстраивает зависимости между задачами
        
        Args:
            use_task_groups: Использовать ли TaskGroup из манифестов (task_group)
            task_group_prefix: Префикс для имен TaskGroup'ов (DEPRECATED)
        
        Raises:
            DAGBuilderError: Если не удалось построить DAG
        """
        try:
            # 1. Строим граф зависимостей
            logger.info("Построение графа зависимостей...")
            self.resolver.build_graph()

            # 2. Получаем порядок выполнения
            execution_order = self.resolver.get_execution_order()
            logger.info(f"Порядок выполнения: {execution_order}")
            
            # 3. Создаем TaskGroups из манифестов
            with self.dag:
                if use_task_groups:
                    self._create_task_groups_from_manifests()
                
                # 4. Создаем задачи для каждого пайплайна
                for pipeline_name in execution_order:
                    self._create_task_for_pipeline(
                        pipeline_name,
                        use_task_groups=use_task_groups,
                    )
                
                # 5. Выстраиваем зависимости
                self._set_task_dependencies()
            
            logger.info(
                f"DAG построен успешно: {len(self._tasks)} задач, "
                f"{len(self._task_groups)} групп"
            )
            
        except Exception as e:
            raise DAGBuilderError(f"Ошибка построения DAG: {e}") from e
    
    def _create_task_groups_from_manifests(self) -> None:
        """Создает TaskGroups на основе поля task_group в манифестах."""
        # Собираем все task_groups из манифестов
        task_groups_set = set()
        
        for pipeline_name in self.resolver.get_all_pipelines():
            task_group_name = self.resolver.get_task_group(pipeline_name)
            if task_group_name:
                task_groups_set.add(task_group_name)
        
        # Создаем TaskGroup для каждой уникальной группы
        for group_name in task_groups_set:
            if group_name not in self._task_groups:
                pipelines_in_group = self.resolver.get_pipelines_in_group(group_name)
                task_group = self.factory.create_task_group(
                    group_id=group_name,
                    tooltip=f"Task group: {group_name} ({len(pipelines_in_group)} pipelines)",
                )
                self._task_groups[group_name] = task_group
                logger.info(f"Создана группа: {group_name}")

    
    def _create_task_for_pipeline(
        self,
        pipeline_name: str,
        use_task_groups: bool = True,
    ) -> PythonOperator:
        """Создает задачу для конкретного пайплайна.
        
        Args:
            pipeline_name: Имя пайплайна
            use_task_groups: Использовать ли TaskGroup из манифеста (task_group)
        
        Returns:
            Созданный PythonOperator
        """
        # Получаем путь к манифесту
        manifest_path = self.resolver.get_manifest_path(pipeline_name)
        if manifest_path is None:
            raise DAGBuilderError(f"Манифест не найден для пайплайна: {pipeline_name}")
        
        # Загружаем манифест
        manifest = self.loader.load(manifest_path)
        
        # Определяем TaskGroup
        task_group = None
        if use_task_groups:
            # Проверяем, есть ли task_group в манифесте
            task_group_name = self.resolver.get_task_group(pipeline_name)
            if task_group_name:
                # Используем существующую TaskGroup
                task_group = self._task_groups.get(task_group_name)
                if task_group is None:
                    logger.warning(
                        f"TaskGroup '{task_group_name}' не найдена для {pipeline_name}"
                    )
        
        # Создаем задачу
        task = self.factory.create_task(
            pipeline_name=pipeline_name,
            manifest_path=manifest_path,
            manifest=manifest,
            task_group=task_group,
        )
        
        # Сохраняем в словарь
        self._tasks[pipeline_name] = task
        
        return task
    
    def _set_task_dependencies(self) -> None:
        """Выстраивает зависимости между задачами на основе графа."""
        for pipeline_name, task in self._tasks.items():
            # Получаем зависимости из графа
            dependencies = self.resolver.get_dependencies(pipeline_name)
            
            if not dependencies:
                continue
            
            # Создаем upstream задачи
            upstream_tasks = []
            for dep_name in dependencies:
                dep_task = self._tasks.get(dep_name)
                if dep_task is None:
                    logger.warning(f"Зависимость {dep_name} не найдена для {pipeline_name}")
                    continue
                upstream_tasks.append(dep_task)
            
            # Устанавливаем зависимости через native Airflow >>
            if upstream_tasks:
                if len(upstream_tasks) == 1:
                    upstream_tasks[0] >> task
                else:
                    # Несколько зависимостей: [dep1, dep2] >> task
                    upstream_tasks >> task
                
                logger.debug(
                    f"Зависимости установлены: {[t.task_id for t in upstream_tasks]} >> {task.task_id}"
                )
    
    def get_task(self, pipeline_name: str) -> Optional[PythonOperator]:
        """Возвращает задачу по имени пайплайна.
        
        Args:
            pipeline_name: Имя пайплайна
        
        Returns:
            PythonOperator или None
        """
        return self._tasks.get(pipeline_name)
    
    def get_all_tasks(self) -> Dict[str, PythonOperator]:
        """Возвращает все созданные задачи.
        
        Returns:
            Словарь {pipeline_name: PythonOperator}
        """
        return self._tasks.copy()