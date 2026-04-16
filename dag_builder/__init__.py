"""Модуль для динамического построения Airflow DAG из YAML манифестов.

Этот модуль предоставляет функцию build_dag_from_yaml() для создания DAG'ов
с автоматическим управлением зависимостями между пайплайнами.

Основные компоненты:
  - build_dag_from_yaml: Главная функция для построения DAG из манифестов
  - ManifestLoader: Загрузчик и валидатор YAML манифестов
  - DependencyResolver: Резолвер зависимостей между манифестами
  - TaskFactory: Фабрика для создания Airflow задач
  - DAGBuilder: Построитель графа задач в DAG

"""

from dag_builder.builder import build_dag_from_yaml
from dag_builder.manifest_loader import ManifestLoader
from dag_builder.dependency_resolver import DependencyResolver
from dag_builder.task_factory import TaskFactory
from dag_builder.dag_builder_core import DAGBuilder

__all__ = [
    # Главная функция
    "build_dag_from_yaml",
    
    # Компоненты
    "ManifestLoader",
    "DependencyResolver", 
    "TaskFactory",
    "DAGBuilder",
]