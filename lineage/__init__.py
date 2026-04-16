"""Модуль lineage для управления зависимостями между dlt пайплайнами.

Этот пакет предоставляет полнофункциональную систему управления зависимостями
между YAML манифестами с проверкой циклических зависимостей.

"""

from lineage.dependency_graph import (
    CyclicDependencyError,
    DependencyGraph,
    DependencyNode,
)

from lineage.manifest_dependency_resolver import (
    InvalidDependencyError,
    ManifestDependencyError,
    ManifestDependencyResolver,
    ManifestNotFoundError,
    _extract_task_group,
    _extract_depends_on,
    _extract_depends_on_simple,
)

__all__ = [
    # Граф зависимостей
    "DependencyGraph",
    "DependencyNode",
    "CyclicDependencyError",
    
    # Резолвер манифестов
    "ManifestDependencyResolver",
    "ManifestDependencyError",
    "ManifestNotFoundError",
    "InvalidDependencyError",
    
    # Функции извлечения из манифестов
    "_extract_task_group",
    "_extract_depends_on",
    "_extract_depends_on_simple",
]