"""Резолвер зависимостей между YAML манифестами.

Этот модуль предоставляет класс DependencyResolver для построения
графа зависимостей между пайплайнами на основе поля depends_on в манифестах.
"""

import logging
from pathlib import Path
from typing import List, Optional, Set

from lineage import (
    DependencyGraph,
    CyclicDependencyError,
)
from dag_builder.manifest_loader import ManifestLoader

logger = logging.getLogger(__name__)


class DependencyResolutionError(Exception):
    """Ошибка при разрешении зависимостей."""
    pass


class DependencyResolver:
    """Резолвер зависимостей между манифестами.
    
    Строит граф зависимостей ТОЛЬКО для явно добавленных манифестов
    через add_manifest() и их рекурсивных зависимостей.
    
    Манифесты, которые просто находятся в директории, но не добавлены
    и не являются зависимостями, НЕ попадают в граф.
    
    Attributes:
        loader: Загрузчик манифестов
        manifests_dir: Директория с манифестами
        _graph: Граф зависимостей
        _loaded_manifests: Множество путей к загруженным манифестам
    """
    
    def __init__(self, loader: ManifestLoader):
        """Инициализирует резолвер зависимостей.
        
        Args:
            loader: Экземпляр ManifestLoader для загрузки манифестов
        """
        self.loader = loader
        self.manifests_dir = loader.base_dir
        self._graph: Optional[DependencyGraph] = None
        self._loaded_manifests: Set[Path] = set()
    
    def add_manifest(self, manifest_path: Path | str) -> None:
        """Добавляет манифест в граф зависимостей.
        
        Рекурсивно загружает все зависимости этого манифеста.
        
        Args:
            manifest_path: Путь к манифесту (абсолютный или относительный, с .yaml или без)
        
        Raises:
            DependencyResolutionError: Если манифест не найден или есть ошибки
        """
        # Преобразуем в Path
        path = Path(manifest_path)
        
        # Если путь относительный, разрешаем относительно base_dir
        if not path.is_absolute():
            path = (self.manifests_dir / path).resolve()
        
        if not path.suffix:
            path = path.with_suffix('.yaml')
        
        # Проверяем, не загружен ли уже
        if path in self._loaded_manifests:
            logger.debug(f"Манифест уже загружен: {path.name}")
            return
        
        # Проверяем существование
        if not path.exists():
            raise DependencyResolutionError(f"Манифест не найден: {path}")
        
        # Загружаем манифест
        manifest = self.loader.load(path)
        pipeline_name = manifest.get("pipeline", {}).get("name")
        
        if not pipeline_name:
            raise DependencyResolutionError(f"pipeline.name не найден в {path}")
        
        self._loaded_manifests.add(path)
        logger.debug(f"Добавлен манифест: {pipeline_name}")
        
        # Загружаем зависимости
        depends_on = manifest.get("depends_on", [])
        if depends_on:
            logger.debug(f"Манифест {pipeline_name} имеет зависимости: {depends_on}")
            for dep_name in depends_on:
                dep_path = self.loader.find_manifest_by_name(dep_name)
                if dep_path is None:
                    raise DependencyResolutionError(
                        f"Зависимость '{dep_name}' для '{pipeline_name}' не найдена в {self.manifests_dir}"
                    )
                # Рекурсивно добавляем зависимость
                self.add_manifest(dep_path)
    
    def build_graph(self) -> DependencyGraph:
        """Строит граф зависимостей для загруженных манифестов.
        
        ВАЖНО: Строит граф ТОЛЬКО для манифестов, добавленных через add_manifest()
        и их зависимостей. Манифесты, которые просто лежат в директории, но не были
        явно добавлены или не являются зависимостями, НЕ попадут в граф.
        
        Returns:
            Граф зависимостей
        
        Raises:
            DependencyResolutionError: Если есть циклические зависимости
        """
        if self._graph is not None:
            return self._graph
        
        if not self._loaded_manifests:
            raise DependencyResolutionError("Нет загруженных манифестов. Вызовите add_manifest() сначала.")
        
        try:
            # Строим граф вручную, используя ТОЛЬКО загруженные манифесты
            graph = DependencyGraph()
            
            # Для каждого загруженного манифеста
            for manifest_path in self._loaded_manifests:
                manifest = self.loader.load(manifest_path)
                pipeline_name = manifest.get("pipeline", {}).get("name")
                
                if not pipeline_name:
                    raise DependencyResolutionError(f"pipeline.name не найден в {manifest_path}")
                
                # Извлекаем зависимости
                depends_on = manifest.get("depends_on", [])
                
                # Преобразуем в список имен (простой формат)
                deps_list = []
                if isinstance(depends_on, list):
                    for dep in depends_on:
                        if isinstance(dep, str):
                            deps_list.append(dep)
                        elif isinstance(dep, dict):
                            # Расширенный формат - пропускаем group зависимости
                            # (они уже разрешены в add_manifest)
                            if "path" in dep:
                                # Найти имя пайплайна по пути
                                dep_path = self.loader.find_manifest_by_name(dep["path"])
                                if dep_path:
                                    dep_manifest = self.loader.load(dep_path)
                                    dep_name = dep_manifest.get("pipeline", {}).get("name")
                                    if dep_name:
                                        deps_list.append(dep_name)
                
                # Добавляем узел в граф
                graph.add_node(
                    name=pipeline_name,
                    depends_on=deps_list,
                    metadata={
                        "manifest_path": str(manifest_path),
                        "manifest_name": manifest_path.name,
                    },
                )
            
            # Валидация на циклы
            graph.validate()
            
            self._graph = graph
            
            logger.info(f"Граф зависимостей построен: {len(self._graph.get_all_nodes())} узлов")
            
            return self._graph
            
        except CyclicDependencyError as e:
            raise DependencyResolutionError(f"Циклическая зависимость: {e}") from e
        except Exception as e:
            raise DependencyResolutionError(f"Ошибка построения графа: {e}") from e
    
    def get_execution_order(self) -> List[str]:
        """Возвращает порядок выполнения пайплайнов (топологическая сортировка).
        
        Returns:
            Список имен пайплайнов в порядке выполнения
        
        Raises:
            DependencyResolutionError: Если граф не построен
        """
        if self._graph is None:
            raise DependencyResolutionError("Граф не построен. Вызовите build_graph() сначала.")
        
        try:
            return self._graph.topological_sort()
        except Exception as e:
            raise DependencyResolutionError(f"Ошибка топологической сортировки: {e}") from e
    
    def get_dependencies(self, pipeline_name: str) -> List[str]:
        """Возвращает прямые зависимости пайплайна.
        
        Args:
            pipeline_name: Имя пайплайна
        
        Returns:
            Список имен пайплайнов, от которых зависит данный пайплайн
        """
        if self._graph is None:
            raise DependencyResolutionError("Граф не построен. Вызовите build_graph() сначала.")
        
        node = self._graph.get_node(pipeline_name)
        if node is None:
            return []
        
        return node.dependencies
    
    def get_dependents(self, pipeline_name: str) -> List[str]:
        """Возвращает зависимые пайплайны (reverse dependencies).
        
        Args:
            pipeline_name: Имя пайплайна
        
        Returns:
            Список имен пайплайнов, которые зависят от данного пайплайна
        """
        if self._resolver is None:
            raise DependencyResolutionError("Резолвер не инициализирован. Вызовите build_graph() сначала.")
        
        try:
            return self._resolver.get_dependents(pipeline_name)
        except Exception:
            return []
    
    def get_manifest_path(self, pipeline_name: str) -> Optional[Path]:
        """Возвращает путь к манифесту по имени пайплайна.
        
        Args:
            pipeline_name: Имя пайплайна
        
        Returns:
            Путь к манифесту или None
        """
        return self.loader.find_manifest_by_name(pipeline_name)
    
    def get_task_group(self, pipeline_name: str) -> Optional[str]:
        """Возвращает имя task_group из манифеста пайплайна.
        
        Args:
            pipeline_name: Имя пайплайна
        
        Returns:
            Имя task_group или None
        """
        for manifest_path in self._loaded_manifests:
            manifest = self.loader.load(manifest_path)
            if manifest.get("pipeline", {}).get("name") == pipeline_name:
                return manifest.get("task_group")
        return None
    
    def get_pipelines_in_group(self, task_group: str) -> List[str]:
        """Возвращает список пайплайнов в указанной группе.
        
        Args:
            task_group: Имя группы
        
        Returns:
            Список имен пайплайнов
        """
        pipelines = []
        for manifest_path in self._loaded_manifests:
            manifest = self.loader.load(manifest_path)
            if manifest.get("task_group") == task_group:
                pipeline_name = manifest.get("pipeline", {}).get("name")
                if pipeline_name:
                    pipelines.append(pipeline_name)
        return pipelines
    
    def get_all_pipelines(self) -> List[str]:
        """Возвращает список всех пайплайнов.
        
        Returns:
            Список имен пайплайнов
        """
        if self._graph is None:
            raise DependencyResolutionError("Граф не построен. Вызовите build_graph() сначала.")
        
        return [node.name for node in self._graph.get_all_nodes()]