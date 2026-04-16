"""Резолвер зависимостей манифестов для dlt пайплайнов.

Этот модуль разрешает зависимости между YAML манифестами и строит
граф зависимостей для генерации Airflow DAG.

Основные обязанности:
  - Парсинг поля 'depends_on' из манифестов
  - Построение DependencyGraph из директории манифестов
  - Валидация межманифестных зависимостей
  - Предоставление информации о lineage для оркестрации

Принципы проектирования:
  - Dependency Inversion: зависит от абстрактного DependencyGraph
  - Open/Closed: расширяем для новых форматов манифестов
  - Interface Segregation: сфокусирован только на резолвинге манифестов
"""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Dict, List, Mapping, Optional

import yaml

from lineage.dependency_graph import CyclicDependencyError, DependencyGraph

logger = logging.getLogger(__name__)


class ManifestDependencyError(Exception):
    """Базовое исключение для ошибок зависимостей манифестов."""


class ManifestNotFoundError(ManifestDependencyError):
    """Исключение при отсутствии манифеста, на который ссылается зависимость."""


class InvalidDependencyError(ManifestDependencyError):
    """Исключение при некорректном объявлении зависимости."""


def _load_manifest_raw(path: Path) -> Dict:
    """Загрузить манифест без полного резолвинга (облегчённый вариант)."""
    if not path.exists():
        raise FileNotFoundError(f"Манифест не найден: {path}")
    
    data = yaml.safe_load(path.read_text(encoding="utf-8"))
    if not isinstance(data, dict):
        raise ValueError(f"Манифест должен быть YAML mapping: {path}")
    
    return data


def _extract_pipeline_name(manifest: Mapping, manifest_path: Path) -> str:
    """Извлечь имя пайплайна из манифеста."""
    pipeline = manifest.get("pipeline") or {}
    name = str(pipeline.get("name") or "").strip()
    
    if not name:
        raise ValueError(f"pipeline.name обязателен в {manifest_path}")
    
    return name


def _extract_task_group(manifest: Mapping) -> Optional[str]:
    """Извлечь task_group из манифеста.
    
    Args:
        manifest: Словарь манифеста
        
    Returns:
        Имя task_group или None если не указано
    """
    task_group = manifest.get("task_group")
    
    if task_group is None:
        return None
    
    if not isinstance(task_group, str):
        raise InvalidDependencyError(
            f"'task_group' должен быть строкой, получен: {type(task_group)}"
        )
    
    return task_group.strip()


def _extract_depends_on(manifest: Mapping, manifest_path: Path) -> List[Dict[str, str]]:
    """Извлечь список depends_on из манифеста.
    
    Поддерживаемые форматы:
      # Простой формат (обратная совместимость)
      depends_on:
        - pipeline_a
        - pipeline_b
      
      # Расширенный формат (с path и group)
      depends_on:
        - path: pipeline_a.yaml
        - group: preprocessing_group
        - pipeline_c  # Можно миксовать форматы
    """
    depends_on = manifest.get("depends_on")
    
    if depends_on is None:
        return []
    
    if not isinstance(depends_on, list):
        raise InvalidDependencyError(
            f"'depends_on' должен быть списком в {manifest_path}, получен: {type(depends_on)}"
        )
    
    result: List[Dict[str, str]] = []
    
    for idx, dep in enumerate(depends_on):
        # Простой формат (строка) - обратная совместимость
        if isinstance(dep, str):
            dep_name = dep.strip()
            if not dep_name:
                raise InvalidDependencyError(
                    f"'depends_on[{idx}]' не может быть пустым в {manifest_path}"
                )
            
            # Определяем тип по расширению
            if dep_name.endswith(".yaml"):
                # Путь к файлу
                result.append({"type": "path", "value": dep_name})
            else:
                # Имя пайплайна
                result.append({"type": "name", "value": dep_name})
        
        # Расширенный формат (словарь)
        elif isinstance(dep, dict):
            # Проверяем наличие path или group
            if "path" in dep:
                path_value = str(dep["path"]).strip()
                if not path_value:
                    raise InvalidDependencyError(
                        f"'depends_on[{idx}].path' не может быть пустым в {manifest_path}"
                    )
                result.append({"type": "path", "value": path_value})
            
            elif "group" in dep:
                group_value = str(dep["group"]).strip()
                if not group_value:
                    raise InvalidDependencyError(
                        f"'depends_on[{idx}].group' не может быть пустым в {manifest_path}"
                    )
                result.append({"type": "group", "value": group_value})
            
            else:
                raise InvalidDependencyError(
                    f"'depends_on[{idx}]' должен содержать 'path' или 'group' в {manifest_path}"
                )
        
        else:
            raise InvalidDependencyError(
                f"'depends_on[{idx}]' должен быть строкой или словарем в {manifest_path}, "
                f"получен: {type(dep)}"
            )
    
    return result


def _extract_depends_on_simple(manifest: Mapping, manifest_path: Path) -> List[str]:
    """Извлечь список depends_on в упрощенном формате (только имена пайплайнов)
    
    Эта функция для обратной совместимости и используется в местах,
    где нужен простой список имен.
    
    Args:
        manifest: Словарь манифеста
        manifest_path: Путь к манифесту
        
    Returns:
        Список имён пайплайнов
    """
    deps_structured = _extract_depends_on(manifest, manifest_path)
    result = []
    
    for dep in deps_structured:
        if dep["type"] == "name":
            result.append(dep["value"])
        elif dep["type"] == "path":
            # Извлекаем имя из пути
            path_val = dep["value"]
            if path_val.endswith(".yaml"):
                result.append(path_val[:-5])  # Убираем .yaml
            else:
                result.append(path_val)
        # group игнорируем в упрощенном формате
    
    return result


class ManifestDependencyResolver:
    """Резолвер зависимостей между манифестами dlt пайплайнов
    
    Этот класс сканирует директорию манифестов, извлекает информацию о зависимостях
    и строит валидированный граф зависимостей.
    
    """

    def __init__(self, manifests_dir: Path) -> None:
        """Инициализировать резолвер.
        
        Args:
            manifests_dir: Директория с YAML манифестами
        """
        if not manifests_dir.exists():
            raise ValueError(f"Директория манифестов не существует: {manifests_dir}")
        
        if not manifests_dir.is_dir():
            raise ValueError(f"Не является директорией: {manifests_dir}")
        
        self.manifests_dir = manifests_dir.resolve()
        self._manifest_map: Dict[str, Path] = {}
        self._dependency_map: Dict[str, List[Dict[str, str]]] = {}
        self._task_groups: Dict[str, List[str]] = {}  # task_group -> [pipeline_names]

    def discover_manifests(self) -> Dict[str, Path]:
        """Обнаружить все YAML манифесты в директории.
        
        Также строит карту task_group -> [pipeline_names] для группировки.
        
        Returns:
            Словарь, отображающий pipeline_name -> manifest_path
        """
        manifest_files = sorted(self.manifests_dir.glob("*.yaml"))
        result: Dict[str, Path] = {}
        
        for mpath in manifest_files:
            try:
                manifest = _load_manifest_raw(mpath)
                pipeline_name = _extract_pipeline_name(manifest, mpath)
                
                if pipeline_name in result:
                    raise ValueError(
                        f"Дубликат имени пайплайна '{pipeline_name}' найден в:\n"
                        f"  - {result[pipeline_name]}\n"
                        f"  - {mpath}"
                    )
                
                result[pipeline_name] = mpath
                
                # Извлекаем task_group и добавляем в карту групп
                task_group = _extract_task_group(manifest)
                if task_group:
                    if task_group not in self._task_groups:
                        self._task_groups[task_group] = []
                    self._task_groups[task_group].append(pipeline_name)
                    logger.debug(f"Пайплайн {pipeline_name} добавлен в группу: {task_group}")
                
                logger.debug(f"Обнаружен манифест: {pipeline_name} -> {mpath.name}")
                
            except Exception as e:
                logger.warning(f"Пропуск некорректного манифеста {mpath.name}: {e}")
                continue
        
        return result

    def extract_dependencies(self) -> Dict[str, List[Dict[str, str]]]:
        """Извлечь зависимости из всех манифестов.
        
        Returns:
            Словарь, отображающий pipeline_name -> список зависимостей (structured format)
        """
        if not self._manifest_map:
            self._manifest_map = self.discover_manifests()
        
        result: Dict[str, List[Dict[str, str]]] = {}
        
        for pipeline_name, mpath in self._manifest_map.items():
            try:
                manifest = _load_manifest_raw(mpath)
                depends_on_structured = _extract_depends_on(manifest, mpath)
                result[pipeline_name] = depends_on_structured
                
                if depends_on_structured:
                    deps_str = []
                    for dep in depends_on_structured:
                        if dep["type"] == "group":
                            deps_str.append(f"group:{dep['value']}")
                        elif dep["type"] == "path":
                            deps_str.append(f"path:{dep['value']}")
                        else:
                            deps_str.append(dep['value'])
                    
                    logger.info(
                        f"Пайплайн '{pipeline_name}' зависит от: {', '.join(deps_str)}"
                    )
                
            except Exception as e:
                raise InvalidDependencyError(
                    f"Не удалось извлечь зависимости из {mpath}: {e}"
                ) from e
        
        return result

    def build_graph(self, *, validate: bool = True) -> DependencyGraph:
        """Построить и опционально валидировать граф зависимостей.
        
        Поддерживает зависимости от:
        - Конкретных пайплайнов (по имени)
        - Файлов манифестов (по path)
        - Групп пайплайнов (по task_group)
        
        Args:
            validate: Если True, валидировать граф на наличие циклов и отсутствующих зависимостей
            
        Returns:
            Экземпляр DependencyGraph
            
        Raises:
            ManifestNotFoundError: Если зависимость ссылается на несуществующий манифест
            CyclicDependencyError: Если обнаружены циклические зависимости
            InvalidDependencyError: Если объявления зависимостей некорректны
        """
        self._manifest_map = self.discover_manifests()
        self._dependency_map = self.extract_dependencies()
        
        graph = DependencyGraph()
        
        # Сначала добавить все узлы
        for pipeline_name, mpath in self._manifest_map.items():
            depends_on_structured = self._dependency_map.get(pipeline_name, [])
            
            # Разрешаем зависимости в список имен пайплайнов
            resolved_deps = self._resolve_dependencies(pipeline_name, depends_on_structured)
            
            # Валидировать, что все зависимости существуют
            for dep in resolved_deps:
                if dep not in self._manifest_map:
                    raise ManifestNotFoundError(
                        f"Пайплайн '{pipeline_name}' зависит от '{dep}', "
                        f"но манифест для '{dep}' не существует в {self.manifests_dir}"
                    )
            
            graph.add_node(
                name=pipeline_name,
                depends_on=resolved_deps,
                metadata={
                    "manifest_path": str(mpath),
                    "manifest_name": mpath.name,
                },
            )
        
        if validate:
            try:
                graph.validate()
                logger.info(
                    f"Граф зависимостей валидирован успешно: "
                    f"{len(self._manifest_map)} пайплайнов, "
                    f"{len(self._task_groups)} групп"
                )
            except CyclicDependencyError as e:
                logger.error(f"Обнаружена циклическая зависимость: {e.cycle}")
                raise
        
        return graph
    
    def _resolve_dependencies(
        self,
        pipeline_name: str,
        depends_on_structured: List[Dict[str, str]]
    ) -> List[str]:
        """Разрешает структурированные зависимости в список имен пайплайнов
        
        Args:
            pipeline_name: Имя пайплайна, для которого разрешаем зависимости
            depends_on_structured: Структурированный список зависимостей
        
        Returns:
            Список имен пайплайнов, от которых зависит данный пайплайн
        """
        resolved = []
        
        for dep in depends_on_structured:
            dep_type = dep["type"]
            dep_value = dep["value"]
            
            if dep_type == "name":
                # Прямая зависимость от пайплайна
                resolved.append(dep_value)
            
            elif dep_type == "path":
                # Зависимость от файла - извлекаем имя пайплайна
                if dep_value.endswith(".yaml"):
                    # Находим пайплайн по имени файла
                    manifest_path = self.manifests_dir / dep_value
                    if manifest_path.exists():
                        try:
                            manifest = _load_manifest_raw(manifest_path)
                            resolved_name = _extract_pipeline_name(manifest, manifest_path)
                            resolved.append(resolved_name)
                        except Exception as e:
                            logger.warning(
                                f"Не удалось загрузить зависимость {dep_value} "
                                f"для {pipeline_name}: {e}"
                            )
                    else:
                        logger.warning(
                            f"Файл зависимости не найден: {manifest_path} "
                            f"для пайплайна {pipeline_name}"
                        )
                else:
                    # path без .yaml - считаем как имя пайплайна
                    resolved.append(dep_value)
            
            elif dep_type == "group":
                # Зависимость от группы - добавляем все пайплайны группы
                group_pipelines = self._task_groups.get(dep_value, [])
                if not group_pipelines:
                    logger.warning(
                        f"Группа '{dep_value}' не найдена для пайплайна {pipeline_name}"
                    )
                else:
                    resolved.extend(group_pipelines)
                    logger.debug(
                        f"Зависимость от группы '{dep_value}' разрешена в: {group_pipelines}"
                    )
        
        return resolved

    def get_manifest_path(self, pipeline_name: str) -> Optional[Path]:
        """Получить путь к манифесту для имени пайплайна."""
        if not self._manifest_map:
            self._manifest_map = self.discover_manifests()
        return self._manifest_map.get(pipeline_name)

    def get_dependencies(self, pipeline_name: str) -> List[str]:
        """Получить прямые зависимости для пайплайна (разрешенные имена).
        
        Args:
            pipeline_name: Имя пайплайна
        
        Returns:
            Список имен пайплайнов-зависимостей
        """
        if not self._dependency_map:
            self._dependency_map = self.extract_dependencies()
        
        structured_deps = self._dependency_map.get(pipeline_name, [])
        return self._resolve_dependencies(pipeline_name, structured_deps)
    
    def get_task_group(self, pipeline_name: str) -> Optional[str]:
        """Получить task_group для пайплайна.
        
        Args:
            pipeline_name: Имя пайплайна
        
        Returns:
            Имя task_group или None
        """
        if not self._manifest_map:
            self._manifest_map = self.discover_manifests()
        
        manifest_path = self._manifest_map.get(pipeline_name)
        if not manifest_path:
            return None
        
        manifest = _load_manifest_raw(manifest_path)
        return _extract_task_group(manifest)
    
    def get_pipelines_in_group(self, task_group: str) -> List[str]:
        """Получить все пайплайны в указанной группе.
        
        Args:
            task_group: Имя группы
        
        Returns:
            Список имен пайплайнов в группе
        """
        if not self._task_groups:
            self.discover_manifests()
        
        return self._task_groups.get(task_group, [])

    def get_lineage_report(self) -> str:
        """Сгенерировать человекочитаемый отчёт о lineage.
        
        Returns:
            Многострочная строка с информацией о зависимостях
        """
        graph = self.build_graph(validate=True)
        
        lines: List[str] = []
        lines.append("=" * 80)
        lines.append("ОТЧЁТ О LINEAGE DLT ПАЙПЛАЙНОВ")
        lines.append("=" * 80)
        lines.append(f"Всего пайплайнов: {len(self._manifest_map)}")
        lines.append("")
        
        execution_levels = graph.get_execution_order()
        lines.append(f"Уровней выполнения: {len(execution_levels)}")
        lines.append("")
        
        for level_idx, level_pipelines in enumerate(execution_levels):
            lines.append(f"Уровень {level_idx} (могут выполняться параллельно):")
            for pipeline in level_pipelines:
                deps = graph.get_dependencies(pipeline)
                if deps:
                    deps_str = ", ".join(sorted(deps))
                    lines.append(f"  - {pipeline} (зависит от: {deps_str})")
                else:
                    lines.append(f"  - {pipeline} (без зависимостей)")
            lines.append("")
        
        lines.append("=" * 80)
        lines.append("ТОПОЛОГИЧЕСКИЙ ПОРЯДОК (последовательность выполнения):")
        lines.append("=" * 80)
        topo_order = graph.topological_sort()
        for idx, pipeline in enumerate(topo_order, 1):
            lines.append(f"{idx:3d}. {pipeline}")
        
        return "\n".join(lines)

    def __repr__(self) -> str:
        return (
            f"ManifestDependencyResolver(manifests_dir={self.manifests_dir}, "
            f"manifests={len(self._manifest_map)})"
        )