"""Реализация графа зависимостей с детекцией циклов.

Этот модуль предоставляет универсальный, production-grade направленный граф для управления
зависимостями между пайплайнами. Поддерживаемые операции:
  - Добавление узлов и рёбер (зависимостей)
  - Топологическая сортировка
  - Детекция циклов с помощью DFS
  - Обратный поиск зависимостей

Принципы проектирования:
  - Single Responsibility: только операции с графом
  - Type-safe с правильными аннотациями типов
  - Immutable после валидации (thread-safe чтение)
  - Понятные сообщения об ошибках для отладки
"""

from __future__ import annotations

from collections import defaultdict, deque
from dataclasses import dataclass, field
from typing import Dict, FrozenSet, List, Optional, Set


class CyclicDependencyError(Exception):
    """Исключение при обнаружении циклической зависимости в графе."""

    def __init__(self, cycle: List[str]) -> None:
        self.cycle = cycle
        cycle_str = " -> ".join(cycle)
        super().__init__(f"Обнаружена циклическая зависимость: {cycle_str}")


@dataclass(frozen=True)
class DependencyNode:
    """Узел в графе зависимостей.
    
    Атрибуты:
        name: Уникальный идентификатор узла (например, имя пайплайна)
        dependencies: Множество имён узлов, от которых зависит данный узел
        metadata: Опциональные метаданные (например, путь к манифесту, теги)
    """

    name: str
    dependencies: FrozenSet[str] = field(default_factory=frozenset)
    metadata: Dict[str, str] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if not self.name or not self.name.strip():
            raise ValueError("Имя узла не может быть пустым")


class DependencyGraph:
    """Направленный ациклический граф (DAG) для зависимостей пайплайнов.
    
    Этот класс строит граф зависимостей и валидирует его на наличие циклов.
    После валидации предоставляет топологический порядок и запросы зависимостей
    
    """

    def __init__(self) -> None:
        self._nodes: Dict[str, DependencyNode] = {}
        self._adjacency: Dict[str, Set[str]] = defaultdict(set)  # прямой граф: узел -> зависимые от него
        self._reverse_adjacency: Dict[str, Set[str]] = defaultdict(set)  # обратный: узел -> от кого зависит
        self._validated: bool = False

    def add_node(
        self,
        name: str,
        *,
        depends_on: Optional[List[str]] = None,
        metadata: Optional[Dict[str, str]] = None,
    ) -> None:
        """Добавить узел в граф.
        
        Args:
            name: Уникальный идентификатор узла
            depends_on: Список имён узлов, от которых зависит данный узел
            metadata: Опциональный словарь с метаданными
            
        Raises:
            ValueError: Если узел уже существует или имя некорректно
        """
        if name in self._nodes:
            raise ValueError(f"Узел '{name}' уже существует в графе")

        deps = frozenset(depends_on or [])
        node = DependencyNode(
            name=name,
            dependencies=deps,
            metadata=dict(metadata or {}),
        )
        
        self._nodes[name] = node
        
        # Построение списков смежности
        for dep in deps:
            self._adjacency[dep].add(name)  # dep -> name (name зависит от dep)
            self._reverse_adjacency[name].add(dep)  # name <- dep
        
        # Инвалидировать статус валидации при изменении графа
        self._validated = False

    def get_node(self, name: str) -> Optional[DependencyNode]:
        """Получить узел по имени."""
        return self._nodes.get(name)

    def get_all_nodes(self) -> List[DependencyNode]:
        """Получить все узлы графа."""
        return list(self._nodes.values())

    def get_dependencies(self, name: str) -> Set[str]:
        """Получить прямые зависимости узла (узлы, от которых он зависит)"""
        node = self._nodes.get(name)
        if not node:
            return set()
        return set(node.dependencies)

    def get_dependents(self, name: str) -> Set[str]:
        """Получить прямых зависимых узлов (узлы, которые зависят от данного)."""
        return set(self._adjacency.get(name, set()))

    def validate(self) -> None:
        """Валидировать граф на наличие циклов и отсутствующих зависимостей.
        
        Raises:
            ValueError: Если какие-либо зависимости ссылаются на несуществующие узлы
            CyclicDependencyError: Если обнаружен цикл
        """
        # Проверка на отсутствующие зависимости
        for node in self._nodes.values():
            for dep in node.dependencies:
                if dep not in self._nodes:
                    raise ValueError(
                        f"Узел '{node.name}' зависит от '{dep}', "
                        f"но '{dep}' не существует в графе"
                    )

        # Проверка на циклы с помощью DFS
        cycle = self._detect_cycle()
        if cycle:
            raise CyclicDependencyError(cycle)

        self._validated = True

    def _detect_cycle(self) -> Optional[List[str]]:
        """Обнаружить циклы с помощью DFS (поиск в глубину).
        
        Returns:
            Список имён узлов, образующих цикл, или None если циклов нет
        """
        # Состояния DFS: 0 = не посещён, 1 = в процессе обхода, 2 = полностью обработан
        state: Dict[str, int] = {name: 0 for name in self._nodes}

        def dfs(node: str, path: List[str]) -> Optional[List[str]]:
            if state[node] == 1:  # Обратное ребро - цикл обнаружен
                # Восстановить цикл из пути
                cycle_start = path.index(node)
                return path[cycle_start:] + [node]
            
            if state[node] == 2:  # Уже полностью обработан
                return None

            state[node] = 1  # Пометить как "в процессе обхода"
            path.append(node)

            # Посетить все узлы, которые зависят от текущего узла
            for dependent in self._adjacency.get(node, set()):
                cycle = dfs(dependent, path)
                if cycle:
                    return cycle

            path.pop()
            state[node] = 2  # Пометить как "обработан"
            return None

        # Проверить все узлы (для обработки несвязных компонент)
        for node_name in self._nodes:
            if state[node_name] == 0:
                cycle = dfs(node_name, [])
                if cycle:
                    return cycle

        return None

    def topological_sort(self) -> List[str]:
        """Вернуть узлы в топологическом порядке (зависимости идут первыми)
        
        Returns:
            Список имён узлов в топологическом порядке
            
        Raises:
            RuntimeError: Если граф не был валидирован
            CyclicDependencyError: Если существует цикл (не должно случиться после validate())
        """
        if not self._validated:
            raise RuntimeError(
                "Граф должен быть валидирован перед топологической сортировкой"
            )

        # Алгоритм Кана (Kahn's algorithm)
        in_degree: Dict[str, int] = {name: 0 for name in self._nodes}
        
        for node in self._nodes.values():
            for dep in node.dependencies:
                in_degree[node.name] += 1

        queue: deque[str] = deque([name for name, deg in in_degree.items() if deg == 0])
        result: List[str] = []

        while queue:
            node = queue.popleft()
            result.append(node)

            # Уменьшить степень входа для зависимых узлов
            for dependent in self._adjacency.get(node, set()):
                in_degree[dependent] -= 1
                if in_degree[dependent] == 0:
                    queue.append(dependent)

        if len(result) != len(self._nodes):
            # Это не должно произойти после validate(), но защитная проверка
            raise CyclicDependencyError(
                list(set(self._nodes.keys()) - set(result))
            )

        return result

    def get_execution_order(self) -> List[List[str]]:
        """Получить узлы, сгруппированные по уровням выполнения (для параллельного выполнения)
        
        Returns:
            Список списков, где каждый внутренний список содержит узлы, которые могут
            выполняться параллельно (находятся на одном уровне в дереве зависимостей)
        """
        if not self._validated:
            raise RuntimeError(
                "Граф должен быть валидирован перед получением порядка выполнения "
                "Вызовите validate() сначала"
            )

        in_degree: Dict[str, int] = {name: 0 for name in self._nodes}
        
        for node in self._nodes.values():
            for dep in node.dependencies:
                in_degree[node.name] += 1

        levels: List[List[str]] = []
        current_level = [name for name, deg in in_degree.items() if deg == 0]

        while current_level:
            levels.append(sorted(current_level))  # Сортировка для детерминированного вывода
            next_level: List[str] = []

            for node in current_level:
                for dependent in self._adjacency.get(node, set()):
                    in_degree[dependent] -= 1
                    if in_degree[dependent] == 0:
                        next_level.append(dependent)

            current_level = next_level

        return levels

    def get_transitive_dependencies(self, name: str) -> Set[str]:
        """Получить все транзитивные зависимости (рекурсивно) для узла
        
        Args:
            name: Имя узла
            
        Returns:
            Множество всех узлов, от которых зависит данный узел (прямо или косвенно)
        """
        if name not in self._nodes:
            return set()

        visited: Set[str] = set()
        queue: deque[str] = deque(self._nodes[name].dependencies)

        while queue:
            dep = queue.popleft()
            if dep in visited:
                continue
            visited.add(dep)
            
            # Добавить транзитивные зависимости
            dep_node = self._nodes.get(dep)
            if dep_node:
                for transitive_dep in dep_node.dependencies:
                    if transitive_dep not in visited:
                        queue.append(transitive_dep)

        return visited

    def __repr__(self) -> str:
        return (
            f"DependencyGraph(nodes={len(self._nodes)}, "
            f"validated={self._validated})"
        )