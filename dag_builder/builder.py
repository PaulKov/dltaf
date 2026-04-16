"""Главная точка входа для построения DAG из YAML манифестов.

Этот модуль предоставляет публичную функцию build_dag_from_yaml()
для использования в DAG файлах.
"""

import logging
from pathlib import Path
from typing import Any, Dict, List, Optional, Union

from airflow import DAG

from dag_builder.dag_builder_core import DAGBuilder

logger = logging.getLogger(__name__)


def build_dag_from_yaml(
    dag: DAG,
    manifest_path: Optional[Union[str, Path]] = None,
    manifests: Optional[List[Union[str, Path]]] = None,
    manifests_dir: Optional[Union[str, Path]] = None,
    load_dependencies: bool = True,
    use_task_groups: Optional[bool] = None,  # None = auto-detect
    task_group_prefix: Optional[str] = None,  # DEPRECATED - не используется
    default_task_config: Optional[Dict[str, Any]] = None,
    use_virtualenv: bool = False,
) -> DAGBuilder:
    """Строит Airflow DAG из YAML манифестов с автоматическим управлением зависимостями.
    
    Эта функция является главной точкой входа для создания DAG'ов из манифестов.
    Она автоматически:
    - Загружает манифесты
    - Строит граф зависимостей (через depends_on)
    - Создает Airflow задачи
    - Выстраивает зависимости через native Airflow >> operator
    """
    # Валидация входных параметров
    if manifest_path is None and manifests is None:
        raise ValueError(
            "Необходимо указать либо manifest_path, либо manifests. "
            "Оба параметра не могут быть None."
        )
    
    # Конвертация путей
    if manifests_dir is not None:
        manifests_dir = Path(manifests_dir)
    
    # Создаем билдер
    builder = DAGBuilder(
        dag=dag,
        manifests_dir=manifests_dir,
        default_task_config=default_task_config,
        use_virtualenv=use_virtualenv,
    )
    
    # Добавляем манифесты
    if manifest_path is not None:
        # Один манифест
        logger.info(f"Добавление манифеста: {manifest_path}")
        builder.add_manifest(manifest_path)
        
        # Проверяем: если загруженный манифест имеет task_group,
        # то автоматически загружаем ВСЕ манифесты с task_group или depends_on
        manifest_obj = builder.loader.load(builder.loader.base_dir / f"{manifest_path.replace('.yaml', '')}.yaml" if not manifest_path.endswith('.yaml') else builder.loader.base_dir / manifest_path)
        has_task_group = manifest_obj.get("task_group") is not None
        
        if has_task_group:
            logger.info("Манифест имеет task_group - автоматически загружаем все манифесты с группировкой или зависимостями")
            
            # Сканируем все манифесты в директории
            all_yaml_files = builder.loader.base_dir.glob("*.yaml")
            for yaml_file in all_yaml_files:
                try:
                    other_manifest = builder.loader.load(yaml_file)
                    other_name = other_manifest.get("pipeline", {}).get("name")
                    
                    # Пропускаем уже загруженный манифест
                    if other_name == manifest_obj.get("pipeline", {}).get("name"):
                        continue
                    
                    # Загружаем если есть task_group или depends_on
                    has_other_task_group = other_manifest.get("task_group") is not None
                    has_depends_on = other_manifest.get("depends_on") is not None and len(other_manifest.get("depends_on", [])) > 0
                    
                    if has_other_task_group or has_depends_on:
                        logger.info(f"Автоматически добавлен манифест: {yaml_file.name} (task_group={has_other_task_group}, depends_on={has_depends_on})")
                        builder.add_manifest(yaml_file.name)
                except Exception as e:
                    logger.warning(f"Пропуск манифеста {yaml_file.name}: {e}")
                    continue
        
        if load_dependencies:
            logger.info("Автоматическая загрузка зависимостей включена")
            # Зависимости будут загружены автоматически в resolver.add_manifest()
    
    elif manifests is not None:
        # Несколько манифестов
        logger.info(f"Добавление {len(manifests)} манифестов")
        builder.add_manifests(manifests)
        
        if load_dependencies:
            logger.info("Автоматическая загрузка зависимостей включена")
            # Зависимости будут загружены автоматически
    
    # Автоматическое определение use_task_groups
    if use_task_groups is None:
        # Строим граф для проверки зависимостей
        graph = builder.resolver.build_graph()
        total_pipelines = len(graph.get_all_nodes())
        
        # Автоматическая логика:
        # - Если несколько манифестов или есть зависимости -> используем TaskGroups
        # - Если один манифест без зависимостей -> не используем
        if total_pipelines > 1 or load_dependencies:
            use_task_groups = True
            logger.info(
                f"Автоматическое определение: use_task_groups=True "
                f"({total_pipelines} пайплайнов, load_dependencies={load_dependencies})"
            )
        else:
            use_task_groups = False
            logger.info("Автоматическое определение: use_task_groups=False (один пайплайн без зависимостей)")
    
    # Строим граф задач
    logger.info("Построение графа задач в DAG...")
    builder.build(
        use_task_groups=use_task_groups,
        task_group_prefix=task_group_prefix,
    )
    
    logger.info(f"DAG '{dag.dag_id}' успешно построен")
    
    return builder