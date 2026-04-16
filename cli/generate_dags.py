#!/usr/bin/env python3
"""CLI for generating Airflow DAG files from YAML manifests."""

import argparse
import subprocess
import sys
from pathlib import Path
from typing import Any, Dict, List, Tuple

from lineage import ManifestDependencyResolver, DependencyGraph

_PACKAGE_ROOT = Path(__file__).resolve().parent.parent


def _resolve_path(path: Path, *, base: Path = _PACKAGE_ROOT) -> Path:
    """Разрешает путь: относительный — от base, абсолютный — как есть."""
    p = Path(path)
    if not p.is_absolute():
        p = base / p
    return p.resolve()


# Шаблоны для генерации DAG файлов
SIMPLE_DAG_TEMPLATE = '''"""DAG для {pipeline_name}.

⚠️ АВТОМАТИЧЕСКИ СГЕНЕРИРОВАН из YAML манифеста для CI/CD валидации.
Не редактируйте вручную — изменения будут перезаписаны при следующей генерации!

Простой независимый пайплайн без зависимостей.
{description}
"""

import sys
from pathlib import Path
from datetime import timedelta

# Добавить корневую директорию проекта в PYTHONPATH для импорта модулей
_dag_file_path = Path(__file__).resolve()
_project_root = _dag_file_path.parent.parent
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

from airflow import DAG
from dag_builder import build_dag_from_yaml
import pendulum

default_args = {{
    "owner": "data-platform",
    "retries": {retries},
    "retry_delay": timedelta(minutes={retry_delay_minutes}),
    "execution_timeout": timedelta(hours={execution_timeout_hours}),
}}

with DAG(
    dag_id="{dag_id}",
    default_args=default_args,
    start_date=pendulum.datetime(2024, 1, 1, tz="UTC"),
    schedule="{schedule}",
    catchup=False,
    max_active_runs=1,
    tags={tags},
    doc_md=__doc__,
) as dag:
    build_dag_from_yaml(
        dag=dag,
        manifest_path="{manifest_filename}",
    )
'''

WITH_DEPS_DAG_TEMPLATE = '''"""DAG для {pipeline_name} с автоматической загрузкой зависимостей.

⚠️ АВТОМАТИЧЕСКИ СГЕНЕРИРОВАН из YAML манифеста для CI/CD валидации.
Не редактируйте вручную — изменения будут перезаписаны при следующей генерации!

Этот DAG автоматически загружает все зависимости:
{dependencies_list}

Все зависимости автоматически разрешаются и выстраиваются в граф
через native Airflow dependencies (>>).
{description}
"""

import sys
from pathlib import Path
from datetime import timedelta

# Добавить корневую директорию проекта в PYTHONPATH для импорта модулей
_dag_file_path = Path(__file__).resolve()
_project_root = _dag_file_path.parent.parent
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

from airflow import DAG
from dag_builder import build_dag_from_yaml
import pendulum

default_args = {{
    "owner": "data-platform",
    "retries": {retries},
    "retry_delay": timedelta(minutes={retry_delay_minutes}),
    "execution_timeout": timedelta(hours={execution_timeout_hours}),
}}

with DAG(
    dag_id="{dag_id}",
    default_args=default_args,
    start_date=pendulum.datetime(2024, 1, 1, tz="UTC"),
    schedule="{schedule}",
    catchup=False,
    max_active_runs=1,
    tags={tags},
    doc_md=__doc__,
) as dag:
    build_dag_from_yaml(
        dag=dag,
        manifest_path="{manifest_filename}",
    )
'''


class DAGGenerator:
    """Генератор DAG файлов из YAML манифестов."""
    
    def __init__(
        self,
        manifests_dir: Path,
        output_dir: Path,
        clean: bool = False,
    ):
        self.manifests_dir = manifests_dir
        self.output_dir = output_dir
        self.clean = clean
        
        self.resolver = ManifestDependencyResolver(manifests_dir)
        self.graph: DependencyGraph = None
    
    def generate(self) -> Tuple[int, int]:
        """Генерирует DAG файлы.
        
        Returns:
            Tuple[generated_count, skipped_count]
        """
        print(f"Scanning manifests in: {self.manifests_dir}")
        
        # Строим граф зависимостей
        try:
            self.graph = self.resolver.build_graph()
        except Exception as e:
            print(f"ERROR: failed to build dependency graph: {e}")
            return 0, 0
        
        print(f"Found manifests: {len(self.resolver._manifest_map)}")
        
        # Очистка старых DAG'ов если нужно
        if self.clean:
            self._clean_old_dags()
        
        # Генерация DAG'ов (простой режим: 1 манифест = 1 DAG)
        return self._generate_simple_dags()
    
    def _clean_old_dags(self) -> None:
        """Удаляет старые DAG файлы (кроме тех, что уже в git — production)"""
        print("\nCleaning previously generated DAG files...")

        def _is_tracked_in_git(relative_path: Path) -> bool:
            """Проверяет, есть ли файл в git (tracked)."""
            try:
                result = subprocess.run(
                    ["git", "ls-files", "--error-unmatch", str(relative_path)],
                    capture_output=True,
                    text=True,
                    timeout=5,
                    cwd=Path.cwd(),
                )
                return result.returncode == 0
            except (FileNotFoundError, subprocess.TimeoutExpired):
                return False

        patterns = ["dlt__*.py", "standalone_*.py", "domain__*.py", "simple__*.py"]
        deleted = 0

        for pattern in patterns:
            for dag_file in self.output_dir.glob(pattern):
                # Путь относительно текущей директории (проект = dp-dlt-af)
                relative_path = self.output_dir / dag_file.name
                if _is_tracked_in_git(relative_path):
                    print(f"   protected: {dag_file.name} (tracked in git)")
                    continue

                print(f"   delete: {dag_file.name}")
                dag_file.unlink()
                deleted += 1

        print(f"Removed files: {deleted}")
    
    def _generate_simple_dags(self) -> Tuple[int, int]:
        """Генерирует простые DAG'и (1 манифест = 1 DAG)."""
        print("\nGenerating DAG files (1 manifest = 1 DAG)...")
        
        generated = 0
        skipped = 0
        
        for pipeline_name, manifest_path in self.resolver._manifest_map.items():
            try:
                # Проверяем, есть ли зависимости
                dependencies = self.resolver.get_dependencies(pipeline_name)
                
                if dependencies:
                    # DAG с автозагрузкой зависимостей
                    dag_content = self._render_with_deps_dag(pipeline_name, manifest_path)
                    dag_filename = f"{pipeline_name}_with_deps.py"
                else:
                    # Простой DAG без зависимостей
                    dag_content = self._render_simple_dag(pipeline_name, manifest_path)
                    dag_filename = f"{pipeline_name}.py"
                
                output_path = self.output_dir / dag_filename
                output_path.write_text(dag_content, encoding="utf-8")
                
                print(f"   ok {dag_filename}")
                generated += 1
                
            except Exception as e:
                print(f"   error {pipeline_name}: {e}")
                skipped += 1
        
        return generated, skipped
    
    def _load_manifest_config(self, manifest_path: Path) -> Dict[str, Any]:
        """Загружает конфигурацию из YAML манифеста."""
        import yaml
        
        with open(manifest_path, "r", encoding="utf-8") as f:
            data = yaml.safe_load(f)
        
        if not isinstance(data, dict):
            raise ValueError(f"Манифест должен быть YAML объектом: {manifest_path}")
        
        return data
    
    def _extract_airflow_params(self, manifest_data: Dict[str, Any], pipeline_name: str) -> Dict[str, Any]:
        """Извлекает параметры Airflow из манифеста с fallback на defaults."""
        airflow_config = manifest_data.get("airflow", {})
        
        # Извлекаем source для fallback тегов и schedule
        parts = pipeline_name.split("__")
        source = parts[1] if len(parts) >= 4 else "unknown"
        dataset = parts[4] if len(parts) >= 5 else "unknown"
        
        # Schedule из манифеста или default
        schedule = airflow_config.get("schedule")
        if not schedule:
            # Fallback schedule на основе source
            schedule_defaults = {
                "postgres": "0 3 * * *",
                "oracle": "0 2 * * *",
                "mongodb": "0 5 * * *",
                "pkb": "0 6 * * *",
            }
            schedule = schedule_defaults.get(source.split("_")[0], "0 * * * *")
        
        # Tags из манифеста или default
        tags = airflow_config.get("tags")
        if not tags:
            tags = ["dlt", source.split("_")[0], dataset]
        
        # Default args из манифеста
        default_args = airflow_config.get("default_args", {})
        retries = default_args.get("retries", 2)
        retry_delay_minutes = default_args.get("retry_delay_minutes", 5)
        
        # Task config
        task_config = airflow_config.get("task", {})
        execution_timeout_hours = task_config.get("execution_timeout_hours", 2)
        
        return {
            "schedule": schedule,
            "tags": tags,
            "retries": retries,
            "retry_delay_minutes": retry_delay_minutes,
            "execution_timeout_hours": execution_timeout_hours,
            "default_args": default_args,
        }
    
    def _render_simple_dag(self, pipeline_name: str, manifest_path: Path) -> str:
        """Рендерит простой DAG без зависимостей."""
        # Загружаем конфигурацию из манифеста
        manifest_data = self._load_manifest_config(manifest_path)
        airflow_params = self._extract_airflow_params(manifest_data, pipeline_name)
        
        # Формируем теги для шаблона
        tags_formatted = [f'"{tag}"' for tag in airflow_params["tags"]]
        
        return SIMPLE_DAG_TEMPLATE.format(
            pipeline_name=pipeline_name,
            description="",
            dag_id=pipeline_name,
            manifest_filename=manifest_path.name,
            schedule=airflow_params["schedule"],
            tags=f"[{', '.join(tags_formatted)}]",
            retries=airflow_params["retries"],
            retry_delay_minutes=airflow_params["retry_delay_minutes"],
            execution_timeout_hours=airflow_params["execution_timeout_hours"],
        )
    
    def _render_with_deps_dag(self, pipeline_name: str, manifest_path: Path) -> str:
        """Рендерит DAG с автозагрузкой зависимостей."""
        # Загружаем конфигурацию из манифеста
        manifest_data = self._load_manifest_config(manifest_path)
        airflow_params = self._extract_airflow_params(manifest_data, pipeline_name)
        
        # Получаем полный список зависимостей
        all_deps = self._get_transitive_dependencies(pipeline_name)
        deps_list = "\n".join([f"- {dep}" for dep in [pipeline_name] + all_deps])
        
        # Добавляем тег "with-dependencies" если его нет
        tags = airflow_params["tags"]
        if "with-dependencies" not in tags:
            tags = tags + ["with-dependencies"]
        
        tags_formatted = [f'"{tag}"' for tag in tags]
        
        return WITH_DEPS_DAG_TEMPLATE.format(
            pipeline_name=pipeline_name,
            dependencies_list=deps_list,
            description=f"\nВАЖНО: Запускается после зависимостей ({airflow_params['schedule']}).",
            dag_id=pipeline_name,
            manifest_filename=manifest_path.name,
            schedule=airflow_params["schedule"],
            tags=f"[{', '.join(tags_formatted)}]",
            retries=airflow_params["retries"],
            retry_delay_minutes=airflow_params["retry_delay_minutes"],
            execution_timeout_hours=airflow_params["execution_timeout_hours"],
        )
    
    def _get_transitive_dependencies(self, pipeline_name: str) -> List[str]:
        """Получает все транзитивные зависимости пайплайна."""
        all_deps = []
        visited = set()
        
        def visit(name: str):
            if name in visited:
                return
            visited.add(name)
            
            deps = self.resolver.get_dependencies(name)
            for dep in deps:
                visit(dep)
                if dep not in all_deps:
                    all_deps.append(dep)
        
        visit(pipeline_name)
        return all_deps[::-1]  # Reverse для правильного порядка


def main():
    parser = argparse.ArgumentParser(
        description="Генерация Airflow DAG файлов из YAML манифестов"
    )
    parser.add_argument(
        "--manifests-dir",
        type=Path,
        default=Path("dlt_pipelines/manifests"),
        help="Директория с YAML манифестами (default: dlt_pipelines/manifests)",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=Path("dags"),
        help="Директория для генерации DAG файлов (default: dags)",
    )
    parser.add_argument(
        "--clean",
        action="store_true",
        help="Удалить старые DAG файлы перед генерацией",
    )
    
    args = parser.parse_args()

    # Разрешаем пути относительно корня пакета (независимо от cwd)
    manifests_dir = _resolve_path(args.manifests_dir)
    output_dir = _resolve_path(args.output_dir)

    if not manifests_dir.exists():
        print(f"ОШИБКА: Директория с манифестами не найдена: {manifests_dir}")
        return 1

    output_dir.mkdir(parents=True, exist_ok=True)
    
    print("=" * 70)
    print("Генератор Airflow DAG файлов")
    print("=" * 70)
    
    # Генерация
    generator = DAGGenerator(
        manifests_dir=manifests_dir,
        output_dir=output_dir,
        clean=args.clean,
    )
    
    generated, skipped = generator.generate()
    
    print("\n" + "=" * 70)
    print("OK Генерация завершена!")
    print(f"   Создано DAG'ов: {generated}")
    print(f"   Пропущено: {skipped}")
    print("=" * 70)
    
    return 0 if skipped == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
