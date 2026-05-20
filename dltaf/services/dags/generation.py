from __future__ import annotations

import argparse
import subprocess
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Tuple

import yaml

from dltaf.services.execution.redaction_service import RedactionService
from lineage import DependencyGraph, ManifestDependencyResolver

SIMPLE_DAG_TEMPLATE = '''"""DAG для {pipeline_name}.

⚠️ АВТОМАТИЧЕСКИ СГЕНЕРИРОВАН из YAML манифеста для CI/CD валидации.
Не редактируйте вручную — изменения будут перезаписаны при следующей генерации!

Простой независимый пайплайн без зависимостей.
{description}
"""

import sys
from pathlib import Path
from datetime import timedelta

_dag_file_path = Path(__file__).resolve()
_project_root = _dag_file_path.parent.parent
_manifests_dir = _project_root / "dlt_manifests"
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
        manifests_dir=_manifests_dir,
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

_dag_file_path = Path(__file__).resolve()
_project_root = _dag_file_path.parent.parent
_manifests_dir = _project_root / "dlt_manifests"
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
        manifests_dir=_manifests_dir,
    )
'''


@dataclass(frozen=True)
class DAGGenerationSummary:
    generated: int
    skipped: int


def add_dag_generation_arguments(parser) -> None:
    parser.add_argument(
        "--manifests-dir",
        default="dlt_pipelines/manifests",
        help="Directory with manifest YAML files (default: dlt_pipelines/manifests).",
    )
    parser.add_argument(
        "--output-dir",
        default="dags",
        help="Directory where generated DAG files are written (default: dags).",
    )
    parser.add_argument(
        "--clean",
        action="store_true",
        help="Delete previously generated local DAG files before generation.",
    )


class DAGGenerationService:
    def __init__(self, *, ctx, logger) -> None:
        self.ctx = ctx
        self.logger = logger
        self.redaction = RedactionService()

    def _resolve_path(self, value: str | Path) -> Path:
        path = Path(value).expanduser()
        if path.is_absolute():
            return path.resolve()
        return (self.ctx.repo_root / path).resolve()

    @staticmethod
    def _is_git_tracked(root: Path, relative_path: Path) -> bool:
        try:
            result = subprocess.run(
                ["git", "ls-files", "--error-unmatch", str(relative_path)],
                capture_output=True,
                text=True,
                timeout=5,
                cwd=str(root),
            )
            return result.returncode == 0
        except (FileNotFoundError, subprocess.TimeoutExpired):
            return False

    def _load_manifest_config(self, manifest_path: Path) -> Dict[str, Any]:
        data = yaml.safe_load(manifest_path.read_text(encoding="utf-8"))
        if not isinstance(data, dict):
            raise ValueError(f"Manifest must be a YAML mapping: {manifest_path}")
        return data

    def _extract_airflow_params(self, manifest_data: Dict[str, Any], pipeline_name: str) -> Dict[str, Any]:
        airflow_config = manifest_data.get("airflow", {}) or {}
        parts = pipeline_name.split("__")
        source = parts[1] if len(parts) >= 4 else "unknown"
        dataset = parts[4] if len(parts) >= 5 else "unknown"
        schedule = airflow_config.get("schedule") or {
            "postgres": "0 3 * * *",
            "oracle": "0 2 * * *",
            "mongodb": "0 5 * * *",
            "pkb": "0 6 * * *",
        }.get(source.split("_")[0], "0 * * * *")
        tags = airflow_config.get("tags") or ["dlt", source.split("_")[0], dataset]
        default_args = airflow_config.get("default_args", {}) or {}
        task_config = airflow_config.get("task", {}) or {}
        return {
            "schedule": schedule,
            "tags": list(tags),
            "retries": default_args.get("retries", 2),
            "retry_delay_minutes": default_args.get("retry_delay_minutes", 5),
            "execution_timeout_hours": task_config.get("execution_timeout_hours", 2),
        }

    def _render_simple_dag(self, pipeline_name: str, manifest_path: Path) -> str:
        manifest_data = self._load_manifest_config(manifest_path)
        airflow_params = self._extract_airflow_params(manifest_data, pipeline_name)
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

    def _get_transitive_dependencies(self, resolver: ManifestDependencyResolver, pipeline_name: str) -> List[str]:
        all_deps: List[str] = []
        visited: set[str] = set()

        def visit(name: str) -> None:
            if name in visited:
                return
            visited.add(name)
            for dep in resolver.get_dependencies(name):
                visit(dep)
                if dep not in all_deps:
                    all_deps.append(dep)

        visit(pipeline_name)
        return all_deps[::-1]

    def _render_with_deps_dag(self, resolver: ManifestDependencyResolver, pipeline_name: str, manifest_path: Path) -> str:
        manifest_data = self._load_manifest_config(manifest_path)
        airflow_params = self._extract_airflow_params(manifest_data, pipeline_name)
        all_deps = self._get_transitive_dependencies(resolver, pipeline_name)
        deps_list = "\n".join([f"- {dep}" for dep in [pipeline_name, *all_deps]])
        tags = list(airflow_params["tags"])
        if "with-dependencies" not in tags:
            tags.append("with-dependencies")
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

    def _clean_old_dags(self, *, output_dir: Path) -> None:
        print("\nУдаление старых DAG файлов...")
        patterns = ["dlt__*.py", "standalone_*.py", "domain__*.py", "simple__*.py"]
        deleted = 0
        for pattern in patterns:
            for dag_file in output_dir.glob(pattern):
                relative_path = dag_file.relative_to(self.ctx.repo_root)
                if self._is_git_tracked(self.ctx.repo_root, relative_path):
                    print(f"   Защищен: {dag_file.name} (в git, production)")
                    continue
                dag_file.unlink()
                print(f"   Удаление: {dag_file.name}")
                deleted += 1
        print(f"OK Удалено файлов: {deleted}")

    def _build_resolver(self, manifests_dir: Path) -> Tuple[ManifestDependencyResolver, DependencyGraph]:
        resolver = ManifestDependencyResolver(manifests_dir)
        graph = resolver.build_graph()
        return resolver, graph

    def generate(self, *, manifests_dir: Path, output_dir: Path, clean: bool) -> DAGGenerationSummary:
        print(f"Сканирование манифестов в: {manifests_dir}")
        resolver, _graph = self._build_resolver(manifests_dir)
        print(f"OK Найдено манифестов: {len(resolver._manifest_map)}")
        if clean:
            self._clean_old_dags(output_dir=output_dir)
        print("\nГенерация простых DAG'ов (1 манифест = 1 DAG)...")
        generated = 0
        skipped = 0
        for pipeline_name, manifest_path in resolver._manifest_map.items():
            try:
                dependencies = resolver.get_dependencies(pipeline_name)
                if dependencies:
                    content = self._render_with_deps_dag(resolver, pipeline_name, manifest_path)
                    filename = f"{pipeline_name}_with_deps.py"
                else:
                    content = self._render_simple_dag(pipeline_name, manifest_path)
                    filename = f"{pipeline_name}.py"
                output_path = output_dir / filename
                output_path.write_text(content, encoding="utf-8")
                generated += 1
                print(f"   OK {filename}")
            except Exception as exc:
                skipped += 1
                print(f"   ОШИБКА {pipeline_name}: {self.redaction.safe_exception(exc)}")
        return DAGGenerationSummary(generated=generated, skipped=skipped)

    def run(self, args: argparse.Namespace) -> int:
        manifests_dir = self._resolve_path(str(args.manifests_dir))
        output_dir = self._resolve_path(str(args.output_dir))
        if not manifests_dir.exists():
            print(f"ОШИБКА: Директория с манифестами не найдена: {manifests_dir}")
            return 1
        if not output_dir.exists():
            print(f"ОШИБКА: Выходная директория не найдена: {output_dir}")
            return 1
        print("=" * 70)
        print("Генератор Airflow DAG файлов")
        print("=" * 70)
        try:
            summary = self.generate(manifests_dir=manifests_dir, output_dir=output_dir, clean=bool(args.clean))
        except Exception as exc:
            self.logger.error("DAG generation failed: %s", self.redaction.safe_exception(exc))
            print(f"ОШИБКА: {self.redaction.safe_exception(exc)}")
            return 1
        print("\n" + "=" * 70)
        print("OK Генерация завершена!")
        print(f"   Создано DAG'ов: {summary.generated}")
        print(f"   Пропущено: {summary.skipped}")
        print("=" * 70)
        return 0 if summary.skipped == 0 else 1


__all__ = [
    'DAGGenerationService',
    'DAGGenerationSummary',
    'add_dag_generation_arguments',
]
