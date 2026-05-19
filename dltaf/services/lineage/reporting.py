from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any

from dltaf.services.execution.redaction_service import RedactionService
from lineage import CyclicDependencyError, ManifestDependencyResolver


def add_lineage_arguments(parser) -> None:
    parser.add_argument(
        "--manifests-dir",
        default="dlt_pipelines/manifests",
        help="Directory with YAML manifests (default: dlt_pipelines/manifests)",
    )
    parser.add_argument(
        "--format",
        choices=["text", "json", "mermaid"],
        default="text",
        help="Output format (default: text)",
    )


class LineageReportService:
    def __init__(self, *, ctx, logger) -> None:
        self.ctx = ctx
        self.logger = logger
        self.redaction = RedactionService()

    def _resolve_manifests_dir(self, value: str | Path) -> Path:
        path = Path(value).expanduser()
        if path.is_absolute():
            return path.resolve()
        return (self.ctx.repo_root / path).resolve()

    def _resolver(self, manifests_dir: Path) -> ManifestDependencyResolver:
        return ManifestDependencyResolver(manifests_dir)

    def format_text_report(self, resolver: ManifestDependencyResolver) -> str:
        try:
            return resolver.get_lineage_report()
        except CyclicDependencyError as exc:
            return f"❌ ОШИБКА: {self.redaction.safe_exception(exc)}\n\nЦикл: {' → '.join(exc.cycle)}"
        except Exception as exc:
            return f"❌ ОШИБКА: {self.redaction.safe_exception(exc)}"

    def format_json_report(self, resolver: ManifestDependencyResolver) -> str:
        try:
            graph = resolver.build_graph(validate=True)
            report: dict[str, Any] = {
                "status": "success",
                "total_pipelines": len(resolver._manifest_map),
                "pipelines": {},
                "execution_order": {
                    "topological": graph.topological_sort(),
                    "levels": graph.get_execution_order(),
                },
            }
            for pipeline_name in resolver._manifest_map:
                node = graph.get_node(pipeline_name)
                report["pipelines"][pipeline_name] = {
                    "dependencies": list(node.dependencies) if node else [],
                    "dependents": list(graph.get_dependents(pipeline_name)),
                    "transitive_dependencies": list(graph.get_transitive_dependencies(pipeline_name)),
                    "manifest_path": str(resolver.get_manifest_path(pipeline_name)),
                }
            return json.dumps(report, indent=2, ensure_ascii=False)
        except CyclicDependencyError as exc:
            return json.dumps(
                {
                    "status": "error",
                    "error": "cyclic_dependency",
                    "message": self.redaction.safe_exception(exc),
                    "cycle": exc.cycle,
                },
                indent=2,
                ensure_ascii=False,
            )
        except Exception as exc:
            return json.dumps(
                {
                    "status": "error",
                    "error": type(exc).__name__,
                    "message": self.redaction.safe_exception(exc),
                },
                indent=2,
                ensure_ascii=False,
            )

    def format_mermaid_diagram(self, resolver: ManifestDependencyResolver) -> str:
        try:
            graph = resolver.build_graph(validate=True)
            lines = ["graph TD"]
            for pipeline_name in resolver._manifest_map:
                safe_name = pipeline_name.replace("-", "_").replace(".", "_")
                lines.append(f"    {safe_name}[{pipeline_name}]")
            for pipeline_name in resolver._manifest_map:
                safe_name = pipeline_name.replace("-", "_").replace(".", "_")
                for dep in graph.get_dependencies(pipeline_name):
                    safe_dep = dep.replace("-", "_").replace(".", "_")
                    lines.append(f"    {safe_dep} --> {safe_name}")
            return "\n".join(lines)
        except CyclicDependencyError as exc:
            return (
                "❌ Невозможно сгенерировать диаграмму: Обнаружена циклическая зависимость\n\n"
                f"Цикл: {' → '.join(exc.cycle)}"
            )
        except Exception as exc:
            return f"❌ Невозможно сгенерировать диаграмму: {self.redaction.safe_exception(exc)}"

    def render(self, *, resolver: ManifestDependencyResolver, fmt: str) -> str:
        if fmt == "json":
            return self.format_json_report(resolver)
        if fmt == "mermaid":
            return self.format_mermaid_diagram(resolver)
        return self.format_text_report(resolver)

    def run(self, args: argparse.Namespace) -> int:
        manifests_dir = self._resolve_manifests_dir(str(args.manifests_dir))
        if not manifests_dir.exists():
            print(f"❌ Ошибка: Директория манифестов не найдена: {manifests_dir}")
            return 1
        try:
            resolver = self._resolver(manifests_dir)
            print(self.render(resolver=resolver, fmt=str(args.format)))
            resolver.build_graph(validate=True)
            return 0
        except CyclicDependencyError:
            return 1
        except Exception as exc:
            print(f"❌ Ошибка: {self.redaction.safe_exception(exc)}")
            return 1


__all__ = ['LineageReportService', 'add_lineage_arguments']
