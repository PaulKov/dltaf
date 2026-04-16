#!/usr/bin/env python3
"""CLI for rendering lineage and dependency reports for dltaf manifests."""

import argparse
import json
import sys
from pathlib import Path

# Добавить родительскую директорию в путь для импортов
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from lineage import ManifestDependencyResolver, CyclicDependencyError


def format_text_report(resolver: ManifestDependencyResolver) -> str:
    """Generate a human-readable dependency report."""
    try:
        return resolver.get_lineage_report()
    except CyclicDependencyError as e:
        return f"ERROR: {e}\n\nCycle: {' -> '.join(e.cycle)}"
    except Exception as e:
        return f"ERROR: {e}"


def format_json_report(resolver: ManifestDependencyResolver) -> str:
    """Generate a machine-readable JSON report."""
    try:
        graph = resolver.build_graph(validate=True)
        
        report = {
            "status": "success",
            "total_pipelines": len(resolver._manifest_map),
            "pipelines": {},
            "execution_order": {
                "topological": graph.topological_sort(),
                "levels": graph.get_execution_order(),
            }
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
        
    except CyclicDependencyError as e:
        return json.dumps({
            "status": "error",
            "error": "cyclic_dependency",
            "message": str(e),
            "cycle": e.cycle,
        }, indent=2, ensure_ascii=False)
    except Exception as e:
        return json.dumps({
            "status": "error",
            "error": type(e).__name__,
            "message": str(e),
        }, indent=2, ensure_ascii=False)


def format_mermaid_diagram(resolver: ManifestDependencyResolver) -> str:
    """Generate a Mermaid dependency graph."""
    try:
        graph = resolver.build_graph(validate=True)
        
        lines = ["graph TD"]
        
        # Добавить узлы
        for pipeline_name in resolver._manifest_map:
            safe_name = pipeline_name.replace("-", "_").replace(".", "_")
            lines.append(f"    {safe_name}[{pipeline_name}]")
        
        # Добавить рёбра (зависимости)
        # dep --> pipeline означает: "dep выполняется ПЕРЕД pipeline"
        for pipeline_name in resolver._manifest_map:
            safe_name = pipeline_name.replace("-", "_").replace(".", "_")
            deps = graph.get_dependencies(pipeline_name)
            for dep in deps:
                safe_dep = dep.replace("-", "_").replace(".", "_")
                lines.append(f"    {safe_dep} --> {safe_name}")
        
        return "\n".join(lines)
        
    except CyclicDependencyError as e:
        return f"Could not generate diagram: cyclic dependency detected\n\nCycle: {' -> '.join(e.cycle)}"
    except Exception as e:
        return f"Could not generate diagram: {e}"


def main():
    parser = argparse.ArgumentParser(
        description="Render lineage and dependency reports for dltaf manifests",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    
    parser.add_argument(
        "--manifests-dir",
        type=Path,
        default=Path(__file__).resolve().parent.parent / "dlt_pipelines" / "manifests",
        help="Directory with YAML manifests (default: dlt_pipelines/manifests)",
    )
    
    parser.add_argument(
        "--format",
        choices=["text", "json", "mermaid"],
        default="text",
        help="Output format (default: text)",
    )
    
    args = parser.parse_args()
    
    if not args.manifests_dir.exists():
        print(f"ERROR: manifests directory not found: {args.manifests_dir}", file=sys.stderr)
        sys.exit(1)
    
    try:
        resolver = ManifestDependencyResolver(args.manifests_dir)
        
        if args.format == "text":
            print(format_text_report(resolver))
        elif args.format == "json":
            print(format_json_report(resolver))
        elif args.format == "mermaid":
            print(format_mermaid_diagram(resolver))
        
        # Проверка на ошибки валидации
        try:
            resolver.build_graph(validate=True)
            sys.exit(0)
        except CyclicDependencyError:
            sys.exit(1)
        except Exception:
            sys.exit(1)
            
    except Exception as e:
        print(f"ERROR: {e}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
