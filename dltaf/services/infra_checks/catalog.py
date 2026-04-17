from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, List, Optional

from dltaf.extensions.infra_checks import build_infra_check_registry, select_infra_check_names
from dltaf.services.infra_checks.common import build_cli_ctx, dump_payload, render_table
from dltaf.services.manifests.loader import load_manifest


@dataclass(frozen=True)
class InfraChecksCatalogReport:
    rows: List[Dict[str, Any]]
    plugin_report: Dict[str, Any]


def add_infra_checks_list_arguments(parser) -> None:
    parser.add_argument("--manifest", help="Path to a manifest YAML to compute applies/selected")
    parser.add_argument(
        "--no-env-plugins",
        action="store_true",
        default=False,
        help="Do not include plugins from DLT_INFRA_CHECK_PLUGINS env var",
    )
    parser.add_argument(
        "--format",
        choices=["table", "json", "yaml"],
        default="table",
        help="Output format",
    )
    parser.add_argument("--selected-only", action="store_true", help="Show only selected checks")
    parser.add_argument("--show-plugins", action="store_true", help="Include plugin load report")


class InfraChecksCatalogService:
    def __init__(self, *, logger) -> None:
        self.logger = logger

    def build_report(
        self,
        *,
        manifest_path: Optional[str],
        include_env_plugins: bool,
        selected_only: bool,
    ) -> InfraChecksCatalogReport:
        manifest = load_manifest(manifest_path) if manifest_path else None
        ctx = build_cli_ctx()
        registry, plugin_report = build_infra_check_registry(
            manifest=manifest,
            ctx=ctx,
            include_env_plugins=include_env_plugins,
        )
        selected_names: List[str] = []
        applies_by_name: Dict[str, bool] = {}
        if manifest is not None:
            selected_names = select_infra_check_names(manifest=manifest, ctx=ctx, registry=registry)
            for check in registry.all():
                try:
                    applies_by_name[check.name] = bool(check.applies(manifest, ctx))
                except Exception:
                    applies_by_name[check.name] = False
        rows: List[Dict[str, Any]] = []
        for info in registry.infos():
            applies = "?" if manifest is None else ("yes" if applies_by_name.get(info.name) else "no")
            selected = "?" if manifest is None else ("yes" if info.name in selected_names else "no")
            rows.append(
                {
                    "name": info.name,
                    "origin": info.origin,
                    "description": info.description,
                    "applies": applies,
                    "selected": selected,
                }
            )
        if selected_only and manifest is not None:
            rows = [row for row in rows if row.get("selected") == "yes"]
        return InfraChecksCatalogReport(rows=rows, plugin_report=plugin_report)

    def emit(self, report: InfraChecksCatalogReport, *, fmt: str, show_plugins: bool) -> int:
        if fmt in {"json", "yaml"}:
            payload: Dict[str, Any] = {"checks": report.rows}
            if show_plugins:
                payload["plugins"] = report.plugin_report
            print(dump_payload(payload, fmt), end="")
            return 0
        print(render_table(report.rows), end="")
        if show_plugins:
            print("\nPLUGINS")
            print(dump_payload(report.plugin_report, "yaml"), end="")
        return 0

    def run(self, args) -> int:
        report = self.build_report(
            manifest_path=getattr(args, "manifest", None),
            include_env_plugins=not bool(getattr(args, "no_env_plugins", False)),
            selected_only=bool(getattr(args, "selected_only", False)),
        )
        return self.emit(
            report,
            fmt=str(getattr(args, "format", "table")),
            show_plugins=bool(getattr(args, "show_plugins", False)),
        )


__all__ = [
    "InfraChecksCatalogReport",
    "InfraChecksCatalogService",
    "add_infra_checks_list_arguments",
]
