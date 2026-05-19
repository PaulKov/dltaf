from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, List, Mapping

from dltaf.services.execution.online_checks_service import OnlineChecksService
from dltaf.services.infra_checks.common import build_cli_ctx, dump_payload, parse_name_list
from dltaf.services.manifests.loader import load_manifest


@dataclass(frozen=True)
class InfraChecksRunReport:
    checks: Mapping[str, Any]
    report: Mapping[str, Any]
    strict: bool
    strict_ok: bool


def add_infra_checks_run_arguments(parser) -> None:
    parser.add_argument("--manifest", required=True, help="Path to a manifest YAML")
    parser.add_argument(
        "--no-env-plugins",
        action="store_true",
        default=False,
        help="Do not include plugins from DLT_INFRA_CHECK_PLUGINS env var",
    )
    parser.add_argument("--timeout-seconds", type=float, default=5.0, help="Timeout for checks")
    parser.add_argument(
        "--only",
        help="Comma-separated list of check names to run (overrides manifest selection)",
    )
    parser.add_argument(
        "--selected-only",
        action="store_true",
        default=False,
        help="Ignore --only and run exactly what the manifest selects",
    )
    parser.add_argument(
        "--summary-only",
        action="store_true",
        default=False,
        help="Output only {ok, errors, warnings} (CI-friendly)",
    )
    parser.add_argument(
        "--strict",
        action="store_true",
        default=False,
        help="Treat warnings as failures (exit code 3)",
    )
    parser.add_argument("--format", choices=["json", "yaml"], default="yaml", help="Output format")


class InfraChecksRunService:
    def __init__(self, *, logger) -> None:
        self.logger = logger
        self.online_checks = OnlineChecksService()

    def build_report(
        self,
        *,
        manifest_path: str,
        include_env_plugins: bool,
        timeout_seconds: float,
        only_raw: str | None,
        selected_only: bool,
        strict: bool,
    ) -> InfraChecksRunReport:
        manifest = load_manifest(manifest_path)
        ctx = build_cli_ctx()
        registry, plugin_report = self.online_checks.build_registry(
            manifest=manifest,
            ctx=ctx,
            include_env_plugins=include_env_plugins,
        )
        selected_names: List[str] = []
        if only_raw and not selected_only:
            selected_names = parse_name_list(only_raw)
        checks = self.online_checks.run(
            manifest=manifest,
            ctx=ctx,
            timeout_seconds=float(timeout_seconds),
            registry=registry,
            names_override=selected_names if selected_names else None,
        )
        checks = dict(checks)
        checks["plugins"] = plugin_report
        report = self.online_checks.evaluate(checks, registry=registry)
        strict_ok = bool(report.get("ok")) and (not strict or len(report.get("warnings") or []) == 0)
        return InfraChecksRunReport(checks=checks, report=report, strict=strict, strict_ok=bool(strict_ok))

    def emit(self, report: InfraChecksRunReport, *, fmt: str, summary_only: bool) -> int:
        if summary_only:
            print(dump_payload(report.report, fmt), end="")
            return 0 if report.strict_ok else 3
        payload: Dict[str, Any] = {"checks": report.checks, "report": report.report}
        if report.strict:
            payload["strict"] = True
            payload["strict_ok"] = bool(report.strict_ok)
        print(dump_payload(payload, fmt), end="")
        return 0 if report.strict_ok else 3

    def run(self, args) -> int:
        report = self.build_report(
            manifest_path=str(args.manifest),
            include_env_plugins=not bool(getattr(args, "no_env_plugins", False)),
            timeout_seconds=float(getattr(args, "timeout_seconds", 5.0)),
            only_raw=getattr(args, "only", None),
            selected_only=bool(getattr(args, "selected_only", False)),
            strict=bool(getattr(args, "strict", False)),
        )
        return self.emit(
            report,
            fmt=str(getattr(args, "format", "yaml")),
            summary_only=bool(getattr(args, "summary_only", False)),
        )


__all__ = [
    "InfraChecksRunReport",
    "InfraChecksRunService",
    "add_infra_checks_run_arguments",
]
