from __future__ import annotations

import argparse

from ...services.docs.update_dev_metrics_service import UpdateDevMetricsService
from ..base import Command, ensure_parser


class UpdateDevMetricsCommand(Command):
    name = "update-dev-metrics"
    help = "Обновление auto-generated блока метрик в `docs/DEVELOPERS.md`."
    description = "Refresh docs/CODE_METRICS.md and the generated metrics block in developer docs."
    examples = (
        "dltaf docs update-dev-metrics",
        "dltaf docs update-dev-metrics --check",
    )
    legacy_entrypoint = None

    def register(self, subparsers: argparse._SubParsersAction) -> argparse.ArgumentParser:
        p = subparsers.add_parser(
            self.name,
            help=self.help,
            description=self.description,
        )
        p.add_argument("--check", action="store_true", help="Only check whether docs are up to date.")
        p.add_argument("--docs-dir", default="docs", help="Docs directory (default: docs).")
        p.add_argument("--show-diff", action="store_true", help="Show a unified diff when --check fails.")
        p.add_argument("--diff-lines", type=int, default=200, help="Maximum diff lines to print.")
        p.set_defaults(_command=self)
        return ensure_parser(p)

    def run(self, args: argparse.Namespace, ctx) -> int:
        svc = UpdateDevMetricsService(ctx=ctx, logger=ctx.logger)
        return svc.run(args)
