from __future__ import annotations

import argparse

from ...services.dev.repo_split import RepoSplitService
from ..base import Command, ensure_parser


class DevExportRepoSplitPlanCommand(Command):
    name = "export-repo-split-plan"
    help = "Экспортировать exact move maps, layouts и dry-run cutover checklist для split на 2 репозитория."
    description = "Собирает plan bundle для физического разделения на `dltaf` и `dltaf-airflow`."
    examples = (
        "dltaf dev export-repo-split-plan --out-dir build/repo-split-plan",
        "dltaf dev export-repo-split-plan --out-dir build/repo-split-plan --clean",
    )

    def register(self, subparsers: argparse._SubParsersAction) -> argparse.ArgumentParser:
        p = subparsers.add_parser(self.name, help=self.help, description=self.description)
        p.add_argument("--out-dir", required=True, help="Output directory for exported split plan bundle.")
        p.add_argument("--clean", action="store_true", help="Delete output directory before export.")
        p.set_defaults(_command=self)
        return ensure_parser(p)

    def run(self, args: argparse.Namespace, ctx) -> int:
        svc = RepoSplitService(ctx=ctx, logger=ctx.logger)
        return svc.export_repo_split_plan(args)
