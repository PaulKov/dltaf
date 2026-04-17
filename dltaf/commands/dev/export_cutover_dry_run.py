from __future__ import annotations

import argparse

from ...services.dev.repo_split import RepoSplitService
from ..base import Command, ensure_parser


class DevExportCutoverDryRunCommand(Command):
    name = "export-cutover-dry-run"
    help = "Собрать полный dry-run bundle с двумя будущими репозиториями и cutover-документацией."
    description = "Экспортирует `dltaf-repo/` и `dltaf-airflow-repo/` skeletons вместе с move maps, layouts и CI/cutover документами."
    examples = (
        "dltaf dev export-cutover-dry-run --out-dir build/repo-split-dry-run",
        "dltaf dev export-cutover-dry-run --out-dir build/repo-split-dry-run --clean",
    )

    def register(self, subparsers: argparse._SubParsersAction) -> argparse.ArgumentParser:
        p = subparsers.add_parser(self.name, help=self.help, description=self.description)
        p.add_argument("--out-dir", required=True, help="Output directory for exported dry-run split bundle.")
        p.add_argument("--clean", action="store_true", help="Delete output directory before export.")
        p.set_defaults(_command=self)
        return ensure_parser(p)

    def run(self, args: argparse.Namespace, ctx) -> int:
        svc = RepoSplitService(ctx=ctx, logger=ctx.logger)
        return svc.export_cutover_dry_run(args)
