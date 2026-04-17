from __future__ import annotations

import argparse

from ...services.dev.repo_split import RepoSplitService
from ..base import Command, ensure_parser


class DevExportImportRehearsalCommand(Command):
    name = "export-import-rehearsal"
    help = "Собрать bundle для rehearsal-импорта skeleton'ов в два временных репозитория."
    description = "Экспортирует cutover dry-run bundle и дополняет его shell scripts/checklists для rehearsal-импорта framework и consumer skeleton'ов."
    examples = (
        "dltaf dev export-import-rehearsal --out-dir build/repo-split-rehearsal",
        "dltaf dev export-import-rehearsal --out-dir build/repo-split-rehearsal --clean",
    )

    def register(self, subparsers: argparse._SubParsersAction) -> argparse.ArgumentParser:
        p = subparsers.add_parser(self.name, help=self.help, description=self.description)
        p.add_argument("--out-dir", required=True, help="Output directory for rehearsal import bundle.")
        p.add_argument("--clean", action="store_true", help="Delete output directory before export.")
        p.set_defaults(_command=self)
        return ensure_parser(p)

    def run(self, args: argparse.Namespace, ctx) -> int:
        svc = RepoSplitService(ctx=ctx, logger=ctx.logger)
        return svc.export_import_rehearsal(args)
