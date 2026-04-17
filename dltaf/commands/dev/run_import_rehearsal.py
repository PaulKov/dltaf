from __future__ import annotations

import argparse

from ...services.dev.rehearsal import ImportRehearsalService, add_import_rehearsal_arguments
from ..base import Command, ensure_parser


class DevRunImportRehearsalCommand(Command):
    name = "run-import-rehearsal"
    help = "Подготовить два временных repo и прогнать dry-run validation после split."
    description = "Экспортирует rehearsal bundle, готовит framework/consumer rehearsal repos и выполняет validation checks с отчётами."
    examples = (
        "dltaf dev run-import-rehearsal --out-dir build/repo-split-rehearsal-run --clean",
        "dltaf dev run-import-rehearsal --out-dir build/repo-split-rehearsal-run --skip-consumer-validation",
    )

    def register(self, subparsers: argparse._SubParsersAction) -> argparse.ArgumentParser:
        p = subparsers.add_parser(self.name, help=self.help, description=self.description)
        add_import_rehearsal_arguments(p)
        p.set_defaults(_command=self)
        return ensure_parser(p)

    def run(self, args: argparse.Namespace, ctx) -> int:
        return ImportRehearsalService(ctx=ctx, logger=ctx.logger).run(args)
