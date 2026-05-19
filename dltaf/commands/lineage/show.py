from __future__ import annotations

import argparse

from dltaf.commands.base import Command, ensure_parser
from dltaf.services.lineage.reporting import LineageReportService, add_lineage_arguments


class LineageShowCommand(Command):
    name = "show"
    help = "Показ lineage и зависимостей pipeline."
    description = "Native Wave 8 implementation backed by dltaf.services.lineage.reporting"
    examples = (
        "dltaf lineage show --format text",
        "dltaf lineage show --format mermaid",
    )
    legacy_entrypoint = "dlt-show-lineage"

    def register(self, subparsers: argparse._SubParsersAction) -> argparse.ArgumentParser:
        parser = subparsers.add_parser(
            self.name,
            help=self.help,
            description=self.description,
            formatter_class=argparse.RawDescriptionHelpFormatter,
        )
        add_lineage_arguments(parser)
        parser.set_defaults(_command=self)
        return ensure_parser(parser)

    def run(self, args: argparse.Namespace, ctx) -> int:
        return LineageReportService(ctx=ctx, logger=ctx.logger).run(args)
