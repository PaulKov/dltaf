from __future__ import annotations

import argparse

from dltaf.commands.base import Command, ensure_parser
from dltaf.services.runs.unit_audit_listing import AuditRunUnitsService, add_runs_units_arguments


class RunsUnitsCommand(Command):
    name = "units"
    help = "Показ unit-level audit-истории запусков pipeline."
    description = "Просмотр детальной истории unit-level observability из ClickHouse."
    examples = (
        "dltaf runs units --pipeline dlt__pkb_conclusion__to__clickhouse__adata --limit 100",
        "dltaf runs units --run-id 123e4567-e89b-12d3-a456-426614174000",
    )

    def register(self, subparsers: argparse._SubParsersAction) -> argparse.ArgumentParser:
        parser = subparsers.add_parser(
            self.name,
            help=self.help,
            description=self.description,
            formatter_class=argparse.RawDescriptionHelpFormatter,
        )
        add_runs_units_arguments(parser)
        parser.set_defaults(_command=self)
        return ensure_parser(parser)

    def run(self, args: argparse.Namespace, ctx) -> int:
        return AuditRunUnitsService(logger=ctx.logger).run(args)

