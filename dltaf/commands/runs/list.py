from __future__ import annotations

import argparse

from dltaf.commands.base import Command, ensure_parser
from dltaf.services.runs.audit_listing import AuditRunsService, add_runs_list_arguments


class RunsListCommand(Command):
    name = "list"
    help = "Показ audit-истории запусков pipeline из ClickHouse."
    description = "Native Wave 8 implementation backed by dltaf.services.runs.audit_listing"
    examples = (
        "dltaf runs list --pipeline dlt__uploader__to__clickhouse__b057 --limit 20",
    )
    legacy_entrypoint = "dlt-pipeline-runs"

    def register(self, subparsers: argparse._SubParsersAction) -> argparse.ArgumentParser:
        parser = subparsers.add_parser(
            self.name,
            help=self.help,
            description=self.description,
            formatter_class=argparse.RawDescriptionHelpFormatter,
        )
        add_runs_list_arguments(parser)
        parser.set_defaults(_command=self)
        return ensure_parser(parser)

    def run(self, args: argparse.Namespace, ctx) -> int:
        return AuditRunsService(logger=ctx.logger).run(args)
