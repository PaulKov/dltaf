from __future__ import annotations

import argparse

from ...services.dev.handoff import CutoverHandoffService, add_cutover_handoff_arguments
from ..base import Command, ensure_parser


class DevExportCutoverHandoffCommand(Command):
    name = 'export-cutover-handoff'
    help = 'Собрать финальный handoff bundle для физического split на `dltaf` и `dltaf-airflow`.'
    description = 'Экспортирует rehearsal/dry-run bundle и дополняет его финальными handoff-документами, runbook и sign-off checklist.'
    examples = (
        'dltaf dev export-cutover-handoff --out-dir build/repo-split-handoff',
        'dltaf dev export-cutover-handoff --out-dir build/repo-split-handoff --clean',
    )

    def register(self, subparsers: argparse._SubParsersAction) -> argparse.ArgumentParser:
        p = subparsers.add_parser(self.name, help=self.help, description=self.description)
        add_cutover_handoff_arguments(p)
        p.set_defaults(_command=self)
        return ensure_parser(p)

    def run(self, args: argparse.Namespace, ctx) -> int:
        return CutoverHandoffService(ctx=ctx, logger=ctx.logger).run(args)
