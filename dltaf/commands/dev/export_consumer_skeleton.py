from __future__ import annotations

import argparse

from ...services.dev.repo_split import RepoSplitService
from ..base import Command, ensure_parser


class DevExportConsumerSkeletonCommand(Command):
    name = "export-consumer-skeleton"
    help = "Собрать файловый skeleton для будущего consumer repo `dltaf-airflow`."
    description = "Копирует consumer-classified файлы в отдельную директорию и пишет inventory-отчёты."
    examples = (
        "dltaf dev export-consumer-skeleton --out-dir build/dltaf-airflow-skeleton",
        "dltaf dev export-consumer-skeleton --out-dir build/dltaf-airflow-skeleton --clean",
    )

    def register(self, subparsers: argparse._SubParsersAction) -> argparse.ArgumentParser:
        p = subparsers.add_parser(self.name, help=self.help, description=self.description)
        p.add_argument("--out-dir", required=True, help="Output directory for exported consumer skeleton.")
        p.add_argument("--clean", action="store_true", help="Delete output directory before export.")
        p.set_defaults(_command=self)
        return ensure_parser(p)

    def run(self, args: argparse.Namespace, ctx) -> int:
        svc = RepoSplitService(ctx=ctx, logger=ctx.logger)
        return svc.export_consumer_skeleton(args)
