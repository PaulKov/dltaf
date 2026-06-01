from __future__ import annotations

import argparse

from ...services.dev.repo_split import RepoSplitService
from ..base import Command, ensure_parser


class DevRepoSplitInventoryCommand(Command):
    name = "repo-split-inventory"
    help = "Построить inventory файлов для разделения на `dltaf` и `dltaf-airflow`."
    description = "Классифицирует файлы mixed repo на framework/consumer/unresolved."
    examples = (
        "dltaf dev repo-split-inventory --format md",
        "dltaf dev repo-split-inventory --format json --out build/repo-split-inventory.json",
    )

    def register(self, subparsers: argparse._SubParsersAction) -> argparse.ArgumentParser:
        p = subparsers.add_parser(self.name, help=self.help, description=self.description)
        p.add_argument("--format", choices=("md", "json"), default="md", help="Output format.")
        p.add_argument("--out", default="", help="Optional output file. Defaults to stdout.")
        p.set_defaults(_command=self)
        return ensure_parser(p)

    def run(self, args: argparse.Namespace, ctx) -> int:
        svc = RepoSplitService(ctx=ctx, logger=ctx.logger)
        return svc.inventory(args)
