from __future__ import annotations

import argparse

from ...services.docs.generate_cli_reference_service import GenerateCliReferenceService
from ..base import Command, ensure_parser


class GenerateCliReferenceCommand(Command):
    name = "generate-cli-reference"
    help = "Генерация `docs/CLI_REFERENCE.md` из umbrella CLI registry."
    description = "Generate or check the umbrella CLI reference document."
    examples = (
        "dltaf docs generate-cli-reference",
        "dltaf docs generate-cli-reference --check --show-diff",
    )
    legacy_entrypoint = None

    def register(self, subparsers: argparse._SubParsersAction) -> argparse.ArgumentParser:
        p = subparsers.add_parser(
            self.name,
            help=self.help,
            description=self.description,
        )
        p.add_argument("--docs-dir", default="docs", help="Docs directory (default: docs).")
        p.add_argument("--check", action="store_true", help="Only check whether the file is up to date.")
        p.add_argument("--show-diff", action="store_true", help="Show a unified diff when --check fails.")
        p.add_argument("--diff-lines", type=int, default=200, help="Maximum diff lines to print.")
        p.set_defaults(_command=self)
        return ensure_parser(p)

    def run(self, args: argparse.Namespace, ctx) -> int:
        svc = GenerateCliReferenceService(ctx=ctx, logger=ctx.logger)
        return svc.run(args)
