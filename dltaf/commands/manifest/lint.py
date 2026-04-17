from __future__ import annotations

import argparse

from dltaf.commands.base import Command, ensure_parser
from dltaf.services.manifests.lint import ManifestLintService, add_manifest_lint_arguments


class ManifestLintCommand(Command):
    name = "lint"
    help = "Проверка manifests по schema и runtime rules."
    description = "Native Wave 7 implementation backed by dltaf.services.manifests.lint"
    examples = (
        "dltaf manifest lint --manifests-dir dlt_pipelines/manifests",
        "dltaf manifest lint --manifest dlt_pipelines/manifests/dlt__x.yaml",
    )
    legacy_entrypoint = "dlt-manifest-lint"

    def register(self, subparsers: argparse._SubParsersAction) -> argparse.ArgumentParser:
        parser = subparsers.add_parser(
            self.name,
            help=self.help,
            description=self.description,
            formatter_class=argparse.RawDescriptionHelpFormatter,
        )
        add_manifest_lint_arguments(parser)
        parser.set_defaults(_command=self)
        return ensure_parser(parser)

    def run(self, args: argparse.Namespace, ctx) -> int:
        return ManifestLintService(logger=ctx.logger).run(args)
