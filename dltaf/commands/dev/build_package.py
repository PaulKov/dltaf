from __future__ import annotations

import argparse

from ...services.dev.package_artifacts import DevPackageService
from ..base import Command, ensure_parser


class DevBuildPackageCommand(Command):
    name = "build-package"
    help = "Сборка dev package artifact по commit SHA без release tag."
    description = "Сборка dev wheel/sdist с commit-based версией и рендер dev deploy bundle."
    examples = (
        "dltaf dev build-package --dist-dir dist-dev",
        "dltaf dev build-package --dist-dir dist-dev --bundle-dir dist-dev/dev_bundle",
    )

    def register(self, subparsers: argparse._SubParsersAction) -> argparse.ArgumentParser:
        p = subparsers.add_parser(self.name, help=self.help, description=self.description)
        p.add_argument("--dist-dir", default="dist-dev", help="Output directory for built artifacts.")
        p.add_argument("--version", default="", help="Override computed dev version.")
        p.add_argument("--env-file", default="", help="Optional path for generated .env metadata.")
        p.add_argument("--json-file", default="", help="Optional path for generated JSON metadata.")
        p.add_argument("--bundle-dir", default="", help="Optional path for rendered dev deploy bundle.")
        p.set_defaults(_command=self)
        return ensure_parser(p)

    def run(self, args: argparse.Namespace, ctx) -> int:
        svc = DevPackageService(ctx=ctx, logger=ctx.logger)
        return svc.build(args)
