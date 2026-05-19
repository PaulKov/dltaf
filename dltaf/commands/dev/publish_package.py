from __future__ import annotations

import argparse

from ...services.dev.package_artifacts import DevPackageService
from ..base import Command, ensure_parser


class DevPublishPackageCommand(Command):
    name = "publish-package"
    help = "Публикация dev package artifact в package registry."
    description = "Публикация ранее собранных dev wheel/sdist artifacts в package registry для dev-проверки."
    examples = (
        "dltaf dev publish-package --dist-dir dist-dev",
        "dltaf dev publish-package --dist-dir dist-dev --repository-url https://gitlab.example/api/v4/projects/1/packages/pypi",
    )

    def register(self, subparsers: argparse._SubParsersAction) -> argparse.ArgumentParser:
        p = subparsers.add_parser(self.name, help=self.help, description=self.description)
        p.add_argument("--dist-dir", default="dist-dev", help="Directory containing built wheel/sdist files.")
        p.add_argument("--repository-url", default="", help="Repository upload URL (defaults to GitLab project PyPI URL in CI).")
        p.add_argument("--username", default="", help="Repository username (default: gitlab-ci-token in CI).")
        p.add_argument("--password", default="", help="Repository password/token (default: CI_JOB_TOKEN/TWINE_PASSWORD).")
        p.set_defaults(_command=self)
        return ensure_parser(p)

    def run(self, args: argparse.Namespace, ctx) -> int:
        svc = DevPackageService(ctx=ctx, logger=ctx.logger)
        return svc.publish(args)
