from __future__ import annotations

import argparse

from ...services.dev.package_artifacts import DevPackageService
from ..base import Command, ensure_parser


class DevSmokeInstalledPackageCommand(Command):
    name = "smoke-installed-package"
    help = "Установка опубликованной dev package version и запуск smoke-проверок."
    description = "Создание чистой venv, установка опубликованной dev package version из package registry и запуск package smoke."
    examples = (
        "dltaf dev smoke-installed-package --version 0.1.0.dev123+gabc1234.main --simple-index-url https://gitlab.example/api/v4/projects/1/packages/pypi/simple",
    )

    def register(self, subparsers: argparse._SubParsersAction) -> argparse.ArgumentParser:
        p = subparsers.add_parser(self.name, help=self.help, description=self.description)
        p.add_argument("--package-name", default="", help="Имя пакета (по умолчанию: dltaf).")
        p.add_argument("--version", default="", help="Published package version to install.")
        p.add_argument("--simple-index-url", default="", help="Simple package index URL.")
        p.add_argument("--username", default="", help="Package registry username.")
        p.add_argument("--password", default="", help="Package registry password/token.")
        p.set_defaults(_command=self)
        return ensure_parser(p)

    def run(self, args: argparse.Namespace, ctx) -> int:
        svc = DevPackageService(ctx=ctx, logger=ctx.logger)
        return svc.smoke_installed(args)
