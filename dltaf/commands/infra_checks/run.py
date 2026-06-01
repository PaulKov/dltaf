from __future__ import annotations

import argparse

from dltaf.commands.base import Command, ensure_parser
from dltaf.services.infra_checks import InfraChecksRunService, add_infra_checks_run_arguments


class InfraChecksRunCommand(Command):
    name = "run"
    help = "Запуск выбранных онлайн-проверок инфраструктуры."
    description = "Native Wave 7 implementation backed by dltaf.services.infra_checks.runner"
    legacy_entrypoint = "dlt-infra-checks run"
    examples = (
        "dltaf infra-checks run --manifest dlt_pipelines/manifests/dlt__uploader__to__clickhouse__b057.yaml --format yaml",
    )

    def register(self, subparsers: argparse._SubParsersAction) -> argparse.ArgumentParser:
        parser = subparsers.add_parser(self.name, help=self.help, description=self.description)
        add_infra_checks_run_arguments(parser)
        parser.set_defaults(_command=self)
        return ensure_parser(parser)

    def run(self, args: argparse.Namespace, ctx) -> int:
        return InfraChecksRunService(logger=ctx.logger).run(args)
