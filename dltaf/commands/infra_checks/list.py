from __future__ import annotations

import argparse

from dltaf.commands.base import Command, ensure_parser
from dltaf.services.infra_checks import InfraChecksCatalogService, add_infra_checks_list_arguments


class InfraChecksListCommand(Command):
    name = "list"
    help = "Показ доступных онлайн-проверок инфраструктуры."
    description = "Native Wave 7 implementation backed by dltaf.services.infra_checks.catalog"
    legacy_entrypoint = "dlt-infra-checks list"
    examples = (
        "dltaf infra-checks list --manifest dlt_pipelines/manifests/dlt__uploader__to__clickhouse__b057.yaml",
    )

    def register(self, subparsers: argparse._SubParsersAction) -> argparse.ArgumentParser:
        parser = subparsers.add_parser(self.name, help=self.help, description=self.description)
        add_infra_checks_list_arguments(parser)
        parser.set_defaults(_command=self)
        return ensure_parser(parser)

    def run(self, args: argparse.Namespace, ctx) -> int:
        return InfraChecksCatalogService(logger=ctx.logger).run(args)
