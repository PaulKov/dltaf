from __future__ import annotations

import argparse

from dltaf.commands.base import Command, ensure_parser
from dltaf.services.contracts import ContractTestService, add_contract_test_arguments
from dltaf.services.execution.redaction_service import redact_text


class ContractTestCommand(Command):
    name = "test"
    help = "Запуск payload contract tests по manifests/samples."
    description = "Native Wave 7 implementation backed by dltaf.services.contracts.testing"
    legacy_entrypoint = "dlt-contract-test"
    examples = (
        "dltaf contracts test --manifests-dir dlt_pipelines/manifests --format json",
    )

    def register(self, subparsers: argparse._SubParsersAction) -> argparse.ArgumentParser:
        parser = subparsers.add_parser(
            self.name,
            help=self.help,
            description=self.description,
        )
        add_contract_test_arguments(parser)
        parser.set_defaults(_command=self)
        return ensure_parser(parser)

    def run(self, args: argparse.Namespace, ctx) -> int:
        try:
            return ContractTestService(logger=ctx.logger).run(args)
        except Exception as exc:
            print(f"ERROR: {redact_text(str(exc))}")
            return 2
