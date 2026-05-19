from __future__ import annotations

import argparse

from dltaf.commands.base import Command, ensure_parser
from dltaf.services.dags.generation import DAGGenerationService, add_dag_generation_arguments


class DagsGenerateCommand(Command):
    name = "generate"
    help = "Генерация Airflow DAG wrappers из manifests."
    description = "Native Wave 8 implementation backed by dltaf.services.dags.generation"
    examples = (
        "dltaf dags generate --manifests-dir dlt_pipelines/manifests --output-dir dags",
        "dltaf dags generate --clean",
    )
    legacy_entrypoint = "dlt-generate-dags"

    def register(self, subparsers: argparse._SubParsersAction) -> argparse.ArgumentParser:
        parser = subparsers.add_parser(
            self.name,
            help=self.help,
            description=self.description,
            formatter_class=argparse.RawDescriptionHelpFormatter,
        )
        add_dag_generation_arguments(parser)
        parser.set_defaults(_command=self)
        return ensure_parser(parser)

    def run(self, args: argparse.Namespace, ctx) -> int:
        return DAGGenerationService(ctx=ctx, logger=ctx.logger).run(args)
