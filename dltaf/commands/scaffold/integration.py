from __future__ import annotations

import argparse

from ...services.scaffold import ALLOWED_TEMPLATES, ScaffoldIntegrationService
from ..base import Command, ensure_parser


class ScaffoldIntegrationCommand(Command):
    name = "integration"
    help = "Генерация каркаса новой интеграции и manifest."
    description = "Native Wave 6 implementation backed by dltaf.services.scaffold.*"
    examples = (
        "dltaf scaffold integration --pipeline-name dlt__myapi__to__clickhouse__raw --source-kind myapi_raw --template http_pull",
    )
    legacy_entrypoint = "dlt-scaffold-integration"

    def register(self, subparsers: argparse._SubParsersAction) -> argparse.ArgumentParser:
        parser = subparsers.add_parser(self.name, help=self.help, description=self.description)
        parser.add_argument("--pipeline-name", required=True, help="Pipeline name (also manifest filename, without .yaml)")
        parser.add_argument("--source-kind", required=True, help="source.kind value for the integration")
        parser.add_argument("--package", default=None, help="Python package name under dlt_pipelines/ (default: <source_kind>__pipeline)")
        parser.add_argument("--template", default="api_kafka_json", choices=sorted(ALLOWED_TEMPLATES), help="Scaffold template")
        parser.add_argument("--destination", default="clickhouse", help="Default pipeline.destination")
        parser.add_argument("--dataset", default="raw", help="Default pipeline.dataset")
        parser.add_argument("--schedule", default="0 6 * * *", help="Default Airflow schedule cron")
        parser.add_argument("--root-dir", default=".", help="Repository root (default: current directory)")
        parser.add_argument("--no-update-pyproject", action="store_true", help="Do not update pyproject.toml [tool.setuptools].packages")
        parser.add_argument("--force", action="store_true", help="Overwrite existing files")
        parser.set_defaults(_command=self)
        return ensure_parser(parser)

    def run(self, args: argparse.Namespace, ctx) -> int:
        return ScaffoldIntegrationService(logger=ctx.logger).run(args)
