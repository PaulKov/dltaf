from __future__ import annotations

import argparse

from ...services.manifests.doctor import ManifestDoctorService
from ..base import Command, ensure_parser


class ManifestDoctorCommand(Command):
    name = "doctor"
    help = "Analyze manifests, suggest fixes, and generate public-safe templates."
    description = "Manifest doctor for canonical SQLDB manifests, MongoDB examples, and compatibility cleanup."
    examples = (
        "dltaf manifest doctor --manifests-dir dlt_pipelines/manifests",
        "dltaf manifest doctor --manifests-dir dlt_pipelines/manifests --apply",
        "dltaf manifest doctor --template-kind sqldb_catalog --pipeline-name dlt__postgres__to__clickhouse__raw --out dlt_pipelines/manifests/dlt__postgres__to__clickhouse__raw.yaml",
        "dltaf manifest doctor --template-kind sqldb_query --pipeline-name dlt__oracle__to__clickhouse__raw --out dlt_pipelines/manifests/dlt__oracle__to__clickhouse__raw.yaml",
        "dltaf manifest doctor --template-kind mongodb --pipeline-name dlt__mongo__to__clickhouse__raw --out dlt_pipelines/manifests/dlt__mongo__to__clickhouse__raw.yaml",
    )
    legacy_entrypoint = "dlt-manifest-doctor"

    def register(self, subparsers: argparse._SubParsersAction) -> argparse.ArgumentParser:
        parser = subparsers.add_parser(self.name, help=self.help, description=self.description)
        parser.add_argument("--manifests-dir", default="dlt_pipelines/manifests", help="Directory with manifests (*.yaml)")
        parser.add_argument("--manifest", action="append", default=None, help="Path to a single manifest. Can be specified multiple times.")
        parser.add_argument("--apply", action="store_true", help="Apply safe auto-fixes in-place (preserves comments).")
        parser.add_argument("--check", action="store_true", help="Exit with code=1 if any issues were found.")
        parser.add_argument("--fail-fast", action="store_true", help="Stop on first error.")
        parser.add_argument("--template-kind", default=None, help="Generate a template manifest (supported: sqldb_catalog, sqldb_query, mongodb, plus legacy SQL aliases).")
        parser.add_argument("--pipeline-name", default=None, help="Pipeline name for template generation.")
        parser.add_argument("--destination", default="clickhouse", help="pipeline.destination for template generation.")
        parser.add_argument("--dataset", default="raw", help="pipeline.dataset for template generation.")
        parser.add_argument("--out", default=None, help="Output file for template generation (if not set: print to stdout).")
        parser.set_defaults(_command=self)
        return ensure_parser(parser)

    def run(self, args: argparse.Namespace, ctx) -> int:
        return ManifestDoctorService(logger=ctx.logger).run(args)
