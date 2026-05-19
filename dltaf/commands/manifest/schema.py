from __future__ import annotations

import argparse

from ...services.manifests.schema import ManifestSchemaService
from ..base import Command, ensure_parser


class ManifestSchemaCommand(Command):
    name = "schema"
    help = "Генерация JSON Schema для manifests."
    description = "Native Wave 6 implementation backed by dltaf.services.manifests.schema.*"
    examples = (
        "dltaf manifest schema --out docs/manifest.schema.json",
        "dltaf manifest schema --lenient-source",
    )
    legacy_entrypoint = "dlt-manifest-schema"

    def register(self, subparsers: argparse._SubParsersAction) -> argparse.ArgumentParser:
        parser = subparsers.add_parser(self.name, help=self.help, description=self.description)
        parser.add_argument("--out", default=None, help="Output path. If not specified, schema is printed to stdout.")
        parser.add_argument("--lenient", action="store_true", help="Generate lenient schema for common sections.")
        parser.add_argument("--lenient-source", action="store_true", help="Allow extra keys in known source kinds.")
        parser.set_defaults(_command=self)
        return ensure_parser(parser)

    def run(self, args: argparse.Namespace, ctx) -> int:
        svc = ManifestSchemaService(logger=ctx.logger)
        return svc.write_or_print(
            out=args.out,
            strict_common=not bool(args.lenient),
            strict_source=not bool(args.lenient_source),
        )
