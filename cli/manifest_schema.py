"""Compatibility CLI for manifest schema generation.

Wave 6 / Stage 39
-----------------
The implementation now lives in `dltaf.services.manifests.schema`.
"""

from __future__ import annotations

from dltaf.app.deprecation import warn_legacy_entrypoint

from typing import Optional, Sequence

from dltaf.services.manifests.schema import ManifestSchemaService


def main(argv: Optional[Sequence[str]] = None) -> None:  # pragma: no cover
    warn_legacy_entrypoint("dlt-manifest-schema")
    import argparse
    import logging

    parser = argparse.ArgumentParser(description="Generate JSON Schema for manifest YAML")
    parser.add_argument("--out", default=None, help="Output path. If not specified, schema is printed to stdout.")
    parser.add_argument("--lenient", action="store_true", help="Generate lenient schema (allows extra keys in common sections).")
    parser.add_argument("--lenient-source", action="store_true", help="Generate schema where `source:` is permissive for known kinds.")
    args = parser.parse_args(argv)

    svc = ManifestSchemaService(logger=logging.getLogger("dlt.manifest_schema"))
    svc.write_or_print(
        out=args.out,
        strict_common=not bool(args.lenient),
        strict_source=not bool(args.lenient_source),
    )


if __name__ == "__main__":  # pragma: no cover
    main()
