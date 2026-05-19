"""Legacy CLI wrapper for manifest lint.

Wave 7 keeps the historical entrypoint `dlt-manifest-lint`, but the
implementation now lives in `dltaf.services.manifests.lint`.
"""

from __future__ import annotations

from dltaf.app.deprecation import warn_legacy_entrypoint

import argparse
from typing import Optional, Sequence

from dltaf.app.context import AppContext
from dltaf.services.manifests.lint import ManifestLintService, add_manifest_lint_arguments


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Lint dp-dlt-af manifest YAML files")
    add_manifest_lint_arguments(parser)
    return parser


def main(argv: Optional[Sequence[str]] = None) -> None:  # pragma: no cover
    warn_legacy_entrypoint("dlt-manifest-lint")
    args = build_parser().parse_args(argv)
    ctx = AppContext.build(repo_root=".")
    raise SystemExit(ManifestLintService(logger=ctx.logger).run(args))


if __name__ == "__main__":  # pragma: no cover
    main()
