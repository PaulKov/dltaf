#!/usr/bin/env python3
"""Legacy compatibility wrapper for lineage CLI."""

from __future__ import annotations

from dltaf.app.deprecation import warn_legacy_entrypoint

import logging
from typing import Optional, Sequence

from dltaf.app.context import AppContext
from dltaf.services.lineage.reporting import LineageReportService, add_lineage_arguments


def build_parser(prog: str = "dlt-show-lineage"):
    import argparse

    parser = argparse.ArgumentParser(
        description="Визуализация lineage и зависимостей dlt пайплайнов",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        prog=prog,
    )
    add_lineage_arguments(parser)
    return parser


def main(argv: Optional[Sequence[str]] = None) -> int:
    warn_legacy_entrypoint("dlt-show-lineage")
    parser = build_parser()
    args = parser.parse_args(argv)
    ctx = AppContext.build(repo_root=".", logger=logging.getLogger("dlt.lineage"))
    return LineageReportService(ctx=ctx, logger=ctx.logger).run(args)


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
