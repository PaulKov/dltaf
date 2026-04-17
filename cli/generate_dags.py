#!/usr/bin/env python3
"""Legacy compatibility wrapper for DAG generation CLI."""

from __future__ import annotations

from dltaf.app.deprecation import warn_legacy_entrypoint

import logging
from typing import Optional, Sequence

from dltaf.app.context import AppContext
from dltaf.services.dags.generation import DAGGenerationService, add_dag_generation_arguments


def build_parser(prog: str = "dlt-generate-dags"):
    import argparse

    parser = argparse.ArgumentParser(description="Генерация Airflow DAG файлов из YAML манифестов", prog=prog)
    add_dag_generation_arguments(parser)
    return parser


def main(argv: Optional[Sequence[str]] = None) -> int:
    warn_legacy_entrypoint("dlt-generate-dags")
    parser = build_parser()
    args = parser.parse_args(argv)
    ctx = AppContext.build(repo_root=".", logger=logging.getLogger("dlt.generate_dags"))
    return DAGGenerationService(ctx=ctx, logger=ctx.logger).run(args)


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
