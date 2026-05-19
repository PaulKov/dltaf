"""Legacy CLI wrapper for infrastructure online checks.

Wave 7 keeps the historical entrypoint `dlt-infra-checks`, but the
implementation now lives in `dltaf.services.infra_checks.*`.
"""

from __future__ import annotations

from dltaf.app.deprecation import warn_legacy_entrypoint

import argparse
import sys
from typing import List, Optional

from dltaf.app.context import AppContext
from dltaf.services.infra_checks import (
    InfraChecksCatalogService,
    InfraChecksRunService,
    add_infra_checks_list_arguments,
    add_infra_checks_run_arguments,
)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="dlt-infra-checks", description="List/run infra online checks")
    subparsers = parser.add_subparsers(dest="command")

    parser_list = subparsers.add_parser("list", help="List available infra checks")
    add_infra_checks_list_arguments(parser_list)
    parser_list.set_defaults(func=cmd_list)

    parser_run = subparsers.add_parser("run", help="Run infra checks for a manifest")
    add_infra_checks_run_arguments(parser_run)
    parser_run.set_defaults(func=cmd_run)
    return parser


def cmd_list(args) -> int:
    ctx = AppContext.build(repo_root=".")
    return InfraChecksCatalogService(logger=ctx.logger).run(args)


def cmd_run(args) -> int:
    ctx = AppContext.build(repo_root=".")
    return InfraChecksRunService(logger=ctx.logger).run(args)


def main(argv: Optional[List[str]] = None) -> None:
    warn_legacy_entrypoint("dlt-infra-checks")
    parser = build_parser()
    argv2 = list(argv) if argv is not None else sys.argv[1:]
    if not argv2:
        argv2 = ["list"]
    elif argv2[0] not in {"list", "run", "-h", "--help"}:
        argv2 = ["list", *argv2]
    args = parser.parse_args(argv2)
    func = getattr(args, "func", None)
    if not callable(func):
        parser.print_help()
        raise SystemExit(2)
    raise SystemExit(int(func(args) or 0))


if __name__ == "__main__":
    main()
