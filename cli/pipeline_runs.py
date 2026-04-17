from __future__ import annotations

from dltaf.app.deprecation import warn_legacy_entrypoint

import argparse
import logging
from typing import Optional, Sequence

from dltaf.services.runs.audit_listing import AuditRunsService, add_runs_list_arguments


def build_parser(prog: str = "dlt-pipeline-runs") -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Show recent pipeline runs from ClickHouse audit table", prog=prog)
    add_runs_list_arguments(parser)
    return parser


def main(argv: Optional[Sequence[str]] = None) -> int:  # pragma: no cover
    warn_legacy_entrypoint("dlt-pipeline-runs")
    parser = build_parser()
    args = parser.parse_args(argv)
    return AuditRunsService(logger=logging.getLogger("dlt.pipeline_runs")).run(args)


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
