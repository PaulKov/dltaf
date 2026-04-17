"""Legacy CLI wrapper for payload contract tests.

Wave 7 keeps the historical entrypoint `dlt-contract-test`, but the
implementation now lives in `dltaf.services.contracts.testing`.
"""

from __future__ import annotations

from dltaf.app.deprecation import warn_legacy_entrypoint

import argparse
from typing import Optional, Sequence

from dltaf.app.context import AppContext
from dltaf.services.contracts import ContractTestService, add_contract_test_arguments
from dltaf.services.execution.redaction_service import redact_text


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="dlt-contract-test", description="Validate payload contracts + samples")
    add_contract_test_arguments(parser)
    return parser


def main(argv: Optional[Sequence[str]] = None) -> None:
    warn_legacy_entrypoint("dlt-contract-test")
    args = build_parser().parse_args(argv)
    ctx = AppContext.build(repo_root=".")
    try:
        code = ContractTestService(logger=ctx.logger).run(args)
    except Exception as exc:
        print(f"ERROR: {redact_text(str(exc))}")
        raise SystemExit(2)
    raise SystemExit(code)


if __name__ == "__main__":
    main()
