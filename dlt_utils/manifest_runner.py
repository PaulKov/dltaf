"""Public manifest runner API + CLI entrypoint.

Stage 12 refactor
-----------------
Implementation was moved into `dltaf.services.*` modules:
  - `services.manifests` (YAML loading + templating + validation + overrides)
  - `services.execution` (planning + dry-run + execution flow)

Why?
- Reduce coupling hotspots (keep this module thin)
- Make core pieces easier to test and reuse

Stage 14 additions
------------------
- `--plan` and `--dry-run` CLI flags

Stage 15 additions
------------------
- `--plan-output` to write the plan to a file (or '-' for stdout)
- `--plan-format` json|yaml
- `--dry-run-strict` to fail when dry-run checks report errors

Stage 16 additions
------------------
- `--dry-run-online` to add online connectivity checks (ClickHouse/Kafka)
- `--online-timeout-seconds` to control TCP/metadata timeouts for online checks
"""

from __future__ import annotations

from dltaf.app.deprecation import warn_legacy_entrypoint

from pathlib import Path
from typing import Optional, Sequence

from dltaf.services.execution.executor import run_manifest
from dltaf.services.manifests.loader import load_manifest
from dltaf.services.manifests.overrides import parse_set_args, specs_to_mapping
from dltaf.services.manifests.resolver import resolve_manifest
from dltaf.services.manifests.validator import validate_manifest

__all__ = [
    "load_manifest",
    "resolve_manifest",
    "validate_manifest",
    "run_manifest",
    "main",
]


def main(argv: Optional[Sequence[str]] = None) -> None:  # pragma: no cover
    warn_legacy_entrypoint("dlt-manifest-run")
    import argparse

    parser = argparse.ArgumentParser(description="Run dlt pipeline from YAML manifest")
    parser.add_argument("--manifest", required=False, help="Path to manifest YAML")
    parser.add_argument(
        "--validate-all",
        action="store_true",
        help="Validate all manifests in a directory and exit",
    )
    parser.add_argument(
        "--manifests-dir",
        default="dlt_pipelines/manifests",
        help="Directory with manifests (used with --validate-all)",
    )
    parser.add_argument("--write-disposition", dest="write_disposition", default=None)
    parser.add_argument("--validate-only", action="store_true")

    parser.add_argument(
        "--set",
        dest="set_values",
        action="append",
        default=[],
        help=(
            "Override manifest values (repeatable). "
            "Example: --set source.year=2024 --set source.period=\"FULL_YEAR\" "
            "Values are parsed as JSON when possible."
        ),
    )
    parser.add_argument(
        "--time-from",
        "--date-from",
        dest="time_from",
        default=None,
        help=(
            "Backfill time window start (ISO date/datetime). "
            "Shorthand for: --set source.time_window.start=..."
        ),
    )
    parser.add_argument(
        "--time-to",
        "--date-to",
        dest="time_to",
        default=None,
        help=(
            "Backfill time window end (ISO date/datetime). "
            "Shorthand for: --set source.time_window.end=..."
        ),
    )

    mode = parser.add_mutually_exclusive_group()
    mode.add_argument(
        "--plan",
        action="store_true",
        help="Print run plan and exit (no execution).",
    )
    mode.add_argument(
        "--dry-run",
        action="store_true",
        help="Perform dry-run checks, print plan and exit (no side effects).",
    )
    mode.add_argument(
        "--dry-run-online",
        action="store_true",
        help=(
            "Perform dry-run checks + online connectivity checks (ClickHouse/Kafka), "
            "print plan and exit (no side effects)."
        ),
    )

    parser.add_argument(
        "--plan-output",
        default=None,
        help="Write plan to a file (json/yaml). Use '-' for stdout. Requires --plan or --dry-run.",
    )
    parser.add_argument(
        "--plan-format",
        default=None,
        choices=["json", "yaml"],
        help="Plan output format (default: infer from --plan-output extension, else json).",
    )
    parser.add_argument(
        "--dry-run-strict",
        action="store_true",
        help="Fail the run if dry-run checks contain errors (requires --dry-run or --dry-run-online).",
    )

    parser.add_argument(
        "--online-timeout-seconds",
        type=float,
        default=None,
        help=(
            "Timeout (seconds) for online connectivity checks (TCP + metadata). "
            "Used only with --dry-run-online."
        ),
    )

    parser.add_argument(
        "--explain-config",
        action="store_true",
        help="Print where config/env values were resolved from (Vault/Variables/extra_env).",
    )
    args = parser.parse_args(argv)

    if args.validate_all:
        from dltaf.services.execution.redaction import redact_text

        base = Path(args.manifests_dir).expanduser().resolve()
        manifests = sorted(base.glob("*.yaml"))
        if not manifests:
            raise SystemExit(f"No manifests found in: {base}")
        failed = 0
        for m in manifests:
            try:
                manifest = load_manifest(m)
                validate_manifest(manifest)
                print(f"OK  {m.name}")
            except Exception as e:
                failed += 1
                print(f"FAIL {m.name}: {redact_text(str(e))}")
        if failed:
            raise SystemExit(1)
        raise SystemExit(0)

    if not args.manifest:
        raise SystemExit("--manifest is required unless --validate-all is specified")

    if args.dry_run_strict and not (args.dry_run or args.dry_run_online):
        raise SystemExit("--dry-run-strict requires --dry-run or --dry-run-online")
    if (args.plan_output or args.plan_format) and not (args.plan or args.dry_run or args.dry_run_online):
        raise SystemExit("--plan-output/--plan-format require --plan or --dry-run or --dry-run-online")
    if args.online_timeout_seconds is not None and not args.dry_run_online:
        # Avoid silent confusion.
        raise SystemExit("--online-timeout-seconds is only valid with --dry-run-online")

    override_specs = parse_set_args(list(args.set_values or []))
    # Dedicated time window flags have priority over generic `--set`.
    if args.time_from is not None:
        override_specs.append(
            parse_set_args([f"source.time_window.start={args.time_from}"])[0]
        )
    if args.time_to is not None:
        override_specs.append(parse_set_args([f"source.time_window.end={args.time_to}"])[0])

    overrides = specs_to_mapping(override_specs)

    run_manifest(
        args.manifest,
        write_disposition=args.write_disposition,
        overrides=overrides,
        validate_only=args.validate_only,
        explain_config=args.explain_config,
        plan=args.plan,
        dry_run=args.dry_run,
        dry_run_online=args.dry_run_online,
        dry_run_strict=args.dry_run_strict,
        online_timeout_seconds=float(args.online_timeout_seconds or 5.0),
        plan_output=args.plan_output,
        plan_format=args.plan_format,
    )


if __name__ == "__main__":  # pragma: no cover
    main()
