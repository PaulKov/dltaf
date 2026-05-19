from __future__ import annotations

import argparse

from dltaf.commands.base import Command, ensure_parser
from dltaf.services.execution.executor import run_manifest
from dltaf.services.execution.redaction_service import safe_exception_message
from dltaf.services.manifests.overrides import parse_set_args, specs_to_mapping


class ManifestRunCommand(Command):
    name = "run"
    help = "Запуск manifest-driven pipeline."
    description = "Нативная реализация Stage 35 на базе `dltaf.services.*`."
    examples = (
        "dltaf manifest run --manifest dlt_pipelines/manifests/dlt__uploader__to__clickhouse__b057.yaml",
        "dltaf manifest run --manifest ... --plan",
        "dltaf manifest run --manifest ... --set source.year=2024 --time-from 2024-01-01 --time-to 2024-12-31",
    )
    legacy_entrypoint = "dlt-manifest-run"

    def register(self, subparsers: argparse._SubParsersAction) -> argparse.ArgumentParser:
        parser = subparsers.add_parser(
            self.name,
            help=self.help,
            description=self.description,
            formatter_class=argparse.RawDescriptionHelpFormatter,
        )
        parser.add_argument("--manifest", required=True, help="Путь к manifest YAML")
        parser.add_argument("--write-disposition", dest="write_disposition", default=None)
        parser.add_argument("--validate-only", action="store_true")
        parser.add_argument(
            "--set",
            dest="set_values",
            action="append",
            default=[],
            help=(
                "Override manifest values (repeatable). "
                "Example: --set source.year=2024 --set source.period=\"FULL_YEAR\". "
                "Values are parsed as JSON when possible."
            ),
        )
        parser.add_argument(
            "--time-from",
            "--date-from",
            dest="time_from",
            default=None,
            help="Shorthand for --set source.time_window.start=...",
        )
        parser.add_argument(
            "--time-to",
            "--date-to",
            dest="time_to",
            default=None,
            help="Shorthand for --set source.time_window.end=...",
        )
        mode = parser.add_mutually_exclusive_group()
        mode.add_argument("--plan", action="store_true", help="Print run plan and exit (no execution).")
        mode.add_argument(
            "--dry-run",
            action="store_true",
            help="Perform dry-run checks, print plan and exit (no side effects).",
        )
        mode.add_argument(
            "--dry-run-online",
            action="store_true",
            help="Perform dry-run checks + online connectivity checks, print plan and exit.",
        )
        parser.add_argument(
            "--plan-output",
            default=None,
            help="Write plan to a file (json/yaml). Use '-' for stdout.",
        )
        parser.add_argument(
            "--plan-format",
            default=None,
            choices=["json", "yaml"],
            help="Plan output format (default: infer from file extension, else json).",
        )
        parser.add_argument(
            "--dry-run-strict",
            action="store_true",
            help="Fail the run if dry-run checks contain errors.",
        )
        parser.add_argument(
            "--online-timeout-seconds",
            type=float,
            default=None,
            help="Timeout for online connectivity checks (used with --dry-run-online).",
        )
        parser.add_argument("--explain-config", action="store_true")
        parser.add_argument("--log-level", default="INFO")
        parser.set_defaults(_command=self)
        return ensure_parser(parser)

    def _build_overrides(self, args: argparse.Namespace) -> dict[str, object]:
        specs = parse_set_args(list(args.set_values or []))
        if args.time_from is not None:
            specs.extend(parse_set_args([f"source.time_window.start={args.time_from}"]))
        if args.time_to is not None:
            specs.extend(parse_set_args([f"source.time_window.end={args.time_to}"]))
        return specs_to_mapping(specs)

    def run(self, args: argparse.Namespace, ctx) -> int:
        if args.dry_run_strict and not (args.dry_run or args.dry_run_online):
            raise SystemExit("--dry-run-strict requires --dry-run or --dry-run-online")
        if (args.plan_output or args.plan_format) and not (args.plan or args.dry_run or args.dry_run_online):
            raise SystemExit("--plan-output/--plan-format require --plan or --dry-run or --dry-run-online")
        if args.online_timeout_seconds is not None and not args.dry_run_online:
            raise SystemExit("--online-timeout-seconds is only valid with --dry-run-online")

        overrides = self._build_overrides(args)
        try:
            run_manifest(
                args.manifest,
                write_disposition=args.write_disposition,
                overrides=overrides,
                validate_only=bool(args.validate_only),
                explain_config=bool(args.explain_config),
                plan=bool(args.plan),
                dry_run=bool(args.dry_run),
                dry_run_online=bool(args.dry_run_online),
                dry_run_strict=bool(args.dry_run_strict),
                online_timeout_seconds=float(args.online_timeout_seconds or 5.0),
                plan_output=args.plan_output,
                plan_format=args.plan_format,
                configure_logging=True,
                log_level=str(args.log_level or "INFO"),
            )
            return 0
        except SystemExit:
            raise
        except Exception as exc:
            print(safe_exception_message(exc))
            return 1
