from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, List, Sequence

from dltaf.services.execution.redaction_service import redact_text
from dltaf.services.manifests.loader import load_manifest
from dltaf.services.manifests.validator import validate_manifest


@dataclass(frozen=True)
class ManifestLintItem:
    path: Path
    ok: bool
    message: str | None = None


@dataclass(frozen=True)
class ManifestLintReport:
    items: Sequence[ManifestLintItem]

    @property
    def failed(self) -> int:
        return sum(1 for item in self.items if not item.ok)

    @property
    def passed(self) -> int:
        return sum(1 for item in self.items if item.ok)


def add_manifest_lint_arguments(parser) -> None:
    parser.add_argument(
        "--manifests-dir",
        default="dlt_pipelines/manifests",
        help="Directory with manifests (*.yaml)",
    )
    parser.add_argument(
        "--manifest",
        action="append",
        default=None,
        help="Path to a single manifest. Can be specified multiple times.",
    )
    parser.add_argument("--fail-fast", action="store_true", help="Stop on first error.")
    parser.add_argument(
        "--lenient-source",
        action="store_true",
        help=(
            "Do not enforce strict per-kind validation for source keys. "
            "By default the linter forbids unknown keys for known source kinds."
        ),
    )
    parser.add_argument(
        "--allow-filename-mismatch",
        action="store_true",
        help=(
            "Skip the pipeline.name == manifest filename check. "
            "Useful for shipped smoke examples where the file name is intentionally human-oriented."
        ),
    )


class ManifestLintService:
    def __init__(self, *, logger) -> None:
        self.logger = logger

    def discover_yaml_files(self, base: Path) -> List[Path]:
        return sorted([path for path in base.glob("*.yaml") if path.is_file()])

    def collect_manifest_paths(self, *, manifests_dir: str, manifests: Sequence[str] | None) -> List[Path]:
        if manifests:
            return [Path(item).expanduser().resolve() for item in manifests]
        base = Path(str(manifests_dir)).expanduser().resolve()
        if not base.exists():
            raise SystemExit(f"Manifests directory not found: {base}")
        selected = self.discover_yaml_files(base)
        if not selected:
            raise SystemExit("No manifests found")
        return selected

    def lint_paths(
        self,
        paths: Iterable[Path],
        *,
        strict_source: bool,
        enforce_filename_match: bool,
    ) -> ManifestLintReport:
        items: List[ManifestLintItem] = []
        for path in paths:
            try:
                manifest = load_manifest(path)
                validate_manifest(
                    manifest,
                    strict_source=bool(strict_source),
                    enforce_filename_match=bool(enforce_filename_match),
                )
                items.append(ManifestLintItem(path=path, ok=True))
            except Exception as exc:
                items.append(ManifestLintItem(path=path, ok=False, message=redact_text(str(exc))))
        return ManifestLintReport(items=tuple(items))

    def emit_report(self, report: ManifestLintReport) -> int:
        for item in report.items:
            if item.ok:
                print(f"OK   {item.path.name}")
            else:
                print(f"FAIL {item.path.name}: {item.message or 'validation_failed'}")
        return 1 if report.failed else 0

    def run(self, args) -> int:
        paths = self.collect_manifest_paths(
            manifests_dir=str(args.manifests_dir),
            manifests=list(args.manifest or []),
        )
        strict_source = not bool(args.lenient_source)
        enforce_filename_match = not bool(args.allow_filename_mismatch)
        if bool(args.fail_fast):
            for path in paths:
                report = self.lint_paths(
                    [path],
                    strict_source=strict_source,
                    enforce_filename_match=enforce_filename_match,
                )
                code = self.emit_report(report)
                if code != 0:
                    return code
            return 0
        report = self.lint_paths(
            paths,
            strict_source=strict_source,
            enforce_filename_match=enforce_filename_match,
        )
        return self.emit_report(report)


__all__ = [
    "ManifestLintItem",
    "ManifestLintReport",
    "ManifestLintService",
    "add_manifest_lint_arguments",
]
