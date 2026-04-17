from __future__ import annotations

import argparse
from pathlib import Path
from typing import List, Optional, Sequence

from .analysis import analyze_manifest
from .discovery import discover_yaml_files, load_yaml_mapping
from .fixes import apply_fixes_to_text
from .templates import render_template
from dltaf.integrations.sqldb import is_sqlish_source_kind, render_sqldb_canonical_manifest_yaml


class ManifestDoctorService:
    def __init__(self, *, logger) -> None:
        self.logger = logger

    @staticmethod
    def build_parser(prog: Optional[str] = None) -> argparse.ArgumentParser:
        parser = argparse.ArgumentParser(description="Manifest doctor", prog=prog)
        parser.add_argument("--manifests-dir", default="dlt_pipelines/manifests", help="Directory with manifests (*.yaml)")
        parser.add_argument("--manifest", action="append", default=None, help="Path to a single manifest. Can be specified multiple times.")
        parser.add_argument("--apply", action="store_true", help="Apply safe auto-fixes in-place (preserves comments).")
        parser.add_argument("--check", action="store_true", help="Exit with code=1 if any issues were found.")
        parser.add_argument("--fail-fast", action="store_true", help="Stop on first error.")
        parser.add_argument(
            "--template-kind",
            default=None,
            help="Generate a template manifest (supported: sqldb_catalog, sqldb_query, mongodb, plus legacy SQL aliases).",
        )
        parser.add_argument("--print-sqldb-canonical", action="store_true", help="For SQL-family manifests, print canonical sqldb YAML after analysis.")
        parser.add_argument("--write-sqldb-canonical-dir", default=None, help="Write canonical sqldb manifests for SQL-family manifests into the target directory (review output, original files are not modified).")
        parser.add_argument("--pipeline-name", default=None, help="Pipeline name for template generation.")
        parser.add_argument("--destination", default="clickhouse", help="pipeline.destination for template generation.")
        parser.add_argument("--dataset", default="raw", help="pipeline.dataset for template generation.")
        parser.add_argument("--out", default=None, help="Output file for template generation (if not set: print to stdout).")
        return parser

    def run(self, args: argparse.Namespace) -> int:
        if args.template_kind:
            return self._run_template(args)
        return self._run_analysis(args)

    def _run_template(self, args: argparse.Namespace) -> int:
        if not args.pipeline_name:
            raise SystemExit("--pipeline-name is required for template generation")
        content = render_template(kind=str(args.template_kind), pipeline_name=str(args.pipeline_name), destination=str(args.destination), dataset=str(args.dataset))
        if args.out:
            out = Path(str(args.out)).expanduser().resolve()
            out.parent.mkdir(parents=True, exist_ok=True)
            out.write_text(content, encoding="utf-8")
            print(f"Wrote template: {out}")
        else:
            print(content)
        return 0

    def _select_paths(self, args: argparse.Namespace) -> List[Path]:
        if args.manifest:
            return [Path(item).expanduser().resolve() for item in args.manifest]
        base = Path(str(args.manifests_dir)).expanduser().resolve()
        if not base.exists():
            raise SystemExit(f"Manifests directory not found: {base}")
        return discover_yaml_files(base)

    def _run_analysis(self, args: argparse.Namespace) -> int:
        selected = self._select_paths(args)
        if not selected:
            raise SystemExit("No manifests found")
        total_issues = 0
        total_fixed = 0
        for path in selected:
            try:
                data = load_yaml_mapping(path)
                issues = analyze_manifest(path, data)
                fixable = [issue for issue in issues if issue.level == "FIX" and issue.fix is not None]
                warns = [issue for issue in issues if issue.level == "WARN"]
                errs = [issue for issue in issues if issue.level == "ERROR"]
                if not issues:
                    print(f"OK   {path.name}")
                    continue
                total_issues += len(issues)
                print(f"ISSUE {path.name}: fixes={len(fixable)}, warnings={len(warns)}, errors={len(errs)}")
                for issue in issues:
                    print(f"  - [{issue.level}] {issue.code}: {issue.message}")
                if args.apply and fixable:
                    original = path.read_text(encoding="utf-8")
                    updated, applied = apply_fixes_to_text(original, fixable)
                    if applied and updated != original:
                        path.write_text(updated, encoding="utf-8")
                        total_fixed += len(applied)
                        print(f"  APPLY: {', '.join(applied)}")

                source_kind = str(((data.get("source") or {}) if isinstance(data.get("source") or {}, dict) else {}).get("kind") or "").strip()
                if is_sqlish_source_kind(source_kind):
                    canonical_yaml = render_sqldb_canonical_manifest_yaml(data)
                    if args.print_sqldb_canonical:
                        print("  CANONICAL_SQLDB_BEGIN")
                        print(canonical_yaml.rstrip())
                        print("  CANONICAL_SQLDB_END")
                    if args.write_sqldb_canonical_dir:
                        out_dir = Path(str(args.write_sqldb_canonical_dir)).expanduser().resolve()
                        out_dir.mkdir(parents=True, exist_ok=True)
                        out_path = out_dir / path.name
                        out_path.write_text(canonical_yaml, encoding="utf-8")
                        print(f"  CANONICAL_SQLDB_WRITE: {out_path}")

                if args.fail_fast and (errs or (args.check and (warns or fixable))):
                    return 1
            except Exception as exc:
                print(f"FAIL {path.name}: {exc}")
                if args.fail_fast:
                    return 1
                total_issues += 1
        if args.check and total_issues:
            return 1
        print(f"Done. Total issues={total_issues}, applied_fixes={total_fixed}")
        return 0


def main(argv: Optional[Sequence[str]] = None) -> int:
    parser = ManifestDoctorService.build_parser(prog="dlt-manifest-doctor")
    args = parser.parse_args(argv)
    import logging
    svc = ManifestDoctorService(logger=logging.getLogger("dlt.manifest_doctor"))
    return svc.run(args)


__all__ = ["ManifestDoctorService", "main"]
