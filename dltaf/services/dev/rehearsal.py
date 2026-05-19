from __future__ import annotations

import argparse
import json
import os
import shutil
import subprocess
import sys
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Iterable, Sequence

from dltaf.services.dev.repo_split import RepoSplitService
from dltaf.services.execution.redaction_service import redact_text


@dataclass(frozen=True)
class ValidationStepResult:
    name: str
    status: str
    command: list[str]
    cwd: str
    returncode: int | None = None
    stdout: str = ""
    stderr: str = ""
    note: str = ""

    def as_json(self) -> dict[str, object]:
        return asdict(self)


@dataclass(frozen=True)
class RehearsalReport:
    repo_name: str
    repo_dir: str
    steps: list[ValidationStepResult]

    @property
    def failed(self) -> int:
        return sum(1 for step in self.steps if step.status == "failed")

    @property
    def passed(self) -> int:
        return sum(1 for step in self.steps if step.status == "passed")

    @property
    def skipped(self) -> int:
        return sum(1 for step in self.steps if step.status == "skipped")

    @property
    def ok(self) -> bool:
        return self.failed == 0

    def as_json(self) -> dict[str, object]:
        return {
            "repo_name": self.repo_name,
            "repo_dir": self.repo_dir,
            "summary": {
                "passed": self.passed,
                "failed": self.failed,
                "skipped": self.skipped,
                "ok": self.ok,
            },
            "steps": [step.as_json() for step in self.steps],
        }


def add_import_rehearsal_arguments(parser) -> None:
    parser.add_argument("--out-dir", required=True, help="Output directory for rehearsal run bundle and reports.")
    parser.add_argument("--clean", action="store_true", help="Delete output directory before export.")
    parser.add_argument("--framework-package-ref", default="", help="Optional wheel path or pip install spec for consumer validation.")
    parser.add_argument("--skip-framework-validation", action="store_true", help="Only prepare framework rehearsal repo, do not run validation commands.")
    parser.add_argument("--skip-consumer-validation", action="store_true", help="Only prepare consumer rehearsal repo, do not run validation commands.")
    parser.add_argument("--keep-work-dir", action="store_true", help="Keep work directories even if validation fails.")
    parser.add_argument("--framework-pytest-args", default="-q", help="Arguments for framework pytest run (default: -q).")


class ImportRehearsalService:
    def __init__(self, *, ctx, logger) -> None:
        self.ctx = ctx
        self.logger = logger
        self._redact = redact_text

    def _python(self) -> str:
        return sys.executable

    def _clean_env(self) -> dict[str, str]:
        env = dict(os.environ)
        env.setdefault("PYTHONHASHSEED", "0")
        env.pop("PYTHONPATH", None)
        return env

    def _ensure_clean_dir(self, path: Path, *, clean: bool) -> None:
        if path.exists() and clean:
            shutil.rmtree(path)
        path.mkdir(parents=True, exist_ok=True)

    def _prepare_repo(self, *, src_dir: Path, dst_dir: Path, clean: bool, commit_message: str) -> Path:
        if dst_dir.exists() and clean:
            shutil.rmtree(dst_dir)
        dst_dir.parent.mkdir(parents=True, exist_ok=True)
        shutil.copytree(src_dir, dst_dir, dirs_exist_ok=False)
        if shutil.which("git"):
            subprocess.run(["git", "init"], cwd=dst_dir, check=False, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
            subprocess.run(["git", "checkout", "-b", "split/cutover-rehearsal"], cwd=dst_dir, check=False, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
            subprocess.run(["git", "add", "."], cwd=dst_dir, check=False, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
            subprocess.run(["git", "commit", "-m", commit_message], cwd=dst_dir, check=False, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        return dst_dir

    def _truncate(self, value: str, limit: int = 4000) -> str:
        value = self._redact(value)
        if len(value) <= limit:
            return value
        return value[:limit] + "\n...[truncated]"

    def _run_step(self, *, name: str, command: Sequence[str], cwd: Path, env: dict[str, str]) -> ValidationStepResult:
        proc = subprocess.run(list(command), cwd=str(cwd), env=env, text=True, capture_output=True)
        status = "passed" if proc.returncode == 0 else "failed"
        return ValidationStepResult(
            name=name,
            status=status,
            command=list(command),
            cwd=str(cwd),
            returncode=proc.returncode,
            stdout=self._truncate(proc.stdout),
            stderr=self._truncate(proc.stderr),
        )

    def _skip_step(self, *, name: str, cwd: Path, note: str) -> ValidationStepResult:
        return ValidationStepResult(
            name=name,
            status="skipped",
            command=[],
            cwd=str(cwd),
            note=self._truncate(note),
        )

    def _venv_python(self, venv_dir: Path) -> Path:
        if os.name == "nt":
            return venv_dir / "Scripts" / "python.exe"
        return venv_dir / "bin" / "python"

    def _create_venv(self, venv_dir: Path) -> Path:
        if venv_dir.exists():
            shutil.rmtree(venv_dir)
        subprocess.run([self._python(), "-m", "venv", "--system-site-packages", str(venv_dir)], check=True)
        py = self._venv_python(venv_dir)
        return py

    def _find_latest_wheel(self, dist_dir: Path) -> Path | None:
        wheels = sorted(dist_dir.glob("*.whl"))
        return wheels[-1] if wheels else None

    def _render_report_md(self, report: RehearsalReport) -> str:
        lines = [
            f"# Rehearsal report: {report.repo_name}",
            "",
            f"- repo_dir: `{report.repo_dir}`",
            f"- passed: **{report.passed}**",
            f"- failed: **{report.failed}**",
            f"- skipped: **{report.skipped}**",
            f"- ok: **{'yes' if report.ok else 'no'}**",
            "",
        ]
        for step in report.steps:
            lines.extend(
                [
                    f"## {step.name}",
                    "",
                    f"- status: **{step.status}**",
                    f"- cwd: `{step.cwd}`",
                ]
            )
            if step.command:
                lines.append(f"- command: `{ ' '.join(step.command) }`")
            if step.returncode is not None:
                lines.append(f"- returncode: `{step.returncode}`")
            if step.note:
                lines.append(f"- note: {step.note}")
            if step.stdout:
                lines.extend(["", "### stdout", "", "```text", step.stdout, "```"])
            if step.stderr:
                lines.extend(["", "### stderr", "", "```text", step.stderr, "```"])
            lines.append("")
        return "\n".join(lines).rstrip() + "\n"

    def _write_report_files(self, path_prefix: Path, report: RehearsalReport) -> None:
        path_prefix.parent.mkdir(parents=True, exist_ok=True)
        (path_prefix.with_suffix(".json")).write_text(
            json.dumps(report.as_json(), ensure_ascii=False, indent=2) + "\n",
            encoding="utf-8",
        )
        (path_prefix.with_suffix(".md")).write_text(self._render_report_md(report), encoding="utf-8")

    def _validate_framework(self, repo_dir: Path, *, pytest_args: str) -> tuple[RehearsalReport, Path | None]:
        env = self._clean_env()
        steps: list[ValidationStepResult] = []
        py = self._python()

        steps.append(self._run_step(name="dependency_rules", command=[py, "-m", "ci_scripts.dependency_rules", "--repo-root", "."], cwd=repo_dir, env=env))
        steps.append(self._run_step(name="docs_generate_cli_reference", command=[py, "-m", "dltaf.cli", "--repo-root", ".", "docs", "generate-cli-reference", "--check", "--show-diff"], cwd=repo_dir, env=env))
        steps.append(self._run_step(name="docs_update_dev_metrics", command=[py, "-m", "dltaf.cli", "--repo-root", ".", "docs", "update-dev-metrics", "--check", "--show-diff"], cwd=repo_dir, env=env))

        pytest_cmd = [py, "-m", "pytest"] + [part for part in pytest_args.split() if part]
        steps.append(self._run_step(name="pytest", command=pytest_cmd, cwd=repo_dir, env=env))

        wheel_path: Path | None = None
        try:
            import build  # noqa: F401
        except Exception:
            steps.append(self._skip_step(name="build", cwd=repo_dir, note="Модуль 'build' не установлен; пропускаю сборку wheel/sdist."))
            steps.append(self._skip_step(name="package_smoke", cwd=repo_dir, note="Нет wheel-артефакта для package smoke."))
        else:
            build_step = self._run_step(name="build", command=[py, "-m", "build"], cwd=repo_dir, env=env)
            steps.append(build_step)
            if build_step.status == "passed":
                wheel_path = self._find_latest_wheel(repo_dir / "dist")
            if wheel_path is None:
                steps.append(self._skip_step(name="package_smoke", cwd=repo_dir, note="После сборки не найден wheel-артефакт в dist/."))
            else:
                venv_dir = repo_dir / ".rehearsal-venv-framework"
                vpy = self._create_venv(venv_dir)
                pip_cmd = [str(vpy), "-m", "pip", "install", "--no-deps", "--force-reinstall", str(wheel_path)]
                steps.append(self._run_step(name="install_framework_wheel", command=pip_cmd, cwd=repo_dir, env=env))
                steps.append(self._run_step(name="package_smoke", command=[str(vpy), "-m", "ci_scripts.package_smoke"], cwd=repo_dir, env=env))

        return RehearsalReport(repo_name="framework", repo_dir=str(repo_dir), steps=steps), wheel_path

    def _validate_consumer(self, repo_dir: Path, *, framework_package_ref: str) -> RehearsalReport:
        env = self._clean_env()
        steps: list[ValidationStepResult] = []

        if not framework_package_ref:
            steps.append(self._skip_step(name="install_framework_package", cwd=repo_dir, note="Не задан framework package ref и wheel не был собран; consumer validation пропущена."))
            return RehearsalReport(repo_name="consumer", repo_dir=str(repo_dir), steps=steps)

        venv_dir = repo_dir / ".rehearsal-venv-consumer"
        vpy = self._create_venv(venv_dir)
        steps.append(self._run_step(name="install_framework_package", command=[str(vpy), "-m", "pip", "install", "--no-deps", "--force-reinstall", framework_package_ref], cwd=repo_dir, env=env))
        steps.append(self._run_step(name="manifest_lint", command=[str(vpy), "-m", "dltaf.cli", "--repo-root", ".", "manifest", "lint", "--manifests-dir", "dlt_pipelines/manifests"], cwd=repo_dir, env=env))
        steps.append(self._run_step(name="contracts_test", command=[str(vpy), "-m", "dltaf.cli", "--repo-root", ".", "contracts", "test", "--manifests-dir", "dlt_pipelines/manifests", "--output", "-", "--format", "json"], cwd=repo_dir, env=env))
        steps.append(self._run_step(name="dags_generate", command=[str(vpy), "-m", "dltaf.cli", "--repo-root", ".", "dags", "generate", "--manifests-dir", "dlt_pipelines/manifests", "--output-dir", "build/dags-generated", "--clean"], cwd=repo_dir, env=env))

        if shutil.which("helm"):
            steps.append(self._run_step(name="helm_lint", command=["helm", "lint", "airflow"], cwd=repo_dir, env=env))
            steps.append(self._run_step(name="helm_template", command=["helm", "template", "dltaf-airflow", "airflow"], cwd=repo_dir, env=env))
        else:
            steps.append(self._skip_step(name="helm_lint", cwd=repo_dir, note="Команда helm не найдена; пропускаю lint/template."))

        return RehearsalReport(repo_name="consumer", repo_dir=str(repo_dir), steps=steps)

    def _write_summary(self, reports_dir: Path, reports: Iterable[RehearsalReport]) -> None:
        items = [report.as_json() for report in reports]
        summary = {
            "reports": items,
            "overall_ok": all(bool(item["summary"]["ok"]) for item in items),
        }
        (reports_dir / "SUMMARY.json").write_text(json.dumps(summary, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
        lines = ["# Import rehearsal summary", ""]
        for item in items:
            summary_item = item["summary"]
            lines.extend([
                f"## {item['repo_name']}",
                "",
                f"- ok: **{'yes' if summary_item['ok'] else 'no'}**",
                f"- passed: **{summary_item['passed']}**",
                f"- failed: **{summary_item['failed']}**",
                f"- skipped: **{summary_item['skipped']}**",
                f"- report: `{item['repo_name']}.md`",
                "",
            ])
        (reports_dir / "SUMMARY.md").write_text("\n".join(lines).rstrip() + "\n", encoding="utf-8")

    def run(self, args: argparse.Namespace) -> int:
        out_dir = Path(args.out_dir).expanduser().resolve()
        if out_dir.exists() and args.clean:
            shutil.rmtree(out_dir)
        out_dir.mkdir(parents=True, exist_ok=True)

        bundle_dir = out_dir / "bundle"
        work_dir = out_dir / "work"
        reports_dir = out_dir / "reports"
        reports_dir.mkdir(parents=True, exist_ok=True)

        class _Args:
            clean = True
            out_dir = ""

        export_args = _Args()
        export_args.out_dir = str(bundle_dir)
        repo_split = RepoSplitService(ctx=self.ctx, logger=self.logger)
        repo_split.export_import_rehearsal(export_args)

        framework_src = bundle_dir / "cutover-dry-run" / "dltaf-repo"
        consumer_src = bundle_dir / "cutover-dry-run" / "dltaf-airflow-repo"
        framework_repo = self._prepare_repo(src_dir=framework_src, dst_dir=work_dir / "dltaf-rehearsal", clean=args.clean, commit_message="Rehearsal import: dltaf framework skeleton")
        consumer_repo = self._prepare_repo(src_dir=consumer_src, dst_dir=work_dir / "dltaf-airflow-rehearsal", clean=args.clean, commit_message="Rehearsal import: dltaf-airflow consumer skeleton")

        reports: list[RehearsalReport] = []
        built_wheel: Path | None = None

        if args.skip_framework_validation:
            framework_report = RehearsalReport(
                repo_name="framework",
                repo_dir=str(framework_repo),
                steps=[self._skip_step(name="framework_validation", cwd=framework_repo, note="Пропущено по флагу --skip-framework-validation")],
            )
        else:
            framework_report, built_wheel = self._validate_framework(framework_repo, pytest_args=str(args.framework_pytest_args))
        reports.append(framework_report)
        self._write_report_files(reports_dir / "framework", framework_report)

        package_ref = str(args.framework_package_ref or "").strip()
        if not package_ref and built_wheel is not None:
            package_ref = str(built_wheel)

        if args.skip_consumer_validation:
            consumer_report = RehearsalReport(
                repo_name="consumer",
                repo_dir=str(consumer_repo),
                steps=[self._skip_step(name="consumer_validation", cwd=consumer_repo, note="Пропущено по флагу --skip-consumer-validation")],
            )
        else:
            consumer_report = self._validate_consumer(consumer_repo, framework_package_ref=package_ref)
        reports.append(consumer_report)
        self._write_report_files(reports_dir / "consumer", consumer_report)

        self._write_summary(reports_dir, reports)

        if not args.keep_work_dir and all(report.ok for report in reports):
            self.logger.info("Import rehearsal passed; keeping work dir at %s for inspection.", work_dir)
        else:
            self.logger.info("Import rehearsal work dir: %s", work_dir)

        ok = all(report.ok for report in reports)
        self.logger.info("Import rehearsal reports written to: %s", reports_dir)
        return 0 if ok else 1


class DummyArgs(argparse.Namespace):
    pass


__all__ = [
    "ImportRehearsalService",
    "RehearsalReport",
    "ValidationStepResult",
    "add_import_rehearsal_arguments",
]
