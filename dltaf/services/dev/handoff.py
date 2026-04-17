from __future__ import annotations

import argparse
import json
import shutil
from pathlib import Path

from dltaf.services.dev.repo_split import RepoSplitService


def add_cutover_handoff_arguments(parser) -> None:
    parser.add_argument("--out-dir", required=True, help="Output directory for final cutover handoff bundle.")
    parser.add_argument("--clean", action="store_true", help="Delete output directory before export.")


class CutoverHandoffService:
    def __init__(self, *, ctx, logger) -> None:
        self.ctx = ctx
        self.logger = logger

    def _write_text(self, path: Path, text: str) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(text.rstrip() + "\n", encoding="utf-8")

    def _status_payload(self) -> dict[str, object]:
        return {
            "plan": "repo split",
            "stage": 63,
            "status": "ready_for_physical_cutover",
            "split_preparation_progress_percent": 100,
            "remaining_required_steps": 0,
            "next_action": "Execute physical split in two new repositories using the handoff bundle.",
            "framework_repo": "dltaf",
            "consumer_repo": "dltaf-airflow",
        }

    def _render_readme(self) -> str:
        return "\n".join(
            [
                "# Cutover handoff bundle",
                "",
                "Этот bundle — финальный handoff-пакет для **физического** разделения mixed repo на два репозитория:",
                "",
                "- `dltaf` — framework / package / CLI / tests / docs / CI",
                "- `dltaf-airflow` — manifests / SQL / DAG wrappers / Helm / ArgoCD / deploy",
                "",
                "Текущий статус подготовки:",
                "- split-preparation: **100%**",
                "- remaining required preparation steps: **0**",
                "",
                "Содержимое bundle:",
                "- `cutover-dry-run/` — оба skeleton repo + rehearsal/dry-run материалы",
                "- `HANDOFF_STATUS.json` — машинно-читаемый статус готовности",
                "- `EXECUTIVE_SUMMARY.md` — краткий summary для handoff",
                "- `CUTOVER_DAY_RUNBOOK.md` — порядок действий в день cutover",
                "- `FRAMEWORK_REPO_CREATION.md` — как создавать и наполнять repo `dltaf`",
                "- `CONSUMER_REPO_CREATION.md` — как создавать и наполнять repo `dltaf-airflow`",
                "- `PACKAGE_RELEASE_HANDOFF.md` — handoff по package/release pipeline",
                "- `DEPLOY_HANDOFF.md` — handoff по Airflow/Helm/Argo consumer-репозиторию",
                "- `ROLLBACK_PLAN.md` — что делать при неуспешном cutover",
                "- `FINAL_SIGNOFF_CHECKLIST.md` — финальный checklist перед и после cutover",
                "",
                "Рекомендуемый порядок: сначала прочитать `EXECUTIVE_SUMMARY.md`, затем `CUTOVER_DAY_RUNBOOK.md`.",
            ]
        )

    def _render_exec_summary(self) -> str:
        return "\n".join(
            [
                "# Executive summary",
                "",
                "Подготовка к split завершена. Все обязательные preparatory-артефакты уже есть:",
                "",
                "- file inventory",
                "- exact move maps",
                "- framework/consumer skeleton export",
                "- dry-run cutover bundle",
                "- rehearsal import bundle",
                "- rehearsal validation runner",
                "",
                "Что остаётся сделать:",
                "",
                "1. Создать два новых репозитория: `dltaf` и `dltaf-airflow`.",
                "2. Импортировать в них skeleton/file sets из `cutover-dry-run/`.",
                "3. Включить CI по приложенным checklist/example-файлам.",
                "4. Прогнать framework и consumer validation уже в новых репозиториях.",
                "5. Выполнить controlled cutover для dev/stage/prod consumption.",
                "",
                "Текущая рекомендация: сначала сделать cutover как dry-run в двух временных репозиториях, затем повторить тем же порядком в реальных repo.",
            ]
        )

    def _render_cutover_day(self) -> str:
        return "\n".join(
            [
                "# Cutover day runbook",
                "",
                "## До начала окна",
                "",
                "1. Заморозить merge в текущий mixed repo на время cutover окна.",
                "2. Убедиться, что handoff bundle собран заново от актуального `main`/release commit.",
                "3. Подготовить доступы к двум новым репозиториям и package registry.",
                "4. Назначить owner для framework side и owner для consumer/deploy side.",
                "",
                "## Во время cutover",
                "",
                "1. Создать repo `dltaf` и импортировать содержимое из `cutover-dry-run/dltaf-repo/`.",
                "2. Создать repo `dltaf-airflow` и импортировать содержимое из `cutover-dry-run/dltaf-airflow-repo/`.",
                "3. Подключить CI по example/checklist материалам из bundle.",
                "4. В framework repo прогнать quality gates и package build/smoke.",
                "5. В consumer repo установить/подтянуть package `dltaf` и прогнать manifests/contracts/dags/helm checks.",
                "6. Зафиксировать package version / commit SHA, который будет использовать consumer repo.",
                "",
                "## После cutover",
                "",
                "1. Обновить внутренние ссылки/README/Confluence на новые repo.",
                "2. Для mixed repo перевести README в archival/redirect режим.",
                "3. Отдельно согласовать deprecation срок для старого mixed repo как source of truth.",
            ]
        )

    def _render_repo_creation(self, *, framework: bool) -> str:
        if framework:
            lines = [
                "# Framework repo creation",
                "",
                "## Цель",
                "",
                "Создать новый репозиторий `dltaf` как canonical framework/package repo.",
                "",
                "## Что импортировать",
                "",
                "- всё содержимое из `cutover-dry-run/dltaf-repo/`",
                "",
                "## Что проверить после импорта",
                "",
                "- `python -m ci_scripts.dependency_rules --repo-root .`",
                "- `python -m dltaf.cli --repo-root . docs generate-cli-reference --check --show-diff`",
                "- `python -m dltaf.cli --repo-root . docs update-dev-metrics --check --show-diff`",
                "- `pytest -q`",
                "- `python -m build`",
                "- `python -m ci_scripts.package_smoke`",
                "",
                "## Что зафиксировать в CI/CD",
                "",
                "- release by tag для package `dltaf`",
                "- dev artifact flow без release tags",
                "- package smoke как обязательный gate",
            ]
        else:
            lines = [
                "# Consumer repo creation",
                "",
                "## Цель",
                "",
                "Создать новый репозиторий `dltaf-airflow` как consumer/deploy repo.",
                "",
                "## Что импортировать",
                "",
                "- всё содержимое из `cutover-dry-run/dltaf-airflow-repo/`",
                "",
                "## Что проверить после импорта",
                "",
                "- установить package `dltaf` из framework repo или registry",
                "- `dltaf manifest lint --manifests-dir dlt_pipelines/manifests`",
                "- `dltaf contracts test --manifests-dir dlt_pipelines/manifests --output - --format json`",
                "- `dltaf dags generate --manifests-dir dlt_pipelines/manifests --output-dir build/dags-generated --clean`",
                "- `helm lint` / `helm template` (если helm есть в окружении)",
                "",
                "## Что зафиксировать в CI/CD",
                "",
                "- package version pin для `dltaf`",
                "- dev package override без release tag",
                "- consumer validation pipeline",
            ]
        return "\n".join(lines)

    def _render_package_handoff(self) -> str:
        return "\n".join(
            [
                "# Package release handoff",
                "",
                "Framework repo `dltaf` после split отвечает за:",
                "",
                "- build wheel/sdist",
                "- package smoke",
                "- publish dev artifacts по commit SHA / branch",
                "- publish release artifacts по tag",
                "- release notes и deprecation policy",
                "",
                "Consumer repo `dltaf-airflow` не должен содержать код framework и не должен rebuild framework из source checkout.",
                "",
                "### Минимальный handoff contract",
                "",
                "- package name: `dltaf`",
                "- CLI: `dltaf`",
                "- dev artifact flow: commit-based",
                "- stage/prod flow: version pin / release tag",
            ]
        )

    def _render_deploy_handoff(self) -> str:
        return "\n".join(
            [
                "# Deploy handoff",
                "",
                "Repo `dltaf-airflow` после split отвечает за deploy-consumption package `dltaf`.",
                "",
                "## Параметры, которые должны появиться в values/Argo",
                "",
                "- `dltaf.packageName`",
                "- `dltaf.packageVersion`",
                "- `dltaf.extraIndexUrl`",
                "- `dltaf.indexUrl`",
                "- `dltaf.installMode`",
                "",
                "## Ожидаемая модель",
                "",
                "- dev: commit-based dev package без release tag",
                "- stage/prod: version pin на published release package",
                "- Airflow/DAG wrappers импортируют framework только как установленный package `dltaf`",
            ]
        )

    def _render_rollback(self) -> str:
        return "\n".join(
            [
                "# Rollback plan",
                "",
                "Если cutover не проходит:",
                "",
                "1. Не продвигать package/version pin в consumer deploy flows.",
                "2. Оставить mixed repo единственным источником истины.",
                "3. Зафиксировать найденные проблемы в framework или consumer skeleton и повторить rehearsal.",
                "4. Не удалять historical CI/jobs в mixed repo до финального signoff.",
                "",
                "Rollback считается успешным, если старый mixed repo всё ещё способен пройти базовые проверки и обслуживать текущие deploy flows.",
            ]
        )

    def _render_signoff(self) -> str:
        return "\n".join(
            [
                "# Final sign-off checklist",
                "",
                "## Перед cutover",
                "- [ ] handoff bundle пересобран от актуального commit",
                "- [ ] framework owner назначен",
                "- [ ] consumer/deploy owner назначен",
                "- [ ] package registry credentials проверены",
                "- [ ] доступы к новым repo подтверждены",
                "",
                "## После импорта в новые repo",
                "- [ ] framework CI green",
                "- [ ] consumer CI green",
                "- [ ] package build и smoke green",
                "- [ ] manifests lint / contracts / dags generate green",
                "- [ ] helm/argo validation green",
                "",
                "## Перед production handoff",
                "- [ ] version pin strategy согласована",
                "- [ ] rollback owner назначен",
                "- [ ] communication sent",
                "- [ ] old mixed repo переведён в maintenance/archival mode plan",
            ]
        )

    def _write_nested_handoff_notes(self, root: Path) -> None:
        framework_repo = root / "cutover-dry-run" / "dltaf-repo"
        consumer_repo = root / "cutover-dry-run" / "dltaf-airflow-repo"
        self._write_text(
            framework_repo / "FINAL_HANDOFF.md",
            "\n".join(
                [
                    "# Final framework handoff",
                    "",
                    "Этот skeleton уже готов к физическому переносу в repo `dltaf`.",
                    "Следующий обязательный шаг: выполнить checklist из `CI_MIGRATION_CHECKLIST.md`, затем прогнать package build/smoke.",
                ]
            ),
        )
        self._write_text(
            consumer_repo / "FINAL_HANDOFF.md",
            "\n".join(
                [
                    "# Final consumer handoff",
                    "",
                    "Этот skeleton уже готов к физическому переносу в repo `dltaf-airflow`.",
                    "Следующий обязательный шаг: подключить опубликованный package `dltaf` и прогнать manifest/contracts/dags/helm validation.",
                ]
            ),
        )

    def run(self, args: argparse.Namespace) -> int:
        out_dir = Path(args.out_dir).expanduser().resolve()
        if out_dir.exists() and args.clean:
            shutil.rmtree(out_dir)
        out_dir.mkdir(parents=True, exist_ok=True)

        split_service = RepoSplitService(ctx=self.ctx, logger=self.logger)
        split_service.export_import_rehearsal(argparse.Namespace(out_dir=str(out_dir), clean=False))

        status = self._status_payload()
        (out_dir / "HANDOFF_STATUS.json").write_text(
            json.dumps(status, ensure_ascii=False, indent=2) + "\n",
            encoding="utf-8",
        )
        self._write_text(out_dir / "README.md", self._render_readme())
        self._write_text(out_dir / "EXECUTIVE_SUMMARY.md", self._render_exec_summary())
        self._write_text(out_dir / "CUTOVER_DAY_RUNBOOK.md", self._render_cutover_day())
        self._write_text(out_dir / "FRAMEWORK_REPO_CREATION.md", self._render_repo_creation(framework=True))
        self._write_text(out_dir / "CONSUMER_REPO_CREATION.md", self._render_repo_creation(framework=False))
        self._write_text(out_dir / "PACKAGE_RELEASE_HANDOFF.md", self._render_package_handoff())
        self._write_text(out_dir / "DEPLOY_HANDOFF.md", self._render_deploy_handoff())
        self._write_text(out_dir / "ROLLBACK_PLAN.md", self._render_rollback())
        self._write_text(out_dir / "FINAL_SIGNOFF_CHECKLIST.md", self._render_signoff())
        self._write_nested_handoff_notes(out_dir)

        self.logger.info("Cutover handoff bundle exported: %s", out_dir)
        return 0


__all__ = ["CutoverHandoffService", "add_cutover_handoff_arguments"]
