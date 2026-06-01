from __future__ import annotations

import argparse
import json
import shutil
from dataclasses import dataclass
from fnmatch import fnmatch
from pathlib import Path
from typing import Iterable

from dltaf.app.context import AppContext

FRAMEWORK_PATTERNS: tuple[str, ...] = (
    "README.md",
    ".gitlab-ci.yml",
    ".pre-commit-config.yaml",
    ".gitleaks.toml",
    ".secrets.baseline",
    ".gitignore",
    "MANIFEST.in",
    "mypy.ini",
    "pytest.ini",
    "pyproject.toml",
    "requirements.txt",
    "requirements-dev.txt",
    "docker-compose.integration.yml",
    "dltaf/**",
    "dlt_utils/**",
    "cli/**",
    "dag_builder/**",
    "lineage/**",
    "ci_scripts/**",
    "docs/**",
    "tests/**",
    "integration/**",
    "dlt_pipelines/__init__.py",
    "dlt_pipelines/pkbconc__dwh__pipeline/**",
    "dlt_pipelines/uploader__b057__pipeline/**",
    "dlt_pipelines/mongodb__bpm_cc__pipeline/**",
)

CONSUMER_PATTERNS: tuple[str, ...] = (
    "dlt_pipelines/manifests/**",
    "dlt_pipelines/sql/**",
    "dags/**",
    "airflow/**",
    "argocd/**",
)

DEFAULT_EXCLUDES: tuple[str, ...] = (
    ".git/**",
    ".mypy_cache/**",
    ".pytest_cache/**",
    ".ruff_cache/**",
    ".venv/**",
    ".venv*/**",
    "__pycache__/**",
    "*.pyc",
    "*.pyo",
    "*.zip",
    "*.patch",
    "dist/**",
    "dist-dev/**",
    "build/**",
    "*.egg-info/**",
    "mr_comment.md",
)

CONSUMER_REQUIRED_DIRS: tuple[str, ...] = (
    "dlt_pipelines/manifests",
    "dlt_pipelines/sql",
    "dags",
    "airflow",
    "argocd",
)

FRAMEWORK_REASON_HINTS: tuple[tuple[str, str], ...] = (
    ("dltaf/**", "canonical framework code"),
    ("dlt_utils/**", "compatibility layer and legacy entrypoints"),
    ("cli/**", "legacy CLI wrappers"),
    ("dag_builder/**", "reusable DAG builder logic"),
    ("lineage/**", "framework lineage tooling"),
    ("ci_scripts/**", "framework CI and smoke tooling"),
    ("docs/**", "framework documentation"),
    ("tests/**", "framework tests"),
    ("integration/**", "framework integration smoke assets"),
    ("dlt_pipelines/__init__.py", "compatibility package root for historical wrappers"),
    ("dlt_pipelines/**", "built-in integration runtime assets"),
)

CONSUMER_REASON_HINTS: tuple[tuple[str, str], ...] = (
    ("dlt_pipelines/manifests/**", "consumer manifests"),
    ("dlt_pipelines/sql/**", "consumer/business SQL files"),
    ("dags/**", "Airflow DAG wrappers / generated DAGs"),
    ("airflow/**", "Helm chart and values"),
    ("argocd/**", "ArgoCD / GitOps definitions"),
)


@dataclass(frozen=True)
class RepoSplitInventory:
    framework_files: list[str]
    consumer_files: list[str]
    unresolved_files: list[str]

    def as_json(self) -> dict[str, object]:
        return {
            "framework_files": self.framework_files,
            "consumer_files": self.consumer_files,
            "unresolved_files": self.unresolved_files,
            "counts": {
                "framework": len(self.framework_files),
                "consumer": len(self.consumer_files),
                "unresolved": len(self.unresolved_files),
            },
        }

    def render_markdown(self) -> str:
        counts = self.as_json()["counts"]
        lines: list[str] = [
            "# Repo split inventory",
            "",
            "Этот файл сгенерирован командой `dltaf dev repo-split-inventory`.",
            "",
            "## Сводка",
            "",
            f"- framework files: **{counts['framework']}**",
            f"- consumer files: **{counts['consumer']}**",
            f"- unresolved files: **{counts['unresolved']}**",
            "",
            "## Важное решение по `dlt_pipelines/__init__.py`",
            "",
            "- `dlt_pipelines/__init__.py` относится к **framework repo `dltaf`**.",
            "- Он нужен только как compatibility package root для historical wrapper modules.",
            "- В consumer repo `dltaf-airflow` каталог `dlt_pipelines/` остаётся обычным файловым деревом для `manifests/` и `sql/`, без обязательного Python package root.",
            "",
        ]
        for title, items in (
            ("Framework repo (`dltaf`)", self.framework_files),
            ("Consumer repo (`dltaf-airflow`)", self.consumer_files),
            ("Не классифицировано (нужно решение)", self.unresolved_files),
        ):
            lines.append(f"## {title}")
            lines.append("")
            if items:
                lines.extend(f"- `{item}`" for item in items)
            else:
                lines.append("_Пусто_")
            lines.append("")
        return "\n".join(lines).rstrip() + "\n"


@dataclass(frozen=True)
class RepoMoveMapEntry:
    source_path: str
    target_repo: str
    target_path: str
    reason: str


@dataclass(frozen=True)
class RepoMoveMap:
    repo_name: str
    entries: list[RepoMoveMapEntry]
    unresolved_files: list[str]

    def as_json(self) -> dict[str, object]:
        return {
            "repo_name": self.repo_name,
            "entries": [
                {
                    "source_path": item.source_path,
                    "target_repo": item.target_repo,
                    "target_path": item.target_path,
                    "reason": item.reason,
                }
                for item in self.entries
            ],
            "unresolved_files": self.unresolved_files,
            "counts": {
                "entries": len(self.entries),
                "unresolved": len(self.unresolved_files),
            },
        }

    def render_markdown(self) -> str:
        counts = self.as_json()["counts"]
        title = "dltaf" if self.repo_name == "framework" else "dltaf-airflow"
        lines = [
            f"# Move map: {title}",
            "",
            "Этот файл показывает точный mapping путей из mixed repo в целевой репозиторий.",
            "",
            f"- target repo: **{title}**",
            f"- files: **{counts['entries']}**",
            f"- unresolved files in source inventory: **{counts['unresolved']}**",
            "",
            "| Source path | Target path | Reason |",
            "| --- | --- | --- |",
        ]
        for item in self.entries:
            lines.append(
                f"| `{item.source_path}` | `{item.target_path}` | {item.reason} |"
            )
        if self.unresolved_files:
            lines.extend(
                [
                    "",
                    "## Unresolved files in source inventory",
                    "",
                    "Эти файлы пока не включены в move map и требуют отдельного решения:",
                    "",
                ]
            )
            lines.extend(f"- `{item}`" for item in self.unresolved_files)
        return "\n".join(lines).rstrip() + "\n"


def _iter_repo_files(repo_root: Path) -> Iterable[str]:
    for path in sorted(repo_root.rglob("*")):
        if path.is_dir():
            continue
        rel = path.relative_to(repo_root).as_posix()
        if any(fnmatch(rel, pattern) for pattern in DEFAULT_EXCLUDES):
            continue
        yield rel


def _classify(rel: str) -> str:
    if any(fnmatch(rel, pattern) for pattern in FRAMEWORK_PATTERNS):
        return "framework"
    if any(fnmatch(rel, pattern) for pattern in CONSUMER_PATTERNS):
        return "consumer"
    return "unresolved"


def _reason(rel: str, *, target: str) -> str:
    hints = FRAMEWORK_REASON_HINTS if target == "framework" else CONSUMER_REASON_HINTS
    for pattern, reason in hints:
        if fnmatch(rel, pattern):
            return reason
    return "move to target repo"


def build_inventory(repo_root: Path) -> RepoSplitInventory:
    framework: list[str] = []
    consumer: list[str] = []
    unresolved: list[str] = []

    for rel in _iter_repo_files(repo_root):
        kind = _classify(rel)
        if kind == "framework":
            framework.append(rel)
        elif kind == "consumer":
            consumer.append(rel)
        else:
            unresolved.append(rel)

    return RepoSplitInventory(
        framework_files=framework,
        consumer_files=consumer,
        unresolved_files=unresolved,
    )


def build_framework_move_map(inventory: RepoSplitInventory) -> RepoMoveMap:
    return RepoMoveMap(
        repo_name="framework",
        entries=[
            RepoMoveMapEntry(
                source_path=rel,
                target_repo="dltaf",
                target_path=rel,
                reason=_reason(rel, target="framework"),
            )
            for rel in inventory.framework_files
        ],
        unresolved_files=inventory.unresolved_files,
    )


def build_consumer_move_map(inventory: RepoSplitInventory) -> RepoMoveMap:
    return RepoMoveMap(
        repo_name="consumer",
        entries=[
            RepoMoveMapEntry(
                source_path=rel,
                target_repo="dltaf-airflow",
                target_path=rel,
                reason=_reason(rel, target="consumer"),
            )
            for rel in inventory.consumer_files
        ],
        unresolved_files=inventory.unresolved_files,
    )


def _copy_files(repo_root: Path, out_dir: Path, files: list[str]) -> None:
    for rel in files:
        src = repo_root / rel
        dst = out_dir / rel
        dst.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(src, dst)


def _write_shared_reports(out_dir: Path, inventory: RepoSplitInventory) -> None:
    (out_dir / "repo_split_inventory.json").write_text(
        json.dumps(inventory.as_json(), ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )
    (out_dir / "REPO_SPLIT_INVENTORY.md").write_text(
        inventory.render_markdown(),
        encoding="utf-8",
    )


def _write_move_maps(out_dir: Path, inventory: RepoSplitInventory) -> None:
    framework_map = build_framework_move_map(inventory)
    consumer_map = build_consumer_move_map(inventory)
    (out_dir / "framework_move_map.json").write_text(
        json.dumps(framework_map.as_json(), ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )
    (out_dir / "consumer_move_map.json").write_text(
        json.dumps(consumer_map.as_json(), ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )
    (out_dir / "FRAMEWORK_MOVE_MAP.md").write_text(
        framework_map.render_markdown(),
        encoding="utf-8",
    )
    (out_dir / "CONSUMER_MOVE_MAP.md").write_text(
        consumer_map.render_markdown(),
        encoding="utf-8",
    )


def _tree(files: list[str], *, max_depth: int = 2) -> str:
    tree: dict[str, dict] = {}
    for rel in files:
        node = tree
        for part in Path(rel).parts:
            node = node.setdefault(part, {})

    def render(node: dict[str, dict], depth: int) -> list[str]:
        lines: list[str] = []
        indent = "  " * depth
        for name in sorted(node):
            child = node[name]
            lines.append(f"{indent}- `{name}`")
            if child and depth + 1 < max_depth:
                lines.extend(render(child, depth + 1))
            elif child and depth + 1 >= max_depth:
                lines.append(f"{indent}  - `…`")
        return lines

    return "\n".join(render(tree, 0)).rstrip() + "\n"


def _write_layouts(out_dir: Path, inventory: RepoSplitInventory) -> None:
    (out_dir / "FRAMEWORK_REPO_LAYOUT.md").write_text(
        "# Proposed framework repo layout\n\n" + _tree(inventory.framework_files),
        encoding="utf-8",
    )
    (out_dir / "CONSUMER_REPO_LAYOUT.md").write_text(
        "# Proposed consumer repo layout\n\n" + _tree(inventory.consumer_files),
        encoding="utf-8",
    )


def _render_framework_skeleton_readme() -> str:
    return "\n".join(
        [
            "# dltaf framework repo skeleton",
            "",
            "Этот каталог сгенерирован командой `dltaf dev export-framework-skeleton`.",
            "",
            "Он содержит файлы, которые предлагается перенести в отдельный framework repo `dltaf`.",
            "",
            "Особое правило split:",
            "- `dlt_pipelines/__init__.py` остаётся в framework repo как compatibility package root.",
            "- `dlt_pipelines/manifests` и `dlt_pipelines/sql` не относятся к framework repo.",
            "",
            "См. также:",
            "- `REPO_SPLIT_INVENTORY.md`",
            "- `repo_split_inventory.json`",
            "- `FRAMEWORK_MOVE_MAP.md`",
            "- `FRAMEWORK_REPO_LAYOUT.md`",
            "- `DRY_RUN_CUTOVER_CHECKLIST.md`",
            "- `CUTOVER_BRANCH_STRATEGY.md`",
            "- `FRAMEWORK_CI_MIGRATION_CHECKLIST.md`",
            "- `docs/REPO_SPLIT_PLAN.md`",
            "",
        ]
    ) + "\n"


def _render_consumer_skeleton_readme() -> str:
    return "\n".join(
        [
            "# dltaf-airflow consumer repo skeleton",
            "",
            "Этот каталог сгенерирован командой `dltaf dev export-consumer-skeleton`.",
            "",
            "Он содержит файлы, которые предлагается перенести в consumer/deploy repo `dltaf-airflow`.",
            "",
            "Особое правило split:",
            "- `dlt_pipelines/manifests` и `dlt_pipelines/sql` остаются в consumer repo.",
            "- `dlt_pipelines/__init__.py` **не** переносится: consumer repo не обязан хранить `dlt_pipelines` как Python package root.",
            "",
            "См. также:",
            "- `REPO_SPLIT_INVENTORY.md`",
            "- `repo_split_inventory.json`",
            "- `CONSUMER_MOVE_MAP.md`",
            "- `CONSUMER_REPO_LAYOUT.md`",
            "- `DRY_RUN_CUTOVER_CHECKLIST.md`",
            "- `CUTOVER_BRANCH_STRATEGY.md`",
            "- `CONSUMER_CI_MIGRATION_CHECKLIST.md`",
            "- `docs/REPO_SPLIT_PLAN.md`",
            "",
        ]
    ) + "\n"


def _render_cutover_checklist(inventory: RepoSplitInventory) -> str:
    unresolved = len(inventory.unresolved_files)
    return "\n".join(
        [
            "# Dry-run cutover checklist",
            "",
            "Этот checklist предназначен для dry-run подготовки физического split на `dltaf` и `dltaf-airflow`.",
            "",
            "## Перед началом",
            "",
            "- [ ] Зафиксировать окно cutover и ответственных за framework repo и consumer repo.",
            f"- [ ] Проверить inventory: unresolved files = **{unresolved}**.",
            "- [ ] Если unresolved > 0: принять решения по каждому файлу до физического split.",
            "",
            "## Dry-run export",
            "",
            "- [ ] Выполнить `dltaf dev repo-split-inventory --format md --out build/repo-split/REPO_SPLIT_INVENTORY.md`.",
            "- [ ] Выполнить `dltaf dev export-framework-skeleton --out-dir build/dltaf-framework-skeleton --clean`.",
            "- [ ] Выполнить `dltaf dev export-consumer-skeleton --out-dir build/dltaf-airflow-skeleton --clean`.",
            "- [ ] Выполнить `dltaf dev export-repo-split-plan --out-dir build/repo-split-plan --clean`.",
            "- [ ] Выполнить `dltaf dev export-cutover-dry-run --out-dir build/repo-split-dry-run --clean`.",
            "",
            "## Проверки для framework repo",
            "",
            "- [ ] В framework skeleton проходят: `ruff check .`, `python -m mypy --config-file mypy.ini`, `pytest -q`.",
            "- [ ] Проходит `python -m ci_scripts.package_smoke` после build/install wheel.",
            "- [ ] Проходят docs checks: `dltaf docs generate-cli-reference --check` и `dltaf docs update-dev-metrics --check`.",
            "- [ ] Проходит `python -m ci_scripts.dependency_rules --repo-root .`.",
            "",
            "## Проверки для consumer repo",
            "",
            "- [ ] Consumer skeleton не содержит `dlt_pipelines/__init__.py`.",
            "- [ ] После установки package `dltaf` проходят `dltaf manifest lint`, `dltaf contracts test`, `dltaf dags generate`.",
            "- [ ] Helm/Argo конфигурация рендерится без ошибок.",
            "- [ ] Подготовлен branch strategy для cutover consumer CI/CD.",
            "",
            "## Cutover readiness",
            "",
            "- [ ] Framework repo публикует dev artifact под canonical distribution name `dltaf`.",
            "- [ ] Consumer repo умеет использовать опубликованный package вместо исходников framework.",
            "- [ ] Backward compatibility plan для legacy CLI/import paths зафиксирован и понятен команде.",
            "",
            "## После физического split",
            "",
            "- [ ] Проверить stage/dev deploy на package-based consumption.",
            "- [ ] Проверить package smoke и dev artifact flow уже из нового framework repo.",
            "- [ ] Проверить, что docs/CI в обоих репозиториях независимы и не ссылаются на старые пути.",
            "",
        ]
    ) + "\n"


def _render_cutover_branch_strategy() -> str:
    return "\n".join(
        [
            "# Cutover branch strategy",
            "",
            "Этот документ описывает рекомендуемую веточную стратегию для dry-run и физического split на `dltaf` и `dltaf-airflow`.",
            "",
            "## Рекомендуемые ветки в исходном mixed repo",
            "",
            "- `split/framework-bootstrap` — подготовка framework skeleton и CI",
            "- `split/consumer-bootstrap` — подготовка consumer skeleton и CI",
            "- `split/cutover-rehearsal` — dry-run split без merge в production main",
            "- `split/final-cutover` — финальная ветка для freeze window и merge-cutover",
            "",
            "## Dry-run rehearsal",
            "",
            "1. От ветки `main` создать `split/cutover-rehearsal`.",
            "2. Сгенерировать dry-run bundle через `dltaf dev export-cutover-dry-run`.",
            "3. Инициализировать два временных репозитория: `dltaf-rehearsal` и `dltaf-airflow-rehearsal`.",
            "4. Применить move maps и прогнать CI в обоих rehearsal-репозиториях.",
            "5. Зафиксировать найденные проблемы и доработать mixed repo до повторного rehearsal.",
            "",
            "## Финальный cutover",
            "",
            "1. Создать короткое freeze-window для изменений, затрагивающих framework + deploy одновременно.",
            "2. От `main` создать `split/final-cutover`.",
            "3. Экспортировать final dry-run bundle и перенести файлы по exact move maps.",
            "4. Открыть MR/PR в новый repo `dltaf` и в новый repo `dltaf-airflow`.",
            "5. Сначала смержить framework repo, дождаться публикации package `dltaf`.",
            "6. Затем смержить consumer repo, зафиксировав package version / package index settings.",
            "",
            "## Rollback strategy",
            "",
            "- Пока cutover не завершён, `main` mixed repo остаётся источником истины.",
            "- Если rehearsal/cutover ломается, откат — это отказ от merge в новые repo и возврат к mixed repo branch.",
            "- После успешного cutover mixed repo переводится в maintenance-only режим.",
            "",
        ]
    ) + "\n"


def _render_framework_ci_checklist() -> str:
    return "\n".join(
        [
            "# Framework CI/CD migration checklist",
            "",
            "Эта памятка относится к будущему репозиторию `dltaf`.",
            "",
            "## Сохранить/перенести",
            "",
            "- `pre_commit`",
            "- `unit_tests`",
            "- `package_smoke`",
            "- `dependency_rules`",
            "- `docs_checks`",
            "- `build_dev_artifact`",
            "- `publish_dev_artifact`",
            "- `dev_registry_smoke`",
            "- security jobs (`detect_secrets`, `gitleaks_history`)",
            "",
            "## Проверить после split",
            "",
            "- [ ] package name остаётся `dltaf`",
            "- [ ] `python -m build` green",
            "- [ ] `python -m ci_scripts.package_smoke` green",
            "- [ ] docs checks не зависят от consumer repo",
            "- [ ] dev artifact flow публикует package из framework repo, а не из mixed repo",
            "",
            "## Что можно удалить после split",
            "",
            "- consumer-specific deploy jobs",
            "- references на `airflow/`, `argocd/`, `dags/`, consumer manifests и SQL",
            "",
        ]
    ) + "\n"


def _render_consumer_ci_checklist() -> str:
    return "\n".join(
        [
            "# Consumer CI/CD migration checklist",
            "",
            "Эта памятка относится к будущему репозиторию `dltaf-airflow`.",
            "",
            "## Добавить",
            "",
            "- job установки published package `dltaf`",
            "- `dltaf manifest lint --manifests-dir dlt_pipelines/manifests`",
            "- `dltaf contracts test --manifests-dir dlt_pipelines/manifests --output - --format json`",
            "- `dltaf dags generate --manifests-dir dlt_pipelines/manifests --out-dir dags --clean`",
            "- `helm lint` / `helm template` для `airflow/`",
            "- проверки `argocd/` манифестов и values overrides",
            "",
            "## Проверить после split",
            "",
            "- [ ] consumer repo не зависит от исходников framework рядом на диске",
            "- [ ] все команды идут через установленный package `dltaf`",
            "- [ ] values поддерживают package version / package index URL",
            "- [ ] dev flow умеет потреблять `0.x.dev+<sha>` без release tags",
            "",
            "## Что убрать из consumer repo",
            "",
            "- framework source directories (`dltaf/`, `dlt_utils/`, `ci_scripts/` и т.п.)",
            "- framework package smoke / docs generation jobs",
            "",
        ]
    ) + "\n"


def _render_framework_ci_example() -> str:
    return "\n".join(
        [
            "# Пример `.gitlab-ci.yml` для будущего repo `dltaf`",
            "",
            "stages:",
            "  - lint",
            "  - test",
            "  - package",
            "  - publish",
            "",
            "pre_commit:",
            "  stage: lint",
            "  script:",
            "    - pip install -r requirements-dev.txt",
            "    - pre-commit run --all-files --show-diff-on-failure",
            "",
            "unit_tests:",
            "  stage: test",
            "  script:",
            "    - pip install -r requirements-dev.txt",
            "    - pytest -q",
            "",
            "package_smoke:",
            "  stage: package",
            "  script:",
            "    - pip install -r requirements-dev.txt",
            "    - python -m build",
            "    - python -m ci_scripts.package_smoke",
            "",
            "build_dev_artifact:",
            "  stage: package",
            "  script:",
            "    - pip install -r requirements-dev.txt",
            "    - python -m ci_scripts.dev_package build",
            "",
        ]
    ) + "\n"


def _render_consumer_ci_example() -> str:
    return "\n".join(
        [
            "# Пример `.gitlab-ci.yml` для будущего repo `dltaf-airflow`",
            "",
            "stages:",
            "  - validate",
            "  - render",
            "  - deploy",
            "",
            "variables:",
            "  DLTAF_PACKAGE_VERSION: \"0.1.0\"",
            "",
            "manifest_lint:",
            "  stage: validate",
            "  script:",
            "    - pip install \"dltaf==${DLTAF_PACKAGE_VERSION}\"",
            "    - dltaf manifest lint --manifests-dir dlt_pipelines/manifests",
            "",
            "contract_tests:",
            "  stage: validate",
            "  script:",
            "    - pip install \"dltaf==${DLTAF_PACKAGE_VERSION}\"",
            "    - dltaf contracts test --manifests-dir dlt_pipelines/manifests --output - --format json",
            "",
            "generate_dags:",
            "  stage: render",
            "  script:",
            "    - pip install \"dltaf==${DLTAF_PACKAGE_VERSION}\"",
            "    - dltaf dags generate --manifests-dir dlt_pipelines/manifests --out-dir dags --clean",
            "",
            "helm_template:",
            "  stage: render",
            "  script:",
            "    - helm template airflow airflow/ -f airflow/values.yaml > /tmp/airflow-rendered.yaml",
            "",
        ]
    ) + "\n"


def _write_cutover_support_docs(out_dir: Path, inventory: RepoSplitInventory) -> None:
    (out_dir / "DRY_RUN_CUTOVER_CHECKLIST.md").write_text(
        _render_cutover_checklist(inventory),
        encoding="utf-8",
    )
    (out_dir / "CUTOVER_BRANCH_STRATEGY.md").write_text(
        _render_cutover_branch_strategy(),
        encoding="utf-8",
    )
    (out_dir / "FRAMEWORK_CI_MIGRATION_CHECKLIST.md").write_text(
        _render_framework_ci_checklist(),
        encoding="utf-8",
    )
    (out_dir / "CONSUMER_CI_MIGRATION_CHECKLIST.md").write_text(
        _render_consumer_ci_checklist(),
        encoding="utf-8",
    )
    (out_dir / "FRAMEWORK_GITLAB_CI.example.yml").write_text(
        _render_framework_ci_example(),
        encoding="utf-8",
    )
    (out_dir / "CONSUMER_GITLAB_CI.example.yml").write_text(
        _render_consumer_ci_example(),
        encoding="utf-8",
    )


def _write_framework_bootstrap_docs(out_dir: Path) -> None:
    (out_dir / "CUTOVER_BRANCH_STRATEGY.md").write_text(
        _render_cutover_branch_strategy(),
        encoding="utf-8",
    )
    (out_dir / "CI_MIGRATION_CHECKLIST.md").write_text(
        _render_framework_ci_checklist(),
        encoding="utf-8",
    )
    (out_dir / ".gitlab-ci.split.example.yml").write_text(
        _render_framework_ci_example(),
        encoding="utf-8",
    )


def _write_consumer_bootstrap_docs(out_dir: Path) -> None:
    (out_dir / "CUTOVER_BRANCH_STRATEGY.md").write_text(
        _render_cutover_branch_strategy(),
        encoding="utf-8",
    )
    (out_dir / "CI_MIGRATION_CHECKLIST.md").write_text(
        _render_consumer_ci_checklist(),
        encoding="utf-8",
    )
    (out_dir / ".gitlab-ci.split.example.yml").write_text(
        _render_consumer_ci_example(),
        encoding="utf-8",
    )


class RepoSplitService:
    def __init__(self, *, ctx: AppContext, logger) -> None:
        self.ctx = ctx
        self.logger = logger

    def inventory(self, args: argparse.Namespace) -> int:
        inventory = build_inventory(self.ctx.repo_root)
        if args.format == "json":
            content = json.dumps(inventory.as_json(), ensure_ascii=False, indent=2) + "\n"
        else:
            content = inventory.render_markdown()
        if args.out:
            out = Path(args.out).expanduser()
            out.parent.mkdir(parents=True, exist_ok=True)
            out.write_text(content, encoding="utf-8")
            self.logger.info("Repo split inventory written: %s", out)
        else:
            print(content, end="")
        return 0

    def export_framework_skeleton(self, args: argparse.Namespace) -> int:
        out_dir = Path(args.out_dir).expanduser().resolve()
        if out_dir.exists() and args.clean:
            shutil.rmtree(out_dir)
        out_dir.mkdir(parents=True, exist_ok=True)

        inventory = build_inventory(self.ctx.repo_root)
        _copy_files(self.ctx.repo_root, out_dir, inventory.framework_files)
        _write_shared_reports(out_dir, inventory)
        _write_move_maps(out_dir, inventory)
        _write_layouts(out_dir, inventory)
        _write_cutover_support_docs(out_dir, inventory)
        (out_dir / "README_SPLIT_SKELETON.md").write_text(
            _render_framework_skeleton_readme(),
            encoding="utf-8",
        )
        _write_framework_bootstrap_docs(out_dir)
        compat_note = out_dir / "dlt_pipelines" / "README_COMPAT_PACKAGE.md"
        compat_note.parent.mkdir(parents=True, exist_ok=True)
        compat_note.write_text(
            "\n".join(
                [
                    "# Compatibility package root",
                    "",
                    "`dlt_pipelines/__init__.py` остаётся в framework repo только как compatibility package root.",
                    "",
                    "Он нужен historical wrapper-модулям, которые ещё поддерживаются во время deprecation window.",
                    "Consumer repo `dltaf-airflow` не должен копировать этот файл.",
                    "",
                ]
            ) + "\n",
            encoding="utf-8",
        )

        if inventory.unresolved_files:
            self.logger.warning(
                "Framework skeleton exported with %d unresolved files; see %s",
                len(inventory.unresolved_files),
                out_dir / "REPO_SPLIT_INVENTORY.md",
            )
        else:
            self.logger.info("Framework skeleton exported: %s", out_dir)
        return 0

    def export_consumer_skeleton(self, args: argparse.Namespace) -> int:
        out_dir = Path(args.out_dir).expanduser().resolve()
        if out_dir.exists() and args.clean:
            shutil.rmtree(out_dir)
        out_dir.mkdir(parents=True, exist_ok=True)

        inventory = build_inventory(self.ctx.repo_root)
        _copy_files(self.ctx.repo_root, out_dir, inventory.consumer_files)

        for rel_dir in CONSUMER_REQUIRED_DIRS:
            path = out_dir / rel_dir
            path.mkdir(parents=True, exist_ok=True)
            keep = path / ".gitkeep"
            if not any(path.iterdir()):
                keep.write_text("", encoding="utf-8")

        _write_shared_reports(out_dir, inventory)
        _write_move_maps(out_dir, inventory)
        _write_layouts(out_dir, inventory)
        _write_cutover_support_docs(out_dir, inventory)
        (out_dir / "README_SPLIT_SKELETON.md").write_text(
            _render_consumer_skeleton_readme(),
            encoding="utf-8",
        )
        _write_consumer_bootstrap_docs(out_dir)
        if (out_dir / "dlt_pipelines" / "__init__.py").exists():
            raise RuntimeError("consumer skeleton must not contain dlt_pipelines/__init__.py")

        unresolved_note = out_dir / "SPLIT_NEXT_STEPS.md"
        unresolved_note.write_text(
            "\n".join(
                [
                    "# Next steps for `dltaf-airflow` repo",
                    "",
                    "1. Добавить dependency на опубликованный package `dltaf`.",
                    "2. Перевести DAG wrappers на использование установленного package.",
                    "3. Подключить Helm/Argo values для `dltaf.packageVersion` и package registry URL.",
                    "4. Использовать `CONSUMER_MOVE_MAP.md` и `DRY_RUN_CUTOVER_CHECKLIST.md` как cutover bundle.",
                    "",
                ]
            ) + "\n",
            encoding="utf-8",
        )

        if inventory.unresolved_files:
            self.logger.warning(
                "Consumer skeleton exported with %d unresolved files in shared inventory; see %s",
                len(inventory.unresolved_files),
                out_dir / "REPO_SPLIT_INVENTORY.md",
            )
        else:
            self.logger.info("Consumer skeleton exported: %s", out_dir)
        return 0

    def export_repo_split_plan(self, args: argparse.Namespace) -> int:
        out_dir = Path(args.out_dir).expanduser().resolve()
        if out_dir.exists() and args.clean:
            shutil.rmtree(out_dir)
        out_dir.mkdir(parents=True, exist_ok=True)

        inventory = build_inventory(self.ctx.repo_root)
        _write_shared_reports(out_dir, inventory)
        _write_move_maps(out_dir, inventory)
        _write_layouts(out_dir, inventory)
        _write_cutover_support_docs(out_dir, inventory)
        (out_dir / "README.md").write_text(
            "\n".join(
                [
                    "# Repo split plan bundle",
                    "",
                    "Этот каталог содержит exact file move maps, proposed repo layouts и dry-run cutover checklist.",
                    "",
                    "Содержимое:",
                    "- `REPO_SPLIT_INVENTORY.md` / `repo_split_inventory.json`",
                    "- `FRAMEWORK_MOVE_MAP.md` / `framework_move_map.json`",
                    "- `CONSUMER_MOVE_MAP.md` / `consumer_move_map.json`",
                    "- `FRAMEWORK_REPO_LAYOUT.md`",
                    "- `CONSUMER_REPO_LAYOUT.md`",
                    "- `DRY_RUN_CUTOVER_CHECKLIST.md`",
                    "- `CUTOVER_BRANCH_STRATEGY.md`",
                    "- `FRAMEWORK_CI_MIGRATION_CHECKLIST.md`",
                    "- `CONSUMER_CI_MIGRATION_CHECKLIST.md`",
                    "",
                ]
            ) + "\n",
            encoding="utf-8",
        )
        self.logger.info("Repo split plan bundle exported: %s", out_dir)
        return 0

    def export_cutover_dry_run(self, args: argparse.Namespace) -> int:
        out_dir = Path(args.out_dir).expanduser().resolve()
        if out_dir.exists() and args.clean:
            shutil.rmtree(out_dir)
        out_dir.mkdir(parents=True, exist_ok=True)

        inventory = build_inventory(self.ctx.repo_root)
        _write_shared_reports(out_dir, inventory)
        _write_move_maps(out_dir, inventory)
        _write_layouts(out_dir, inventory)
        _write_cutover_support_docs(out_dir, inventory)

        framework_dir = out_dir / "dltaf-repo"
        consumer_dir = out_dir / "dltaf-airflow-repo"

        class _Args:
            clean = True
            out_dir = ""

        fw_args = _Args()
        fw_args.out_dir = str(framework_dir)
        self.export_framework_skeleton(fw_args)

        c_args = _Args()
        c_args.out_dir = str(consumer_dir)
        self.export_consumer_skeleton(c_args)

        (out_dir / "README.md").write_text(
            "\n".join(
                [
                    "# Repo split dry-run bundle",
                    "",
                    "Этот каталог содержит подготовленный dry-run набор для физического разделения mixed repo на два репозитория:",
                    "",
                    "- `dltaf-repo/` — будущий framework repo",
                    "- `dltaf-airflow-repo/` — будущий consumer/deploy repo",
                    "",
                    "Также в корне лежат move maps, layouts, branch strategy и CI migration checklists.",
                    "",
                ]
            ) + "\n",
            encoding="utf-8",
        )
        (out_dir / "CUTOVER_EXECUTION_ORDER.md").write_text(
            "\n".join(
                [
                    "# Cutover execution order",
                    "",
                    "1. Проверить dry-run bundle и exact move maps.",
                    "2. Прогнать CI в `dltaf-repo/` как в будущем framework repo.",
                    "3. Прогнать CI в `dltaf-airflow-repo/` с установленным published package `dltaf`.",
                    "4. Выполнить rehearsal import в двух временных репозиториях.",
                    "5. Повторить rehearsal до зелёного статуса обоих репозиториев.",
                    "6. Только после этого переходить к физическому split и MR/PR в новые репозитории.",
                    "",
                ]
            ) + "\n",
            encoding="utf-8",
        )
        self.logger.info("Repo split dry-run bundle exported: %s", out_dir)
        return 0


    def export_import_rehearsal(self, args: argparse.Namespace) -> int:
        out_dir = Path(args.out_dir).expanduser().resolve()
        if out_dir.exists() and args.clean:
            shutil.rmtree(out_dir)
        out_dir.mkdir(parents=True, exist_ok=True)

        inventory = build_inventory(self.ctx.repo_root)

        class _Args:
            clean = True
            out_dir = ""

        dry_args = _Args()
        dry_args.out_dir = str(out_dir / "cutover-dry-run")
        self.export_cutover_dry_run(dry_args)

        _write_import_rehearsal_bundle(out_dir, inventory)

        self.logger.info("Import rehearsal bundle exported: %s", out_dir)
        return 0


def _render_rehearsal_execution_order() -> str:
    return "\n".join(
        [
            "# Rehearsal execution order",
            "",
            "Этот документ описывает рекомендуемый порядок rehearsal-импорта skeleton'ов в два временных репозитория.",
            "",
            "1. Выполнить `dltaf dev export-import-rehearsal --out-dir build/repo-split-rehearsal --clean`.",
            "2. Из корня bundle выполнить `scripts/10_rehearse_framework_import.sh`.",
            "3. Затем выполнить `scripts/20_rehearse_consumer_import.sh`.",
            "4. После подготовки временных репозиториев выполнить validation scripts для каждого repo.",
            "5. Зафиксировать найденные проблемы и обновить mixed repo перед реальным cutover.",
            "",
        ]
    ) + "\n"


def _render_framework_rehearsal_readme() -> str:
    return "\n".join(
        [
            "# Framework import rehearsal",
            "",
            "Этот документ описывает rehearsal-import будущего framework repo `dltaf`.",
            "",
            "## Шаги",
            "",
            "1. `./scripts/10_rehearse_framework_import.sh [WORK_DIR]`",
            "2. `./scripts/30_validate_framework_rehearsal.sh <WORK_DIR>/dltaf-rehearsal`",
            "",
            "## Ожидаемый результат",
            "",
            "- repo успешно инициализирован как отдельный git checkout",
            "- проходят `pytest -q`, `package_smoke`, docs checks и dependency rules",
            "",
        ]
    ) + "\n"


def _render_consumer_rehearsal_readme() -> str:
    return "\n".join(
        [
            "# Consumer import rehearsal",
            "",
            "Этот документ описывает rehearsal-import будущего consumer repo `dltaf-airflow`.",
            "",
            "## Шаги",
            "",
            "1. `./scripts/20_rehearse_consumer_import.sh [WORK_DIR]`",
            "2. `./scripts/40_validate_consumer_rehearsal.sh <WORK_DIR>/dltaf-airflow-rehearsal <wheel-or-version>`",
            "",
            "## Ожидаемый результат",
            "",
            "- consumer repo не зависит от исходников framework рядом на диске",
            "- все проверки выполняются через установленный package `dltaf`",
            "",
        ]
    ) + "\n"


def _render_framework_import_script() -> str:
    return "\n".join(
        [
            "#!/usr/bin/env bash",
            "set -euo pipefail",
            "",
            'ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"',
            'WORK_DIR="${1:-$ROOT_DIR/work}"',
            'SRC_DIR="$ROOT_DIR/cutover-dry-run/dltaf-repo"',
            'DEST_DIR="$WORK_DIR/dltaf-rehearsal"',
            "",
            'rm -rf "$DEST_DIR"',
            'mkdir -p "$WORK_DIR"',
            'cp -R "$SRC_DIR" "$DEST_DIR"',
            'cd "$DEST_DIR"',
            'git init >/dev/null',
            'git checkout -b split/cutover-rehearsal >/dev/null 2>&1 || git checkout -b split/cutover-rehearsal',
            'git add .',
            'git commit -m "Rehearsal import: dltaf framework skeleton" >/dev/null 2>&1 || true',
            'echo "Framework rehearsal repo prepared at: $DEST_DIR"',
            'echo "Next: scripts/30_validate_framework_rehearsal.sh $DEST_DIR"',
            "",
        ]
    ) + "\n"


def _render_consumer_import_script() -> str:
    return "\n".join(
        [
            "#!/usr/bin/env bash",
            "set -euo pipefail",
            "",
            'ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"',
            'WORK_DIR="${1:-$ROOT_DIR/work}"',
            'SRC_DIR="$ROOT_DIR/cutover-dry-run/dltaf-airflow-repo"',
            'DEST_DIR="$WORK_DIR/dltaf-airflow-rehearsal"',
            "",
            'rm -rf "$DEST_DIR"',
            'mkdir -p "$WORK_DIR"',
            'cp -R "$SRC_DIR" "$DEST_DIR"',
            'cd "$DEST_DIR"',
            'git init >/dev/null',
            'git checkout -b split/cutover-rehearsal >/dev/null 2>&1 || git checkout -b split/cutover-rehearsal',
            'git add .',
            'git commit -m "Rehearsal import: dltaf-airflow consumer skeleton" >/dev/null 2>&1 || true',
            'echo "Consumer rehearsal repo prepared at: $DEST_DIR"',
            'echo "Next: scripts/40_validate_consumer_rehearsal.sh $DEST_DIR <dltaf-wheel-or-version>"',
            "",
        ]
    ) + "\n"


def _render_framework_validation_script() -> str:
    return "\n".join(
        [
            "#!/usr/bin/env bash",
            "set -euo pipefail",
            "",
            'REPO_DIR="${1:-$(pwd)}"',
            'cd "$REPO_DIR"',
            "",
            'python -m pip install -r requirements-dev.txt',
            'python -m pytest -q',
            'python -m ci_scripts.dependency_rules --repo-root .',
            'python -m dltaf.cli --repo-root . docs generate-cli-reference --check --show-diff',
            'python -m dltaf.cli --repo-root . docs update-dev-metrics --check --show-diff',
            'python -m build',
            'python -m ci_scripts.package_smoke',
            "",
        ]
    ) + "\n"


def _render_consumer_validation_script() -> str:
    return "\n".join(
        [
            "#!/usr/bin/env bash",
            "set -euo pipefail",
            "",
            'REPO_DIR="${1:-$(pwd)}"',
            'DLTAF_PACKAGE_REF="${2:-}"',
            'cd "$REPO_DIR"',
            "",
            'python -m pip install -U pip',
            'if [[ -n "$DLTAF_PACKAGE_REF" ]]; then',
            '  python -m pip install "$DLTAF_PACKAGE_REF"',
            'else',
            '  echo "No explicit dltaf package ref passed; attempting plain install from configured indexes..."',
            '  python -m pip install dltaf',
            'fi',
            'dltaf manifest lint --manifests-dir dlt_pipelines/manifests',
            'dltaf contracts test --manifests-dir dlt_pipelines/manifests --output - --format json',
            'dltaf dags generate --manifests-dir dlt_pipelines/manifests --out-dir build/dags-generated --clean',
            'if command -v helm >/dev/null 2>&1; then',
            '  helm lint airflow || true',
            '  helm template dltaf-airflow airflow >/dev/null || true',
            'else',
            '  echo "helm not found; skipping helm lint/template checks"',
            'fi',
            "",
        ]
    ) + "\n"


def _write_import_rehearsal_bundle(out_dir: Path, inventory: RepoSplitInventory) -> None:
    scripts_dir = out_dir / "scripts"
    scripts_dir.mkdir(parents=True, exist_ok=True)

    (out_dir / "README.md").write_text(
        "\n".join(
            [
                "# Import rehearsal bundle",
                "",
                "Этот bundle готовит dry-run импорта skeleton'ов в два временных репозитория.",
                "",
                "Содержимое:",
                "- `cutover-dry-run/` — полный cutover dry-run bundle",
                "- `scripts/10_rehearse_framework_import.sh`",
                "- `scripts/20_rehearse_consumer_import.sh`",
                "- `scripts/30_validate_framework_rehearsal.sh`",
                "- `scripts/40_validate_consumer_rehearsal.sh`",
                "- `REHEARSAL_EXECUTION_ORDER.md`",
                "",
            ]
        ) + "\n",
        encoding="utf-8",
    )
    (out_dir / "REHEARSAL_EXECUTION_ORDER.md").write_text(
        _render_rehearsal_execution_order(),
        encoding="utf-8",
    )
    (out_dir / "FRAMEWORK_IMPORT_REHEARSAL.md").write_text(
        _render_framework_rehearsal_readme(),
        encoding="utf-8",
    )
    (out_dir / "CONSUMER_IMPORT_REHEARSAL.md").write_text(
        _render_consumer_rehearsal_readme(),
        encoding="utf-8",
    )

    scripts = {
        "10_rehearse_framework_import.sh": _render_framework_import_script(),
        "20_rehearse_consumer_import.sh": _render_consumer_import_script(),
        "30_validate_framework_rehearsal.sh": _render_framework_validation_script(),
        "40_validate_consumer_rehearsal.sh": _render_consumer_validation_script(),
    }
    for name, content in scripts.items():
        path = scripts_dir / name
        path.write_text(content, encoding="utf-8")
        path.chmod(0o755)

    for repo_name, note_name in (
        ("dltaf-repo", "FRAMEWORK_IMPORT_REHEARSAL.md"),
        ("dltaf-airflow-repo", "CONSUMER_IMPORT_REHEARSAL.md"),
    ):
        repo_dir = out_dir / "cutover-dry-run" / repo_name
        if repo_dir.exists():
            local_scripts = repo_dir / "scripts"
            local_scripts.mkdir(parents=True, exist_ok=True)
            for name, content in scripts.items():
                target = local_scripts / name
                target.write_text(content, encoding="utf-8")
                target.chmod(0o755)
            (repo_dir / "REHEARSAL_IMPORT.md").write_text(
                (out_dir / note_name).read_text(encoding="utf-8"),
                encoding="utf-8",
            )


__all__ = [
    "RepoMoveMap",
    "RepoMoveMapEntry",
    "RepoSplitInventory",
    "RepoSplitService",
    "build_consumer_move_map",
    "build_framework_move_map",
    "build_inventory",
]
