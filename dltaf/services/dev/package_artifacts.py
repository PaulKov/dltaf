from __future__ import annotations

import argparse
import json
import os
import re
import shutil
import subprocess
import sys
import tempfile
import textwrap
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from urllib.parse import quote

try:  # pragma: no cover - import path depends on interpreter version
    import tomllib  # type: ignore[attr-defined]
except ModuleNotFoundError:  # pragma: no cover - Python < 3.11
    tomllib = None  # type: ignore[assignment]

PACKAGE_NAME = "dltaf"


@dataclass(frozen=True)
class DevPackageMetadata:
    package_name: str
    version: str
    base_version: str
    commit_sha: str
    short_sha: str
    ref_name: str
    ref_slug: str
    build_number: str
    repository_upload_url: str
    simple_index_url: str
    wheel_filename: str | None = None
    sdist_filename: str | None = None

    def as_json(self) -> dict[str, Any]:
        return {
            "package_name": self.package_name,
            "version": self.version,
            "base_version": self.base_version,
            "commit_sha": self.commit_sha,
            "short_sha": self.short_sha,
            "ref_name": self.ref_name,
            "ref_slug": self.ref_slug,
            "build_number": self.build_number,
            "repository_upload_url": self.repository_upload_url,
            "simple_index_url": self.simple_index_url,
            "wheel_filename": self.wheel_filename,
            "sdist_filename": self.sdist_filename,
        }


def _read_pyproject_version(pyproject_path: Path) -> str:
    text = pyproject_path.read_text(encoding="utf-8")
    version = ""
    if tomllib is not None:
        data = tomllib.loads(text)
        project = data.get("project") or {}
        version = str(project.get("version") or "").strip()
    else:
        match = re.search(r'(?m)^version\s*=\s*"([^"]+)"\s*$', text)
        if match:
            version = str(match.group(1)).strip()
    if not version:
        raise ValueError(f"Version is missing in {pyproject_path}")
    return version


def _patch_pyproject_version(text: str, version: str) -> str:
    pattern = re.compile(r'(?m)^(version\s*=\s*")([^"]+)(")$')
    updated, count = pattern.subn(rf'\g<1>{version}\g<3>', text, count=1)
    if count != 1:
        raise ValueError("Failed to patch project version in pyproject.toml")
    return updated


def _sanitize_local_label(value: str, *, fallback: str = "local", max_len: int = 24) -> str:
    normalized = re.sub(r"[^A-Za-z0-9]+", ".", value or "").strip(".").lower()
    if not normalized:
        normalized = fallback
    parts = [part for part in normalized.split(".") if part]
    if not parts:
        parts = [fallback]
    joined = ".".join(parts)
    return joined[:max_len].rstrip(".") or fallback


def _gitlab_upload_url() -> str:
    api = (os.getenv("CI_API_V4_URL") or "").rstrip("/")
    project_id = (os.getenv("CI_PROJECT_ID") or "").strip()
    if api and project_id:
        return f"{api}/projects/{project_id}/packages/pypi"
    return ""


def _gitlab_simple_index_url() -> str:
    host = (os.getenv("CI_SERVER_HOST") or "").strip()
    project_id = (os.getenv("CI_PROJECT_ID") or "").strip()
    if host and project_id:
        return f"https://{host}/api/v4/projects/{project_id}/packages/pypi/simple"
    api = (os.getenv("CI_API_V4_URL") or "").rstrip("/")
    if api and project_id:
        return f"{api}/projects/{project_id}/packages/pypi/simple"
    return ""


def compute_dev_version(
    base_version: str,
    *,
    commit_sha: str | None = None,
    build_number: str | None = None,
    ref_slug: str | None = None,
) -> str:
    sha = _sanitize_local_label((commit_sha or "local")[:12], fallback="local", max_len=12)
    ref_source = ref_slug if ref_slug is not None else (os.getenv("CI_COMMIT_REF_SLUG") or "")
    ref = _sanitize_local_label(ref_source, fallback="branch", max_len=20)
    build = str(
        build_number
        or os.getenv("CI_PIPELINE_IID")
        or os.getenv("CI_PIPELINE_ID")
        or datetime.now(timezone.utc).strftime("%Y%m%d%H%M%S")
    )
    build_digits = re.sub(r"\D+", "", build) or "0"
    return f"{base_version}.dev{build_digits}+g{sha}.{ref}"


class DevPackageService:
    def __init__(self, *, ctx, logger) -> None:
        self.ctx = ctx
        self.logger = logger

    def _run(self, *args: str, cwd: Path | None = None, env: dict[str, str] | None = None) -> None:
        merged_env = dict(os.environ)
        merged_env.setdefault("PYTHONHASHSEED", "0")
        if env:
            merged_env.update(env)
        proc = subprocess.run(
            list(args),
            cwd=str(cwd or self.ctx.repo_root),
            text=True,
            capture_output=True,
            env=merged_env,
        )
        if proc.returncode != 0:
            raise RuntimeError(
                f"Command failed: {' '.join(args)}\nstdout:\n{proc.stdout}\nstderr:\n{proc.stderr}"
            )

    def _ignore_copy(self, _src: str, names: list[str]) -> set[str]:
        ignored = {
            ".git",
            ".pytest_cache",
            ".mypy_cache",
            ".ruff_cache",
            "__pycache__",
            "build",
            "dist",
            "dist-dev",
            ".venv",
            ".venv-smoke",
            ".venv-smoke-installed",
        }
        return {name for name in names if name in ignored or name.endswith(".pyc")}

    def build_metadata(self, *, version: str | None = None) -> DevPackageMetadata:
        pyproject = self.ctx.repo_root / "pyproject.toml"
        base_version = _read_pyproject_version(pyproject)
        commit_sha = (os.getenv("CI_COMMIT_SHA") or "local").strip()
        short_sha = commit_sha[:8] if commit_sha else "local"
        ref_name = os.getenv("CI_COMMIT_REF_NAME") or os.getenv("GIT_BRANCH") or "local"
        ref_slug = os.getenv("CI_COMMIT_REF_SLUG") or _sanitize_local_label(ref_name, fallback="local", max_len=20)
        build_no = str(os.getenv("CI_PIPELINE_IID") or os.getenv("CI_PIPELINE_ID") or "0")
        dev_version = version or compute_dev_version(
            base_version,
            commit_sha=commit_sha,
            build_number=build_no if build_no != "0" else None,
            ref_slug=ref_slug,
        )
        return DevPackageMetadata(
            package_name=PACKAGE_NAME,
            version=dev_version,
            base_version=base_version,
            commit_sha=commit_sha or "local",
            short_sha=short_sha or "local",
            ref_name=str(ref_name),
            ref_slug=str(ref_slug),
            build_number=build_no,
            repository_upload_url=_gitlab_upload_url(),
            simple_index_url=_gitlab_simple_index_url(),
        )

    def _write_env_file(self, path: Path, meta: DevPackageMetadata) -> None:
        lines = [
            f"DLTAF_DEV_PACKAGE_NAME={meta.package_name}",
            f"DLTAF_DEV_PACKAGE_VERSION={meta.version}",
            f"DLTAF_DEV_PACKAGE_BASE_VERSION={meta.base_version}",
            f"DLTAF_DEV_PACKAGE_SHA={meta.commit_sha}",
            f"DLTAF_DEV_PACKAGE_SHORT_SHA={meta.short_sha}",
            f"DLTAF_DEV_PACKAGE_REF_NAME={meta.ref_name}",
            f"DLTAF_DEV_PACKAGE_REF_SLUG={meta.ref_slug}",
            f"DLTAF_DEV_PACKAGE_BUILD_NUMBER={meta.build_number}",
            f"DLTAF_DEV_PACKAGE_UPLOAD_URL={meta.repository_upload_url}",
            f"DLTAF_DEV_PACKAGE_SIMPLE_INDEX_URL={meta.simple_index_url}",
            f"DLTAF_DEV_PACKAGE_WHEEL_FILENAME={meta.wheel_filename or ''}",
            f"DLTAF_DEV_PACKAGE_SDIST_FILENAME={meta.sdist_filename or ''}",
        ]
        path.write_text("\n".join(lines) + "\n", encoding="utf-8")

    def _render_bundle(self, out_dir: Path, meta: DevPackageMetadata) -> None:
        out_dir.mkdir(parents=True, exist_ok=True)
        install_md = textwrap.dedent(
            f"""
            # Dev package bundle

            Package: `{meta.package_name}`
            Version: `{meta.version}`

            This bundle is intended for **dev-only** validation of framework changes
            without creating a release tag.

            ## Recommended flow

            1. Build the dev artifact in CI (`build_dev_artifact`).
            2. Publish it to the project package registry (`publish_dev_artifact`).
            3. Install it in dev Airflow / Argo using a **read-only deploy token**
               (not a release tag).
            4. Run smoke checks in dev using the same commit-based package version.

            ## Canonical pip install example

            ```bash
            pip install \
              --extra-index-url "https://__PYPI_USER__:__PYPI_PASSWORD__@{meta.simple_index_url.replace('https://', '')}" \
              "{meta.package_name}=={meta.version}"
            ```

            ## Notes

            - The package version is derived from the base project version plus `dev` + commit metadata.
            - Prefer a dedicated **deploy token** for dev environments instead of `CI_JOB_TOKEN`.
            - The same version can be promoted across multiple dev smoke runs without creating release tags.
            """
        ).strip() + "\n"
        (out_dir / "README.md").write_text(install_md, encoding="utf-8")

        values_yaml = textwrap.dedent(
            f"""
            # Example Helm/values override for dev-only framework validation
            dltaf:
              package:
                name: "{meta.package_name}"
                version: "{meta.version}"
                extraIndexUrl: "https://__PYPI_USER__:__PYPI_PASSWORD__@{meta.simple_index_url.replace('https://', '')}"
            """
        ).lstrip()
        (out_dir / "values.dltaf-dev.example.yaml").write_text(values_yaml, encoding="utf-8")

        pip_conf = textwrap.dedent(
            f"""
            [global]
            extra-index-url = https://__PYPI_USER__:__PYPI_PASSWORD__@{meta.simple_index_url.replace('https://', '')}
            """
        ).lstrip()
        (out_dir / "pip.conf.example").write_text(pip_conf, encoding="utf-8")

    def build(self, args: argparse.Namespace) -> int:
        dist_dir = Path(self.ctx.repo_root, str(args.dist_dir)).resolve()
        dist_dir.mkdir(parents=True, exist_ok=True)
        meta = self.build_metadata(version=getattr(args, "version", None))

        with tempfile.TemporaryDirectory(prefix="dltaf-dev-build-") as tmp:
            tmp_root = Path(tmp)
            src_dir = tmp_root / "src"
            shutil.copytree(self.ctx.repo_root, src_dir, ignore=self._ignore_copy)
            pyproject = src_dir / "pyproject.toml"
            pyproject.write_text(
                _patch_pyproject_version(pyproject.read_text(encoding="utf-8"), meta.version),
                encoding="utf-8",
            )
            self._run(sys.executable, "-m", "build", "--outdir", str(dist_dir), cwd=src_dir)

        wheels = sorted(dist_dir.glob("*.whl"))
        sdists = sorted(dist_dir.glob("*.tar.gz"))
        wheel_name = wheels[-1].name if wheels else None
        sdist_name = sdists[-1].name if sdists else None
        final_meta = DevPackageMetadata(**{**meta.as_json(), "wheel_filename": wheel_name, "sdist_filename": sdist_name})

        env_path = Path(self.ctx.repo_root, str(args.env_file)).resolve() if getattr(args, "env_file", None) else dist_dir / "dev_package.env"
        json_path = Path(self.ctx.repo_root, str(args.json_file)).resolve() if getattr(args, "json_file", None) else dist_dir / "dev_package.json"
        bundle_dir = Path(self.ctx.repo_root, str(args.bundle_dir)).resolve() if getattr(args, "bundle_dir", None) else dist_dir / "dev_bundle"

        self._write_env_file(env_path, final_meta)
        json_path.write_text(json.dumps(final_meta.as_json(), ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
        self._render_bundle(bundle_dir, final_meta)

        self.logger.info("Dev package built: %s", final_meta.version)
        self.logger.info("Artifacts: %s, %s, %s", env_path, json_path, bundle_dir)
        return 0

    def publish(self, args: argparse.Namespace) -> int:
        dist_dir = Path(self.ctx.repo_root, str(args.dist_dir)).resolve()
        repository_url = str(args.repository_url or _gitlab_upload_url()).strip()
        username = str(args.username or os.getenv("TWINE_USERNAME") or "gitlab-ci-token").strip()
        password = str(args.password or os.getenv("TWINE_PASSWORD") or os.getenv("CI_JOB_TOKEN") or "").strip()

        if not repository_url:
            raise SystemExit("Repository URL is required. Pass --repository-url or provide CI_API_V4_URL/CI_PROJECT_ID.")
        if not password:
            raise SystemExit("Password/token is required. Pass --password or provide TWINE_PASSWORD/CI_JOB_TOKEN.")

        files = [str(p) for p in sorted(dist_dir.glob("*")) if p.suffix in {".whl", ".gz"}]
        if not files:
            raise SystemExit(f"No dist files found in {dist_dir}")

        self._run(
            sys.executable,
            "-m",
            "twine",
            "upload",
            "--non-interactive",
            "--skip-existing",
            "--repository-url",
            repository_url,
            "-u",
            username,
            "-p",
            password,
            *files,
        )
        self.logger.info("Published dev package(s) to %s", repository_url)
        return 0

    def smoke_installed(self, args: argparse.Namespace) -> int:
        package_name = str(args.package_name or os.getenv("DLTAF_DEV_PACKAGE_NAME") or PACKAGE_NAME).strip()
        version = str(args.version or os.getenv("DLTAF_DEV_PACKAGE_VERSION") or "").strip()
        simple_url = str(args.simple_index_url or os.getenv("DLTAF_DEV_PACKAGE_SIMPLE_INDEX_URL") or "").strip()
        username = str(args.username or os.getenv("DLTAF_DEV_PYPI_USERNAME") or "gitlab-ci-token").strip()
        password = str(args.password or os.getenv("DLTAF_DEV_PYPI_PASSWORD") or os.getenv("CI_JOB_TOKEN") or "").strip()

        if not version:
            raise SystemExit("Package version is required. Pass --version or provide DLTAF_DEV_PACKAGE_VERSION.")
        if not simple_url:
            raise SystemExit("Simple index URL is required. Pass --simple-index-url or provide DLTAF_DEV_PACKAGE_SIMPLE_INDEX_URL.")
        if not password:
            raise SystemExit("Package registry password/token is required.")

        auth_url = re.sub(r"^https://", f"https://{quote(username)}:{quote(password)}@", simple_url)
        with tempfile.TemporaryDirectory(prefix="dltaf-dev-smoke-") as tmp:
            tmpdir = Path(tmp)
            venv = tmpdir / "venv"
            python_bin = venv / "bin" / "python"
            self._run(sys.executable, "-m", "venv", str(venv))
            self._run(str(python_bin), "-m", "pip", "install", "--upgrade", "pip")
            self._run(
                str(python_bin),
                "-m",
                "pip",
                "install",
                "--extra-index-url",
                auth_url,
                f"{package_name}=={version}",
            )
            self._run(str(python_bin), "-m", "ci_scripts.package_smoke")
        self.logger.info("Installed-package smoke passed for %s==%s", package_name, version)
        return 0
