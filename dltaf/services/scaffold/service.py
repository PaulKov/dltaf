from __future__ import annotations

import argparse
from pathlib import Path
from typing import Optional, Sequence

from .models import ALLOWED_TEMPLATES, ScaffoldArtifacts, ScaffoldOptions
from .naming import normalize_slug
from .pyproject import ensure_package_in_pyproject
from .templates import (
    render_contract_sample,
    render_contract_schema,
    render_infra_checks_stub,
    render_init_py,
    render_integration_readme,
    render_manifest_yaml,
    render_runner_plugin,
    render_source_api_kafka_json,
    render_source_http_pull,
    render_source_noop,
)

SQL_FAMILY_RESERVED_KINDS = {"sqldb", "sql_database", "oracle_custom_sql", "oracle"}


def write_file(path: Path, content: str, *, force: bool) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if path.exists() and not force:
        raise FileExistsError(f"Refusing to overwrite existing file: {path}")
    path.write_text(content, encoding="utf-8")


class ScaffoldIntegrationService:
    def __init__(self, *, logger) -> None:
        self.logger = logger

    @staticmethod
    def build_parser(prog: Optional[str] = None) -> argparse.ArgumentParser:
        parser = argparse.ArgumentParser(description="Scaffold a new dlt integration", prog=prog)
        parser.add_argument("--pipeline-name", required=True, help="Pipeline name (also manifest filename, without .yaml)")
        parser.add_argument("--source-kind", required=True, help="source.kind value for the integration")
        parser.add_argument("--package", default=None, help="Python package name under dlt_pipelines/ (default: <source_kind>__pipeline)")
        parser.add_argument("--template", default="api_kafka_json", choices=sorted(ALLOWED_TEMPLATES), help="Scaffold template")
        parser.add_argument("--destination", default="clickhouse", help="Default pipeline.destination")
        parser.add_argument("--dataset", default="raw", help="Default pipeline.dataset")
        parser.add_argument("--schedule", default="0 6 * * *", help="Default Airflow schedule cron")
        parser.add_argument("--root-dir", default=".", help="Repository root (default: current directory)")
        parser.add_argument("--no-update-pyproject", action="store_true", help="Do not update pyproject.toml [tool.setuptools].packages")
        parser.add_argument("--force", action="store_true", help="Overwrite existing files")
        return parser

    def normalize_options(self, args: argparse.Namespace) -> ScaffoldOptions:
        pipeline_name = str(args.pipeline_name).strip()
        if not pipeline_name:
            raise SystemExit("--pipeline-name is required")
        source_kind = normalize_slug(args.source_kind)
        if not source_kind:
            raise SystemExit("--source-kind must be a non-empty identifier")
        if source_kind in SQL_FAMILY_RESERVED_KINDS:
            raise SystemExit(
                "Built-in SQL family manifests should use the canonical `sqldb` model. "
                "Do not scaffold them as plugin integrations. Use "
                "`dltaf manifest doctor --template-kind sqldb_catalog` or "
                "`dltaf manifest doctor --template-kind sqldb_query` instead."
            )
        package = args.package
        if package is None:
            package = f"{source_kind}__pipeline"
        package_name = normalize_slug(package)
        if not package_name:
            raise SystemExit("--package must produce a non-empty python package name")
        if not package_name.endswith("__pipeline"):
            package_name = f"{package_name}__pipeline"
        return ScaffoldOptions(
            root_dir=Path(args.root_dir).resolve(),
            pipeline_name=pipeline_name,
            source_kind=source_kind,
            package_name=package_name,
            template=str(args.template),
            destination=str(args.destination),
            dataset=str(args.dataset),
            schedule=str(args.schedule),
            update_pyproject=not bool(args.no_update_pyproject),
            force=bool(args.force),
        )

    def scaffold(self, opts: ScaffoldOptions) -> ScaffoldArtifacts:
        if opts.template not in ALLOWED_TEMPLATES:
            raise ValueError(f"Unknown template: {opts.template}. Allowed: {sorted(ALLOWED_TEMPLATES)}")
        pipelines_dir = opts.root_dir / "dlt_pipelines"
        manifests_dir = pipelines_dir / "manifests"
        integration_dir = pipelines_dir / opts.package_name
        source_file = integration_dir / f"{opts.source_kind}_source.py"
        runner_plugin_file = integration_dir / "runner_plugin.py"
        infra_checks_file = integration_dir / "infra_checks.py"
        readme_file = integration_dir / "README.md"
        init_file = integration_dir / "__init__.py"
        contracts_dir = integration_dir / "contracts"
        contract_schema_file = contracts_dir / "payload.schema.json"
        contract_sample_file = contracts_dir / "payload.sample.json"
        manifest_file = manifests_dir / f"{opts.pipeline_name}.yaml"

        source_module = f"dlt_pipelines.{opts.package_name}.{opts.source_kind}_source"
        runner_plugin_module = f"dlt_pipelines.{opts.package_name}.runner_plugin"
        infra_checks_module = f"dlt_pipelines.{opts.package_name}.infra_checks"

        if opts.template == "api_kafka_json":
            source_py = render_source_api_kafka_json(source_kind=opts.source_kind)
        elif opts.template == "http_pull":
            source_py = render_source_http_pull(source_kind=opts.source_kind)
        else:
            source_py = render_source_noop(source_kind=opts.source_kind)

        runner_py = render_runner_plugin(source_kind=opts.source_kind, source_module=source_module, source_factory=f"{opts.source_kind}_source")
        infra_py = render_infra_checks_stub(source_kind=opts.source_kind)
        manifest_yaml = render_manifest_yaml(
            pipeline_name=opts.pipeline_name,
            source_kind=opts.source_kind,
            package_name=opts.package_name,
            destination=opts.destination,
            dataset=opts.dataset,
            schedule=opts.schedule,
            runner_plugin_module=runner_plugin_module,
            infra_checks_module=infra_checks_module,
        )
        readme_md = render_integration_readme(pipeline_name=opts.pipeline_name, source_kind=opts.source_kind, package_name=opts.package_name, template=opts.template)

        write_file(init_file, render_init_py(), force=opts.force)
        write_file(source_file, source_py, force=opts.force)
        write_file(runner_plugin_file, runner_py, force=opts.force)
        write_file(infra_checks_file, infra_py, force=opts.force)
        write_file(readme_file, readme_md, force=opts.force)
        write_file(contract_schema_file, render_contract_schema(source_kind=opts.source_kind), force=opts.force)
        write_file(contract_sample_file, render_contract_sample(), force=opts.force)
        write_file(manifest_file, manifest_yaml, force=opts.force)

        pyproject_path: Optional[Path] = None
        if opts.update_pyproject:
            candidate = opts.root_dir / "pyproject.toml"
            if ensure_package_in_pyproject(candidate, f"dlt_pipelines.{opts.package_name}"):
                pyproject_path = candidate
        return ScaffoldArtifacts(integration_dir=integration_dir, manifest_path=manifest_file, pyproject_path=pyproject_path)

    def run(self, args: argparse.Namespace) -> int:
        opts = self.normalize_options(args)
        artifacts = self.scaffold(opts)
        print(f"Scaffold created: {artifacts.integration_dir}")
        print(f"Manifest created: {artifacts.manifest_path}")
        if artifacts.pyproject_path is not None and artifacts.pyproject_path.exists():
            print("pyproject.toml updated (added dlt_pipelines subpackage)")
        return 0


def scaffold_integration(opts: ScaffoldOptions) -> ScaffoldArtifacts:
    import logging
    return ScaffoldIntegrationService(logger=logging.getLogger("dlt.scaffold")).scaffold(opts)


def main(argv: Optional[Sequence[str]] = None) -> int:
    parser = ScaffoldIntegrationService.build_parser(prog="dlt-scaffold-integration")
    args = parser.parse_args(argv)
    import logging
    svc = ScaffoldIntegrationService(logger=logging.getLogger("dlt.scaffold"))
    return svc.run(args)


__all__ = ["ScaffoldIntegrationService", "main", "scaffold_integration"]
