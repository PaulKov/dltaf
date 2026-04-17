from __future__ import annotations

import logging
from pathlib import Path
from typing import Any, Mapping, Optional

from dlt_utils.contracts import PayloadContractConfig, build_payload_contract
from dltaf.services.manifests.schema import validate_manifest_schema
from dltaf.extensions.hooks.registry import build_hook_registry
from dltaf.extensions.infra_checks.registry import build_infra_check_registry
from dltaf.extensions.runners.registry import build_runner_registry
from lineage.manifest_dependency_resolver import InvalidDependencyError, _extract_depends_on_simple

logger = logging.getLogger(__name__)


class ManifestValidator:
    """Schema + framework + extension-aware manifest validator."""

    def __init__(self, *, logger_: Optional[logging.Logger] = None) -> None:
        self.logger = logger_ or logger

    def validate(
        self,
        manifest: Mapping[str, Any],
        *,
        strict_source: bool = False,
        enforce_filename_match: bool = True,
    ) -> None:
        schema_model = validate_manifest_schema(
            manifest,
            strict=True,
            strict_source=bool(strict_source),
        )
        if isinstance(manifest, dict):
            internal = {k: v for k, v in manifest.items() if str(k).startswith("__")}
            manifest.clear()
            manifest.update(schema_model.model_dump(mode="json", by_alias=True, exclude_none=True))
            manifest.update(internal)

        self._validate_common_rules(manifest, enforce_filename_match=bool(enforce_filename_match))
        self._validate_hook_configuration(manifest)
        self._validate_online_checks_configuration(manifest)
        self._validate_payload_contract(manifest)
        self._validate_runner(manifest)

    def get_runner(self, manifest: Mapping[str, Any]) -> Any:
        source = manifest.get("source") or {}
        kind = str((source.get("kind") or "")).strip()
        if not kind:
            raise ValueError("source.kind is required")
        registry, plugin_report = build_runner_registry(manifest=manifest, include_env_plugins=True)
        plugin_errors = plugin_report.get("errors") if isinstance(plugin_report, Mapping) else None
        if isinstance(plugin_errors, list) and plugin_errors:
            raise ValueError("Runner plugins failed to load: " + "; ".join(plugin_errors))
        return registry.get(kind)

    def _validate_common_rules(self, manifest: Mapping[str, Any], *, enforce_filename_match: bool) -> None:
        version = manifest.get("version")
        if version != 1:
            raise ValueError(f"Unsupported manifest version: {version!r} (expected 1)")

        pipeline = manifest.get("pipeline") or {}
        if not isinstance(pipeline, Mapping):
            raise ValueError("pipeline section must be a mapping")

        name = str(pipeline.get("name") or "").strip()
        if not name:
            raise ValueError("pipeline.name is required")

        manifest_path = Path(str(manifest.get("__manifest_path__") or ""))
        try:
            depends_on = _extract_depends_on_simple(manifest, manifest_path)
            if name in depends_on:
                raise ValueError(
                    f"Pipeline '{name}' cannot depend on itself. Self-referencing dependency detected in depends_on."
                )
            self.logger.debug("Pipeline '%s' depends_on: %s", name, depends_on)
        except InvalidDependencyError as exc:
            raise ValueError(f"Invalid depends_on in manifest: {exc}") from exc

        if enforce_filename_match and manifest_path.name:
            expected_name = manifest_path.stem
            if expected_name != name:
                raise ValueError(
                    "pipeline.name must match manifest filename. "
                    f"Expected '{expected_name}', got '{name}'."
                )

        airflow_cfg = manifest.get("airflow") or {}
        if airflow_cfg and isinstance(airflow_cfg, Mapping):
            dag_id = airflow_cfg.get("dag_id")
            if dag_id and str(dag_id).strip() != name:
                raise ValueError(
                    "airflow.dag_id must match pipeline.name. "
                    f"Expected '{name}', got '{dag_id}'."
                )

        dest = str(pipeline.get("destination") or "").strip()
        if not dest:
            raise ValueError("pipeline.destination is required")

        dataset = str(pipeline.get("dataset") or "").strip()
        if not dataset:
            raise ValueError("pipeline.dataset is required")

        source = manifest.get("source") or {}
        if not isinstance(source, Mapping):
            raise ValueError("source section must be a mapping")
        kind = str(source.get("kind") or "").strip()
        if not kind:
            raise ValueError("source.kind is required")

    def _validate_hook_configuration(self, manifest: Mapping[str, Any]) -> None:
        run_cfg = manifest.get("run") or {}
        hooks_cfg = (run_cfg.get("hooks") or {}) if isinstance(run_cfg, Mapping) else {}
        if not hooks_cfg:
            return
        if not isinstance(hooks_cfg, Mapping):
            raise ValueError("run.hooks must be a mapping")

        registry, plugin_report = build_hook_registry(manifest=manifest, include_env_plugins=False)
        plugin_errors = plugin_report.get("errors") if isinstance(plugin_report, Mapping) else None
        if isinstance(plugin_errors, list) and plugin_errors:
            raise ValueError("Hook plugins failed to load: " + "; ".join(plugin_errors))

        allowed = set(registry.names())
        requested = set()
        for key in ("only", "enable", "disable"):
            for name in (hooks_cfg.get(key) or []):
                normalized = str(name).strip()
                if normalized:
                    requested.add(normalized)
        unknown = sorted(requested - allowed)
        if unknown:
            raise ValueError("Unknown hook name(s) in run.hooks: " + ", ".join(unknown))

    def _validate_online_checks_configuration(self, manifest: Mapping[str, Any]) -> None:
        run_cfg = manifest.get("run") or {}
        online_cfg = (run_cfg.get("online_checks") or {}) if isinstance(run_cfg, Mapping) else {}
        if not online_cfg:
            return
        if not isinstance(online_cfg, Mapping):
            raise ValueError("run.online_checks must be a mapping")

        registry, plugin_report = build_infra_check_registry(
            manifest=manifest,
            ctx=None,
            include_env_plugins=False,
        )
        plugin_errors = plugin_report.get("errors") if isinstance(plugin_report, Mapping) else None
        if isinstance(plugin_errors, list) and plugin_errors:
            raise ValueError("Infra check plugins failed to load: " + "; ".join(plugin_errors))

        allowed = set(registry.names())
        requested = set()
        for key in ("only", "enable", "disable"):
            for name in (online_cfg.get(key) or []):
                normalized = str(name).strip()
                if normalized:
                    requested.add(normalized)
        unknown = sorted(requested - allowed)
        if unknown:
            raise ValueError("Unknown infra check name(s) in run.online_checks: " + ", ".join(unknown))

    def _validate_payload_contract(self, manifest: Mapping[str, Any]) -> None:
        source = manifest.get("source") or {}
        payload_contract = source.get("payload_contract") if isinstance(source, Mapping) else None
        if not payload_contract:
            return

        manifest_path = Path(str(manifest.get("__manifest_path__") or ""))
        kind = str(source.get("kind") or "").strip() if isinstance(source, Mapping) else ""
        try:
            config = PayloadContractConfig.model_validate(payload_contract)
            if config.mode != "off":
                build_payload_contract(
                    config,
                    name=f"{kind}.payload_contract",
                    manifest_path=manifest_path,
                    logger=self.logger,
                )
        except Exception as exc:
            raise ValueError(f"Invalid source.payload_contract: {exc}") from exc

    def _validate_runner(self, manifest: Mapping[str, Any]) -> None:
        source = manifest.get("source") or {}
        kind = str(source.get("kind") or "").strip()
        registry, plugin_report = build_runner_registry(manifest=manifest, include_env_plugins=False)
        plugin_errors = plugin_report.get("errors") if isinstance(plugin_report, Mapping) else None
        if isinstance(plugin_errors, list) and plugin_errors:
            raise ValueError("Runner plugins failed to load: " + "; ".join(plugin_errors))
        runner = registry.get(kind)
        runner.validate(manifest)


_default_validator = ManifestValidator()


def validate_manifest(
    manifest: Mapping[str, Any],
    *,
    strict_source: bool = False,
    enforce_filename_match: bool = True,
) -> None:
    _default_validator.validate(
        manifest,
        strict_source=strict_source,
        enforce_filename_match=enforce_filename_match,
    )


def get_runner_for_manifest(manifest: Mapping[str, Any]) -> Any:
    return _default_validator.get_runner(manifest)


__all__ = ["ManifestValidator", "validate_manifest", "get_runner_for_manifest"]
