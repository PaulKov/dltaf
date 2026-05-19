from __future__ import annotations

"""Centralized compatibility and deprecation helpers.

`dltaf/*` is now the canonical package and CLI surface. Historical `dlt-*`
entrypoints and legacy import paths remain available as a compatibility layer,
but they are deprecated and documented in docs/COMPATIBILITY.md and
`docs/DEPRECATION_ROADMAP.md`.
"""

import os
import warnings
from dataclasses import dataclass
from typing import Literal, Optional

LegacyPolicy = Literal["off", "warn", "error"]


@dataclass(frozen=True)
class LegacyAlias:
    legacy: str
    replacement: str
    kind: str
    status: str = "deprecated"


LEGACY_ENTRYPOINTS = {
    "dlt-manifest-run": LegacyAlias("dlt-manifest-run", "dltaf manifest run", "entrypoint"),
    "dlt-manifest-lint": LegacyAlias("dlt-manifest-lint", "dltaf manifest lint", "entrypoint"),
    "dlt-manifest-doctor": LegacyAlias("dlt-manifest-doctor", "dltaf manifest doctor", "entrypoint"),
    "dlt-manifest-schema": LegacyAlias("dlt-manifest-schema", "dltaf manifest schema", "entrypoint"),
    "dlt-infra-checks": LegacyAlias("dlt-infra-checks", "dltaf infra-checks", "entrypoint"),
    "dlt-contract-test": LegacyAlias("dlt-contract-test", "dltaf contracts test", "entrypoint"),
    "dlt-pipeline-runs": LegacyAlias("dlt-pipeline-runs", "dltaf runs list", "entrypoint"),
    "dlt-generate-dags": LegacyAlias("dlt-generate-dags", "dltaf dags generate", "entrypoint"),
    "dlt-show-lineage": LegacyAlias("dlt-show-lineage", "dltaf lineage show", "entrypoint"),
    "dlt-scaffold-integration": LegacyAlias("dlt-scaffold-integration", "dltaf scaffold integration", "entrypoint"),
}

LEGACY_MODULES = {
    "dlt_utils.core.context": LegacyAlias("dlt_utils.core.context", "dltaf.app.runtime", "module"),
    "dlt_utils.core.services": LegacyAlias("dlt_utils.core.services", "dltaf.app.services", "module"),
    "dlt_utils.core.redaction": LegacyAlias("dlt_utils.core.redaction", "dltaf.services.execution.redaction", "module"),
    "dlt_utils.core.registry": LegacyAlias("dlt_utils.core.registry", "dltaf.extensions.runners", "module"),
    "dlt_utils.core.hooks": LegacyAlias("dlt_utils.core.hooks", "dltaf.extensions.hooks", "module"),
    "dlt_utils.core.infra_checks": LegacyAlias("dlt_utils.core.infra_checks", "dltaf.extensions.infra_checks", "module"),
    "dlt_utils.core.manifest_io": LegacyAlias("dlt_utils.core.manifest_io", "dltaf.services.manifests.loader/resolver", "module"),
    "dlt_utils.core.manifest_overrides": LegacyAlias("dlt_utils.core.manifest_overrides", "dltaf.services.manifests.overrides", "module"),
    "dlt_utils.core.manifest_validation": LegacyAlias("dlt_utils.core.manifest_validation", "dltaf.services.manifests.validator", "module"),
    "dlt_utils.core.manifest_schema": LegacyAlias("dlt_utils.core.manifest_schema", "dltaf.services.manifests.schema", "module"),
    "dlt_utils.core.run_plan": LegacyAlias("dlt_utils.core.run_plan", "dltaf.services.execution.planner", "module"),
    "dlt_utils.core.run_modes": LegacyAlias("dlt_utils.core.run_modes", "dltaf.services.execution.executor", "module"),
    "dlt_utils.core.run_orchestrator": LegacyAlias("dlt_utils.core.run_orchestrator", "dltaf.services.execution.executor", "module"),
    "dlt_utils.runners.base": LegacyAlias("dlt_utils.runners.base", "dltaf.extensions.runners.protocol", "module"),
    "dlt_utils.runners.uploader_b057": LegacyAlias("dlt_utils.runners.uploader_b057", "dltaf.integrations.uploader_b057.runner", "module"),
    "dlt_utils.runners.pkb_conclusion": LegacyAlias("dlt_utils.runners.pkb_conclusion", "dltaf.integrations.pkb_conclusion.runner", "module"),
    "dlt_utils.runners.sql_database": LegacyAlias("dlt_utils.runners.sql_database", "dltaf.integrations.sql_database.runner", "module"),
    "dlt_utils.runners.oracle_custom_sql": LegacyAlias("dlt_utils.runners.oracle_custom_sql", "dltaf.integrations.oracle_custom_sql.runner", "module"),
    "dlt_utils.runners.mongodb": LegacyAlias("dlt_utils.runners.mongodb", "dltaf.integrations.mongodb.runner", "module"),
    "dlt_pipelines.uploader__b057__pipeline.uploader_b057_source": LegacyAlias(
        "dlt_pipelines.uploader__b057__pipeline.uploader_b057_source",
        "dltaf.integrations.uploader_b057.source",
        "module",
    ),
    "dlt_pipelines.pkbconc__dwh__pipeline.pkb_conclusion_source": LegacyAlias(
        "dlt_pipelines.pkbconc__dwh__pipeline.pkb_conclusion_source",
        "dltaf.integrations.pkb_conclusion.source",
        "module",
    ),
}


class LegacyPolicyError(RuntimeError):
    """Raised when a deprecated surface is blocked by policy."""


def _env_truthy(name: str) -> bool:
    value = os.getenv(name, "0").strip().lower()
    return value in {"1", "true", "yes", "on"}


def _normalise_policy(value: str | None, *, default: LegacyPolicy = "warn") -> LegacyPolicy:
    if not value:
        return default
    normalized = value.strip().lower()
    if normalized in {"off", "warn", "error"}:
        return normalized  # type: ignore[return-value]
    return default


def resolve_policy(kind: Literal["cli", "import"]) -> LegacyPolicy:
    if _env_truthy("DLTAF_SILENCE_LEGACY_WARNINGS"):
        return "off"
    specific_name = "DLTAF_LEGACY_CLI_POLICY" if kind == "cli" else "DLTAF_LEGACY_IMPORT_POLICY"
    specific = os.getenv(specific_name)
    if specific:
        return _normalise_policy(specific)
    return _normalise_policy(os.getenv("DLTAF_LEGACY_POLICY"))


def describe_policy() -> dict[str, str]:
    return {
        "cli": resolve_policy("cli"),
        "import": resolve_policy("import"),
    }


def _message(prefix: str, name: str, target: str) -> str:
    return f"{prefix} '{name}' is deprecated; use '{target}' instead."


def warn_legacy_entrypoint(name: str, *, replacement: Optional[str] = None) -> None:
    alias = LEGACY_ENTRYPOINTS.get(name)
    target = replacement or (alias.replacement if alias else None) or "dltaf"
    policy = resolve_policy("cli")
    if policy == "off":
        return
    message = _message("Legacy CLI entrypoint", name, target)
    if policy == "error":
        raise SystemExit(message)
    warnings.warn(message, FutureWarning, stacklevel=2)


def warn_legacy_module(name: str, *, replacement: Optional[str] = None) -> None:
    alias = LEGACY_MODULES.get(name)
    target = replacement or (alias.replacement if alias else None) or "dltaf"
    policy = resolve_policy("import")
    if policy == "off":
        return
    message = _message("Legacy import path", name, target)
    if policy == "error":
        raise ImportError(message)
    warnings.warn(message, DeprecationWarning, stacklevel=2)


__all__ = [
    "LEGACY_ENTRYPOINTS",
    "LEGACY_MODULES",
    "LegacyAlias",
    "LegacyPolicy",
    "LegacyPolicyError",
    "describe_policy",
    "resolve_policy",
    "warn_legacy_entrypoint",
    "warn_legacy_module",
]
