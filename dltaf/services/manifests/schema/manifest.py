from __future__ import annotations

import json
from typing import Any, Dict, List, Mapping, Union

from pydantic import ValidationError

from .base import AllowExtraModel, ForbidExtraModel
from .common import (
    AirflowConfig,
    AirflowConfigStrict,
    ConnectionsConfig,
    ConnectionsConfigStrict,
    DependsOnRef,
    DependsOnRefStrict,
    PipelineConfig,
    PipelineConfigStrict,
    RunConfig,
    RunConfigStrict,
)
from .sources import SourceConfig, SourceConfigStrict


class ManifestV1(AllowExtraModel):
    version: int = 1
    pipeline: PipelineConfig
    run: RunConfig | None = None
    connections: ConnectionsConfig | None = None
    source: SourceConfig
    airflow: AirflowConfig | None = None
    depends_on: list[Union[str, DependsOnRef]] | None = None
    task_group: str | None = None


class ManifestV1Strict(ForbidExtraModel):
    version: int = 1
    pipeline: PipelineConfigStrict
    run: RunConfigStrict | None = None
    connections: ConnectionsConfigStrict | None = None
    source: SourceConfig
    airflow: AirflowConfigStrict | None = None
    depends_on: list[Union[str, DependsOnRefStrict]] | None = None
    task_group: str | None = None


class ManifestV1LintStrict(ForbidExtraModel):
    version: int = 1
    pipeline: PipelineConfigStrict
    run: RunConfigStrict | None = None
    connections: ConnectionsConfigStrict | None = None
    source: SourceConfigStrict
    airflow: AirflowConfigStrict | None = None
    depends_on: list[Union[str, DependsOnRefStrict]] | None = None
    task_group: str | None = None


def strip_internal_keys(manifest: Mapping[str, Any]) -> Dict[str, Any]:
    return {str(key): value for key, value in manifest.items() if not str(key).startswith("__")}


def validate_manifest_schema(
    manifest: Mapping[str, Any],
    *,
    strict: bool = True,
    strict_source: bool = False,
) -> ManifestV1 | ManifestV1Strict | ManifestV1LintStrict:
    data = strip_internal_keys(manifest) if strict else dict(manifest)
    try:
        if strict:
            if strict_source:
                return ManifestV1LintStrict.model_validate(data)
            return ManifestV1Strict.model_validate(data)
        return ManifestV1.model_validate(data)
    except ValidationError as exc:
        lines: List[str] = []
        for err in exc.errors():
            loc = ".".join([str(part) for part in err.get("loc", []) if part is not None])
            msg = err.get("msg")
            typ = err.get("type")
            lines.append(f"- {loc}: {msg} ({typ})")
        raise ValueError("Manifest schema validation failed:\n" + "\n".join(lines)) from exc


def manifest_json_schema(*, strict: bool = True, strict_source: bool = False) -> Dict[str, Any]:
    if strict:
        model = ManifestV1LintStrict if strict_source else ManifestV1Strict
    else:
        model = ManifestV1
    return model.model_json_schema()


def manifest_json_schema_str(*, strict: bool = True, strict_source: bool = False, indent: int = 2) -> str:
    return json.dumps(
        manifest_json_schema(strict=strict, strict_source=strict_source),
        ensure_ascii=False,
        indent=indent,
    )


__all__ = [
    "ManifestV1",
    "ManifestV1Strict",
    "ManifestV1LintStrict",
    "manifest_json_schema",
    "manifest_json_schema_str",
    "strip_internal_keys",
    "validate_manifest_schema",
]
