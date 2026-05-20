"""Helpers for deriving slim `dltaf` install profiles from consumer manifests.

The public `dltaf` package exposes optional extras so consumers can install only
the runtime capabilities they actually need. This module keeps the mapping from
manifest contract -> package extras in one place, so Airflow runtime, CI, and
E2E prewarm logic stay consistent.
"""

from __future__ import annotations

from dataclasses import dataclass
import re
from pathlib import Path
from typing import Any, Mapping

import yaml


_DLTAF_SPEC_PATTERN = re.compile(
    r"^(?P<name>[A-Za-z0-9_.-]+)(?:\[(?P<extras>[^\]]+)\])?(?P<suffix>.*)$"
)

_SQL_SOURCE_KINDS = {"sqldb", "sql_database", "oracle_custom_sql", "oracle"}
_PREFERRED_EXTRA_ORDER = (
    "all",
    "runtime",
    "clickhouse",
    "sqldb",
    "postgres",
    "oracle",
    "mongodb",
    "vault",
)


@dataclass(frozen=True)
class ParsedInstallSpec:
    name: str
    extras: tuple[str, ...]
    suffix: str


def load_manifest_mapping(path: str | Path) -> dict[str, Any]:
    """Load a manifest file and validate that it is a mapping."""

    manifest_path = Path(path).expanduser().resolve()
    payload = yaml.safe_load(manifest_path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError(f"Manifest must be a mapping: {manifest_path}")
    return payload


def parse_install_spec(requirement: str) -> ParsedInstallSpec:
    """Parse a pip requirement into package name, extras and version suffix."""

    raw = str(requirement or "").strip()
    if not raw:
        raise ValueError("Install spec must not be empty")

    match = _DLTAF_SPEC_PATTERN.match(raw)
    if not match:
        raise ValueError(f"Unsupported install spec: {requirement}")

    extras_raw = match.group("extras") or ""
    extras = tuple(
        extra.strip().lower()
        for extra in extras_raw.split(",")
        if extra.strip()
    )
    return ParsedInstallSpec(
        name=match.group("name").strip(),
        extras=extras,
        suffix=match.group("suffix").strip(),
    )


def format_install_spec(spec: ParsedInstallSpec) -> str:
    """Render a parsed install spec back into a pip-compatible requirement."""

    extras = ",".join(spec.extras)
    extras_part = f"[{extras}]" if extras else ""
    return f"{spec.name}{extras_part}{spec.suffix}"


def expand_dltaf_requirement(requirement: str, manifest: Mapping[str, Any]) -> str:
    """Expand a `dltaf` requirement with extras inferred from a manifest."""

    parsed = parse_install_spec(requirement)
    if parsed.name.lower() != "dltaf":
        return requirement

    inferred_extras = infer_dltaf_extras(manifest)
    combined = _normalize_extras(set(parsed.extras) | inferred_extras)
    return format_install_spec(
        ParsedInstallSpec(name=parsed.name, extras=tuple(combined), suffix=parsed.suffix)
    )


def infer_dltaf_extras(manifest: Mapping[str, Any]) -> set[str]:
    """Infer the minimal `dltaf` extras set required by a manifest."""

    source = _mapping(manifest.get("source"))
    connections = _mapping(manifest.get("connections"))
    source_connection = _mapping(connections.get("source"))
    destination_connection = _mapping(connections.get("destination"))
    pipeline = _mapping(manifest.get("pipeline"))

    source_kind = _lower(source.get("kind"))
    source_connection_kind = _lower(source_connection.get("kind"))
    destination_kind = _lower(destination_connection.get("kind")) or _lower(
        pipeline.get("destination")
    )
    dialect = _lower(source.get("dialect"))
    drivername = _lower(_mapping(source_connection.get("overrides")).get("drivername"))

    extras: set[str] = set()

    if destination_kind == "clickhouse":
        extras.add("clickhouse")

    if source_kind in _SQL_SOURCE_KINDS:
        extras.add("sqldb")

        if (
            source_kind in {"oracle_custom_sql", "oracle"}
            or dialect == "oracle"
            or source_connection_kind == "oracle"
            or "oracle" in drivername
        ):
            extras.add("oracle")
        elif source_connection_kind == "postgres" or "postgresql" in drivername:
            extras.add("postgres")

    if source_kind == "mongodb":
        extras.add("mongodb")

    if manifest_uses_vault(manifest):
        extras.add("vault")

    return set(_normalize_extras(extras))


def manifest_needs_sqlalchemy_upgrade(manifest: Mapping[str, Any]) -> bool:
    """Return True when the legacy embedded runtime must add SQLAlchemy 2.x."""

    extras = infer_dltaf_extras(manifest)
    return "sqldb" in extras or "oracle" in extras or "postgres" in extras


def manifest_uses_vault(manifest: Mapping[str, Any]) -> bool:
    """Detect whether a manifest references Vault-backed connections."""

    def _scan(value: Any) -> bool:
        if isinstance(value, dict):
            for key, item in value.items():
                if str(key).strip().lower() == "vault" and bool(item):
                    return True
                if _scan(item):
                    return True
            return False
        if isinstance(value, list):
            return any(_scan(item) for item in value)
        return False

    return _scan(manifest)


def _mapping(value: Any) -> Mapping[str, Any]:
    return value if isinstance(value, dict) else {}


def _lower(value: Any) -> str:
    return str(value or "").strip().lower()


def _normalize_extras(extras: set[str]) -> list[str]:
    clean = {extra.strip().lower() for extra in extras if extra and extra.strip()}
    if not clean:
        return []

    if "all" in clean:
        return ["all"]

    if {"clickhouse", "vault"} <= clean:
        clean.discard("clickhouse")
        clean.discard("vault")
        clean.add("runtime")

    if "runtime" in clean:
        clean.discard("clickhouse")
        clean.discard("vault")

    order = {name: idx for idx, name in enumerate(_PREFERRED_EXTRA_ORDER)}
    return sorted(clean, key=lambda item: (order.get(item, len(order)), item))
