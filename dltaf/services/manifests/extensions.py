from __future__ import annotations

from copy import deepcopy
from pathlib import Path
from typing import Any, Dict, Mapping, Sequence


MANIFEST_METADATA_EXTENSION_KEYS = frozenset({"catalog"})
MANIFEST_FILE_REFERENCE_KEYS = ("sql_file",)


def strip_manifest_metadata_extensions(
    manifest: Mapping[str, Any],
    *,
    metadata_keys: set[str] | frozenset[str] = MANIFEST_METADATA_EXTENSION_KEYS,
) -> Dict[str, Any]:
    """Return a runtime-safe manifest without consumer-owned metadata blocks.

    Consumer repositories may keep self-service catalog/documentation metadata
    next to the executable manifest.  Framework validation and execution should
    receive only the runtime contract, so known top-level metadata extensions
    are ignored consistently by loaders, validators, and Airflow wrappers.
    """

    return {
        str(key): value
        for key, value in dict(manifest).items()
        if str(key) not in metadata_keys
    }


def build_runtime_manifest_payload(
    manifest: Mapping[str, Any],
    *,
    original_manifest_path: str | Path,
    file_reference_keys: Sequence[str] = MANIFEST_FILE_REFERENCE_KEYS,
) -> Dict[str, Any]:
    """Build a runtime-only manifest payload safe to write outside the repo.

    Temporary runtime manifests are often written to `/tmp`, while SQL assets
    remain in the consumer repository.  Relative file references therefore need
    to be resolved against the original manifest location before the payload is
    written elsewhere.
    """

    runtime_manifest = deepcopy(strip_manifest_metadata_extensions(manifest))
    base_dir = Path(original_manifest_path).expanduser().resolve().parent
    for key_name in file_reference_keys:
        _resolve_path_key(runtime_manifest, key_name=key_name, base_dir=base_dir)
    return runtime_manifest


def _resolve_path_key(value: Any, *, key_name: str, base_dir: Path) -> None:
    if isinstance(value, dict):
        for key, item in value.items():
            if key == key_name and isinstance(item, str):
                value[key] = _resolve_path_value(item, base_dir)
            else:
                _resolve_path_key(item, key_name=key_name, base_dir=base_dir)
        return

    if isinstance(value, list):
        for item in value:
            _resolve_path_key(item, key_name=key_name, base_dir=base_dir)


def _resolve_path_value(value: str, base_dir: Path) -> str:
    stripped_value = value.strip()
    if not stripped_value:
        return value

    path = Path(stripped_value).expanduser()
    if path.is_absolute():
        return stripped_value
    return str((base_dir / path).resolve())


__all__ = [
    "MANIFEST_FILE_REFERENCE_KEYS",
    "MANIFEST_METADATA_EXTENSION_KEYS",
    "build_runtime_manifest_payload",
    "strip_manifest_metadata_extensions",
]
