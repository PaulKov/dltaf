from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Any, Callable, Dict, Mapping, Optional

VaultGetter = Callable[..., Mapping[str, Any]]


@dataclass(frozen=True)
class VaultSecretRef:
    """Reference to a HashiCorp Vault KV secret."""

    mount_point: str
    path: str
    kv_version: Optional[str] = None


def _ensure_vault_env_aliases() -> None:
    vault_addr = str(os.getenv("VAULT_ADDR", "") or "").strip()
    vault_address = str(os.getenv("VAULT_ADDRESS", "") or "").strip()
    if not vault_addr and vault_address:
        os.environ["VAULT_ADDR"] = vault_address
    if not vault_address and vault_addr:
        os.environ["VAULT_ADDRESS"] = vault_addr


def _parse_ref_string(raw_ref: str, *, kv_version: Optional[str] = None) -> VaultSecretRef:
    ref = raw_ref.strip()
    if not ref:
        raise ValueError("vault ref is empty")

    if ref.startswith("vault://"):
        body = ref[len("vault://") :]
        parts = body.split("/", 1)
        if len(parts) != 2:
            raise ValueError(f"invalid vault ref: {raw_ref!r}")
        return VaultSecretRef(parts[0].strip().rstrip("/"), parts[1].strip().strip("/"), kv_version)

    if ":" in ref:
        mount_point, path = ref.split(":", 1)
        mount_point = mount_point.strip().rstrip("/")
        path = path.strip().strip("/")
        if not mount_point or not path:
            raise ValueError(f"invalid vault ref: {raw_ref!r}")
        return VaultSecretRef(mount_point, path, kv_version)

    raise ValueError(
        "Vault ref must be 'mount:path', 'vault://mount/path', or a mapping with mount/path. "
        f"Got: {raw_ref!r}"
    )


def parse_vault_ref(ref: Any) -> VaultSecretRef:
    if ref is None:
        raise ValueError("vault ref is required")
    if isinstance(ref, VaultSecretRef):
        return ref
    if isinstance(ref, Mapping):
        kv_version = ref.get("kv_version")
        kv_version_str = str(kv_version).strip() if kv_version not in (None, "") else None
        embedded_ref = str(ref.get("ref") or "").strip()
        if embedded_ref:
            return _parse_ref_string(embedded_ref, kv_version=kv_version_str)
        mount_point = str(ref.get("mount_point") or ref.get("mount") or "").strip().rstrip("/")
        path = str(ref.get("path") or "").strip().strip("/")
        if not mount_point or not path:
            raise ValueError(f"invalid vault ref mapping: {ref}")
        return VaultSecretRef(mount_point, path, kv_version_str)
    if isinstance(ref, str):
        return _parse_ref_string(ref)
    raise ValueError(f"vault ref must be str or mapping, got: {type(ref)}")


def apply_overrides(secret: Mapping[str, Any], overrides: Optional[Mapping[str, Any]]) -> Dict[str, Any]:
    resolved = dict(secret or {})
    for key, value in dict(overrides or {}).items():
        resolved[str(key)] = value
    return resolved


def get_secret_from_vault(ref: VaultSecretRef, *, getter: VaultGetter | None = None) -> Dict[str, Any]:
    """Fetch a Vault secret through vault-kv-client with Airflow fallback support."""

    active_getter = getter
    if active_getter is None:
        try:
            from vault_kv_client.convenience import get_creds
        except ImportError as exc:  # pragma: no cover - dependency availability
            raise RuntimeError(
                "Vault support requires vault-kv-client. Install 'vault-kv-client' "
                "or pass an injected getter."
            ) from exc
        _ensure_vault_env_aliases()
        active_getter = get_creds

    kwargs: dict[str, Any] = {}
    if ref.kv_version:
        kwargs["kv_version"] = ref.kv_version
    return dict(active_getter(ref.mount_point, ref.path, **kwargs))


def resolve_vault_secret(
    vault_ref: Any,
    *,
    overrides: Optional[Mapping[str, Any]] = None,
    getter: VaultGetter | None = None,
) -> Dict[str, Any]:
    if vault_ref in (None, ""):
        return {}
    return apply_overrides(get_secret_from_vault(parse_vault_ref(vault_ref), getter=getter), overrides)
