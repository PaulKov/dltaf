"""Secret resolution helpers for dltaf runtime services."""

from dltaf.services.secrets.vault import (
    VaultSecretRef,
    apply_overrides,
    get_secret_from_vault,
    parse_vault_ref,
    resolve_vault_secret,
)
from dltaf.services.secrets.connections import build_resolved_env_from_connections
from dltaf.services.secrets.types import ResolvedEnv, ResolvedValue

__all__ = [
    "ResolvedEnv",
    "ResolvedValue",
    "VaultSecretRef",
    "apply_overrides",
    "build_resolved_env_from_connections",
    "get_secret_from_vault",
    "parse_vault_ref",
    "resolve_vault_secret",
]
