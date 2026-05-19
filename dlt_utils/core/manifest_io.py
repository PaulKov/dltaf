"""Compatibility shim for manifest IO.

Stage 35
--------
The implementation now lives in `dltaf.services.manifests`.
Keep this module as a stable import path for legacy callers.
"""

from dltaf.app.deprecation import warn_legacy_module

warn_legacy_module(__name__, replacement="dltaf.services.manifests.loader/resolver")

from dltaf.services.manifests.loader import ManifestLoader, load_manifest
from dltaf.services.manifests.resolver import ManifestResolver, resolve_manifest

__all__ = ["ManifestLoader", "ManifestResolver", "load_manifest", "resolve_manifest"]
