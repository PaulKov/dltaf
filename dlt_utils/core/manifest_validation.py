"""Compatibility shim for manifest validation.

Stage 35
--------
The implementation now lives in `dltaf.services.manifests.validator`.
"""

from dltaf.app.deprecation import warn_legacy_module

warn_legacy_module(__name__, replacement="dltaf.services.manifests.validator")

from dltaf.services.manifests.validator import ManifestValidator, get_runner_for_manifest, validate_manifest

__all__ = ["ManifestValidator", "validate_manifest", "get_runner_for_manifest"]
