"""Compatibility shim for manifest overrides.

Stage 35
--------
The implementation now lives in `dltaf.services.manifests.overrides`.
"""

from dltaf.app.deprecation import warn_legacy_module

warn_legacy_module(__name__, replacement="dltaf.services.manifests.overrides")

from dltaf.services.manifests.overrides import (
    ManifestOverridesService,
    OverrideSpec,
    apply_override,
    apply_overrides,
    parse_path,
    parse_set_args,
    specs_to_mapping,
)

__all__ = [
    "ManifestOverridesService",
    "OverrideSpec",
    "apply_override",
    "apply_overrides",
    "parse_path",
    "parse_set_args",
    "specs_to_mapping",
]
