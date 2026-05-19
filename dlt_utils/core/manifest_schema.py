"""Compatibility shim for manifest schema models and JSON Schema generation.

Wave 6 / Stage 39
-----------------
The implementation now lives in `dltaf.services.manifests.schema`.
This module re-exports the public API and model classes for backward compatibility.
"""

from dltaf.app.deprecation import warn_legacy_module

warn_legacy_module(__name__, replacement="dltaf.services.manifests.schema")

from dltaf.services.manifests.schema import *  # noqa: F403

__all__ = [name for name in globals() if not name.startswith("_")]
