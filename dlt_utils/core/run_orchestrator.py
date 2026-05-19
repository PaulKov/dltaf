"""Compatibility shim for manifest execution orchestration.

Stage 35
--------
The implementation now lives in `dltaf.services.execution.executor`.
"""

from dltaf.app.deprecation import warn_legacy_module

warn_legacy_module(__name__, replacement="dltaf.services.execution.executor")

from dltaf.services.execution.executor import ManifestExecutor, run_manifest

__all__ = ["ManifestExecutor", "run_manifest"]
