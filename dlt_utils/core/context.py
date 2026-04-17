"""Compatibility shim for run context models.

Native implementation now lives in :mod:`dltaf.app.runtime`.
"""

from dltaf.app.deprecation import warn_legacy_module

warn_legacy_module(__name__, replacement="dltaf.app.runtime")

from dltaf.app.runtime import RunContext, RunOptions

__all__ = ["RunContext", "RunOptions"]
