"""Compatibility shim for SourceRunner protocol.

Canonical definition now lives in :mod:`dltaf.extensions.runners.protocol`.
"""

from __future__ import annotations

from dltaf.app.deprecation import warn_legacy_module

warn_legacy_module(__name__, replacement="dltaf.extensions.runners.protocol")

from dltaf.extensions.runners.protocol import SourceRunner

__all__ = ["SourceRunner"]
