from __future__ import annotations

from dltaf.app.deprecation import warn_legacy_module

warn_legacy_module(__name__, replacement="dltaf.integrations.mongodb.runner")

"""Compatibility wrapper for the native Wave 9 mongodb runner."""

from dltaf.integrations.mongodb.runner import MongoDBRunner

__all__ = ["MongoDBRunner"]
