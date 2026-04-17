"""Compatibility CLI for integration scaffolding.

Wave 6 / Stage 39
-----------------
The implementation now lives in `dltaf.services.scaffold`.
"""

from __future__ import annotations

from dltaf.app.deprecation import warn_legacy_entrypoint

from dltaf.services.scaffold import (
    ALLOWED_TEMPLATES as _ALLOWED_TEMPLATES,
    ScaffoldArtifacts,
    ScaffoldIntegrationService,
    ScaffoldOptions,
    ensure_package_in_pyproject as _ensure_package_in_pyproject,
    main as _native_main,
    normalize_slug as _normalize_slug,
    scaffold_integration,
)

__all__ = [
    "ScaffoldArtifacts",
    "ScaffoldIntegrationService",
    "ScaffoldOptions",
    "_ALLOWED_TEMPLATES",
    "_ensure_package_in_pyproject",
    "_normalize_slug",
    "main",
    "scaffold_integration",
]


def build_parser():
    return ScaffoldIntegrationService.build_parser(prog="dlt-scaffold-integration")


def main(argv=None):
    warn_legacy_entrypoint("dlt-scaffold-integration")
    return _native_main(argv)


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
