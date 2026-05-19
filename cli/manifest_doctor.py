"""Compatibility CLI for manifest doctor.

Wave 6 / Stage 39
-----------------
The implementation now lives in `dltaf.services.manifests.doctor`.
"""

from __future__ import annotations

from dltaf.app.deprecation import warn_legacy_entrypoint

from dltaf.services.manifests.doctor import (
    Issue,
    ManifestDoctorService,
    analyze_manifest,
    apply_fixes_to_text,
    bins_strategy_count,
    discover_yaml_files,
    expected_pipeline_name,
    load_yaml_mapping,
    main as _native_main,
    pick_source_kind,
    render_template,
)

__all__ = [
    "Issue",
    "ManifestDoctorService",
    "analyze_manifest",
    "apply_fixes_to_text",
    "bins_strategy_count",
    "discover_yaml_files",
    "expected_pipeline_name",
    "load_yaml_mapping",
    "main",
    "pick_source_kind",
    "render_template",
]


def build_parser():
    return ManifestDoctorService.build_parser(prog="dlt-manifest-doctor")


def main(argv=None):
    warn_legacy_entrypoint("dlt-manifest-doctor")
    return _native_main(argv)


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
