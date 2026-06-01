from __future__ import annotations

from typing import Any

__all__ = ["run_manifest"]


def run_manifest(*args: Any, **kwargs: Any):
    from dlt_utils.manifest_runner import run_manifest as _run_manifest

    return _run_manifest(*args, **kwargs)
