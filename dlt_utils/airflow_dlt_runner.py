from __future__ import annotations

from typing import Any, Dict, Optional

from dlt_utils.manifest_runner import run_manifest
from dlt_utils.vault_env import temporary_environ


def run_dlt_pipeline(
    module_path: str,
    func_name: str = "main",
    func_kwargs: Optional[Dict] = None,
    env: Optional[Dict[str, str]] = None,
    dags_dir: Optional[str] = None,
    add_dags_dir_to_path: bool = False,
    configure_logging: bool = False,
    **_,
) -> None:
    """Legacy universal runner for a python-based pipeline module.

    Kept for backward compatibility.

    Prefer using :func:`run_dlt_manifest` + YAML manifests.
    """

    import importlib
    import sys

    if configure_logging:
        import logging

        logging.basicConfig(
            level=logging.INFO,
            format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
        )

    if add_dags_dir_to_path and dags_dir and dags_dir not in sys.path:
        sys.path.append(dags_dir)

    with temporary_environ(env or {}):
        module = importlib.import_module(module_path)
        func = getattr(module, func_name)

        if func_kwargs:
            func(**func_kwargs)
        else:
            func()


def run_dlt_manifest(
    *,
    manifest_path: str,
    write_disposition: Optional[str] = None,
    extra_env: Optional[Dict[str, str]] = None,
    configure_logging: bool = False,
    log_level: str = "INFO",
    validate_only: bool = False,
) -> Any:
    """Run a pipeline using a YAML manifest.

    This is the preferred integration point for Airflow DAGs.

    Args:
        manifest_path: path to YAML file (absolute or relative)
        write_disposition: optional override
        extra_env: additional env vars to set (non-secret)
        configure_logging: basicConfig inside the task process
        log_level: INFO/WARNING/...
        validate_only: if True, only validate the manifest
    """
    return run_manifest(
        manifest_path,
        write_disposition=write_disposition,
        extra_env=extra_env,
        configure_logging=configure_logging,
        log_level=log_level,
        validate_only=validate_only,
    )