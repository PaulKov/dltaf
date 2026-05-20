"""Shared helpers for runners.

Stage 1: we centralize replace protection wrapper that was previously inside
`dlt_utils.manifest_runner`.

Why:
- keep runner implementations small
- avoid duplication
"""

from __future__ import annotations

import logging
import os
from dataclasses import dataclass
from typing import Any, Callable, Literal, Mapping

from dlt_utils.clickhouse_helpers import (
    check_tables_exist,
    cleanup_staging_tables,
    drop_pending_packages,
    get_expected_table_names,
)
from dltaf.services.execution.redaction import safe_exception_message

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class ReplaceProtectedExecutionError(Exception):
    stage: Literal["source", "load"]
    cause: Exception

    def __post_init__(self) -> None:
        Exception.__init__(self, f"{self.stage} stage failed: {self.cause}")


def run_with_replace_protection(
    *,
    pipeline: Any,
    source_factory: Callable[[], Any],
    manifest: Mapping[str, Any],
    write_disposition: str,
    annotate_errors: bool = False,
) -> Any:
    """Generic wrapper for replace mode protection.

    Handles:
    - Cleanup (pending packages, staging tables)
    - Table existence check
    - Auto-create tables with append if missing
    - Automatic retry on common "missing table/staging" errors

    Note:
        This wrapper is intentionally kept compatible with the legacy implementation.
        Further refactors (hooks, audit logs, retries) will build on top of it.
    """

    pipeline_cfg = manifest.get("pipeline") or {}
    dataset_name = pipeline_cfg.get("dataset", "")

    # Protection for replace mode
    if write_disposition == "replace":
        logger.info("Running cleanup before replace operation...")
        drop_pending_packages(pipeline)
        cleanup_staging_tables(dataset_name)

        # Check if tables exist
        expected_tables = get_expected_table_names(manifest)

        if expected_tables:
            dataset_separator = os.getenv(
                "DESTINATION__CLICKHOUSE__CREDENTIALS__DATASET_TABLE_SEPARATOR", "__"
            )
            table_status = check_tables_exist(dataset_name, expected_tables, dataset_separator)
            missing_tables = [table for table, exists in table_status.items() if not exists]

            if missing_tables:
                logger.warning("Tables do not exist yet: %s", missing_tables)
                logger.info("Creating tables first with append mode...")
                try:
                    # First run with append to create tables
                    try:
                        source_append = source_factory()
                    except Exception as e:
                        if annotate_errors:
                            raise ReplaceProtectedExecutionError(stage="source", cause=e) from e
                        raise
                    pipeline.run(source_append, write_disposition="append")
                    logger.info("Tables created successfully, now proceeding with replace...")
                except Exception as e:
                    logger.error("Failed to create tables with append: %s", safe_exception_message(e))
                    if annotate_errors and not isinstance(e, ReplaceProtectedExecutionError):
                        raise ReplaceProtectedExecutionError(stage="load", cause=e) from e
                    raise

    # Run the pipeline
    try:
        try:
            source = source_factory()
        except Exception as e:
            if annotate_errors:
                raise ReplaceProtectedExecutionError(stage="source", cause=e) from e
            raise
        load_info = pipeline.run(source, write_disposition=write_disposition)
        logger.info("dlt load finished: %s", load_info)
        return load_info

    except Exception as e:
        error_msg = str(e)

        # Check if error is related to non-existent tables or pending packages
        is_table_error = "does not exist" in error_msg.lower()
        is_staging_error = "staging" in error_msg.lower() and "does not exist" in error_msg.lower()
        is_pending_error = "pending packages" in error_msg.lower()

        if (is_table_error or is_staging_error or is_pending_error) and write_disposition == "replace":
            logger.warning(
                "Detected table/staging/pending error, attempting automatic recovery..."
            )

            # Cleanup
            drop_pending_packages(pipeline)
            cleanup_staging_tables(dataset_name)

            # Retry: create tables with append, then replace
            logger.info("Step 1: Creating tables with append mode...")
            try:
                try:
                    source_retry = source_factory()
                except Exception as source_error:
                    if annotate_errors:
                        raise ReplaceProtectedExecutionError(stage="source", cause=source_error) from source_error
                    raise
                pipeline.run(source_retry, write_disposition="append")
                logger.info("Tables created successfully")

                logger.info("Step 2: Running with replace mode...")
                try:
                    source_retry2 = source_factory()
                except Exception as source_error:
                    if annotate_errors:
                        raise ReplaceProtectedExecutionError(stage="source", cause=source_error) from source_error
                    raise
                load_info = pipeline.run(source_retry2, write_disposition="replace")
                logger.info("dlt load finished after recovery: %s", load_info)
                return load_info
            except Exception as retry_error:
                logger.error("Retry failed: %s", safe_exception_message(retry_error))
                if annotate_errors and not isinstance(retry_error, ReplaceProtectedExecutionError):
                    raise ReplaceProtectedExecutionError(stage="load", cause=retry_error) from retry_error
                raise retry_error from e

        # Re-raise original exception if we couldn't handle it
        if annotate_errors:
            raise ReplaceProtectedExecutionError(stage="load", cause=e) from e
        raise
