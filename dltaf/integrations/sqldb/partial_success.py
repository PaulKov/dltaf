from __future__ import annotations

from copy import deepcopy
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Mapping, Optional, Sequence

from dlt_utils.clickhouse_helpers import get_table_storage_stats
from dlt_utils.core.error_taxonomy import classify_exception
from dlt_utils.core.run_result import (
    RunResult,
    TableRunStats,
    extract_load_metrics,
    merge_load_metrics,
)
from dlt_utils.runners.common import ReplaceProtectedExecutionError, run_with_replace_protection
from dltaf.app.runtime import RunContext
from dltaf.integrations.sqldb.config import ResolvedSqlDbConfig
from dltaf.integrations.sqldb.factories import build_single_catalog_table_source_factory, transform_table_name
from dltaf.services.execution.redaction import safe_exception_message

_GIB = 1024.0 ** 3


@dataclass(frozen=True)
class PartialSuccessPolicy:
    mode: str
    tolerate_errors: str
    critical_tables: tuple[str, ...] = ()
    min_success_tables: Optional[int] = None
    min_success_ratio: Optional[float] = None


@dataclass(frozen=True)
class CatalogTableSpec:
    schema_name: str
    source_table: str
    target_table: str


def resolve_partial_success_policy(manifest: Mapping[str, Any]) -> Optional[PartialSuccessPolicy]:
    run_cfg = manifest.get("run") or {}
    partial_cfg = (run_cfg.get("partial_success") or {}) if isinstance(run_cfg, Mapping) else {}
    if not partial_cfg:
        return None

    critical_tables = tuple(str(item).strip() for item in (partial_cfg.get("critical_tables") or []) if str(item).strip())
    min_success_tables = partial_cfg.get("min_success_tables")
    min_success_ratio = partial_cfg.get("min_success_ratio")

    return PartialSuccessPolicy(
        mode=str(partial_cfg.get("mode") or "").strip(),
        tolerate_errors=str(partial_cfg.get("tolerate_errors") or "source_only").strip(),
        critical_tables=critical_tables,
        min_success_tables=int(min_success_tables) if min_success_tables is not None and str(min_success_tables).strip() else None,
        min_success_ratio=float(min_success_ratio) if min_success_ratio is not None and str(min_success_ratio).strip() else None,
    )


def iter_catalog_table_specs(config: ResolvedSqlDbConfig) -> tuple[CatalogTableSpec, ...]:
    catalog = config.source.catalog
    if catalog is None:
        return ()

    transform = dict(getattr(catalog, "table_name_transform", None) or {})
    specs: list[CatalogTableSpec] = []

    schemas = getattr(catalog, "schemas", None)
    if schemas:
        for schema_name, schema_cfg in schemas.items():
            for table_name in tuple(getattr(schema_cfg, "tables", None) or ()):
                specs.append(
                    CatalogTableSpec(
                        schema_name=str(schema_name),
                        source_table=str(table_name),
                        target_table=f"{schema_name}__{table_name}",
                    )
                )
        return tuple(specs)

    schema_name = str(getattr(catalog, "db_schema", None) or "")
    for table_name in tuple(getattr(catalog, "tables", None) or ()):
        specs.append(
            CatalogTableSpec(
                schema_name=schema_name,
                source_table=str(table_name),
                target_table=transform_table_name(transform, schema_name, str(table_name)),
            )
        )
    return tuple(specs)


def build_single_table_execution_manifest(
    execution_manifest: Mapping[str, Any],
    spec: CatalogTableSpec,
) -> dict[str, Any]:
    manifest = deepcopy(dict(execution_manifest))
    source = dict(manifest.get("source") or {})
    schemas = source.get("schemas")
    if isinstance(schemas, Mapping):
        source["schemas"] = {spec.schema_name: {"tables": [spec.source_table]}}
        source.pop("schema", None)
        source.pop("tables", None)
    else:
        source["schema"] = spec.schema_name
        source["tables"] = [spec.source_table]
    manifest["source"] = source
    return manifest


def _is_missing_table_error(exc: Exception) -> bool:
    message = str(exc).lower()
    patterns = (
        "requested table(s) not available",
        "not available in engine",
        "undefinedtable",
        "no such table",
        "table or view does not exist",
        "does not exist",
    )
    return any(pattern in message for pattern in patterns)


def _should_tolerate_error(policy: PartialSuccessPolicy, exc: Exception) -> bool:
    if policy.tolerate_errors == "any_per_table":
        return True
    if policy.tolerate_errors == "missing_table_only":
        return _is_missing_table_error(exc)
    if policy.tolerate_errors == "source_only":
        if isinstance(exc, ReplaceProtectedExecutionError):
            return exc.stage == "source"
        return False
    return False


def _load_stage_label(exc: Exception) -> str:
    if isinstance(exc, ReplaceProtectedExecutionError):
        return str(exc.stage)
    return "unknown"


def _extract_error(exc: Exception) -> Exception:
    if isinstance(exc, ReplaceProtectedExecutionError):
        return exc.cause
    return exc


def _bytes_to_gib(value: Optional[int]) -> Optional[float]:
    if value is None:
        return None
    return float(value) / _GIB


def _now_like_ctx(ctx: RunContext) -> datetime:
    started_at = getattr(ctx, "started_at", None)
    tzinfo = getattr(started_at, "tzinfo", None)
    if tzinfo is not None:
        return datetime.now(tz=tzinfo)
    return datetime.utcnow().replace(tzinfo=None)


def _collect_clickhouse_stats(dataset: str, target_table: str) -> Optional[dict[str, Any]]:
    return get_table_storage_stats(dataset, target_table)


def _build_table_stats(
    *,
    spec: CatalogTableSpec,
    started_at: datetime,
    finished_at: datetime,
    status: str,
    load_info: Any = None,
    exc: Exception | None = None,
    before_stats: Mapping[str, Any] | None = None,
    after_stats: Mapping[str, Any] | None = None,
) -> TableRunStats:
    duration_seconds = max(0.0, (finished_at - started_at).total_seconds())
    metrics = extract_load_metrics(load_info) if load_info is not None else None
    rows_processed = metrics.rows_count if metrics else None
    rows_before = int(before_stats.get("rows_count", 0)) if before_stats is not None else None
    rows_after = int(after_stats.get("rows_count", 0)) if after_stats is not None else None
    bytes_before = int(before_stats.get("bytes_count", 0)) if before_stats is not None else None
    bytes_after = int(after_stats.get("bytes_count", 0)) if after_stats is not None else None

    delta_rows = None
    if rows_before is not None and rows_after is not None:
        delta_rows = rows_after - rows_before

    delta_pct = None
    if delta_rows is not None and rows_before not in (None, 0):
        delta_pct = (float(delta_rows) / float(rows_before)) * 100.0

    size_gb_before = _bytes_to_gib(bytes_before)
    size_gb_after = _bytes_to_gib(bytes_after)
    delta_gb = None
    if size_gb_before is not None and size_gb_after is not None:
        delta_gb = size_gb_after - size_gb_before

    rps = None
    if rows_processed is not None and duration_seconds > 0:
        rps = float(rows_processed) / duration_seconds

    gbps = None
    if delta_gb is not None and duration_seconds > 0:
        gbps = abs(delta_gb) / duration_seconds

    error_kind = None
    error_message = None
    if exc is not None:
        error = _extract_error(exc)
        error_info = classify_exception(error)
        error_kind = f"{error_info.category}:{error_info.code}"
        error_message = safe_exception_message(error, limit=1200)

    return TableRunStats(
        source_table=spec.source_table,
        target_table=spec.target_table,
        started_at=started_at,
        finished_at=finished_at,
        duration_seconds=duration_seconds,
        status=status,
        error_kind=error_kind,
        error_message=error_message,
        rows_processed=rows_processed,
        rows_before=rows_before,
        rows_after=rows_after,
        delta_rows=delta_rows,
        delta_pct=delta_pct,
        size_gb_before=size_gb_before,
        size_gb_after=size_gb_after,
        delta_gb=delta_gb,
        rps=rps,
        gbps=gbps,
    )


def _evaluate_policy(
    *,
    policy: PartialSuccessPolicy,
    results: Sequence[TableRunStats],
) -> tuple[str, str, tuple[str, ...]]:
    total_tables = len(results)
    succeeded = [item for item in results if item.status == "success"]
    failed = [item for item in results if item.status != "success"]
    success_count = len(succeeded)
    failed_tables = [item.source_table for item in failed]

    warnings: list[str] = []
    if failed_tables:
        warnings.append(
            "Skipped/failed tables: " + ", ".join(failed_tables)
        )

    if not failed:
        return "success", f"All {total_tables} table(s) loaded successfully.", tuple(warnings)

    if policy.mode == "any_success":
        if success_count >= 1:
            return (
                "partial_success",
                f"Loaded {success_count}/{total_tables} table(s); tolerated {len(failed)} table failure(s).",
                tuple(warnings),
            )
        return "failed", "Partial-success policy any_success was not satisfied: zero tables loaded.", tuple(warnings)

    if policy.mode == "critical_tables":
        critical = set(policy.critical_tables)
        failed_critical = [item.source_table for item in failed if item.source_table in critical]
        if failed_critical:
            warnings.append("Failed critical tables: " + ", ".join(failed_critical))
            return (
                "failed",
                "Partial-success policy critical_tables was not satisfied.",
                tuple(warnings),
            )
        return (
            "partial_success",
            f"Loaded {success_count}/{total_tables} table(s); all critical tables succeeded.",
            tuple(warnings),
        )

    if policy.mode == "threshold":
        ratio = float(success_count) / float(total_tables) if total_tables else 0.0
        enough_count = policy.min_success_tables is None or success_count >= int(policy.min_success_tables)
        enough_ratio = policy.min_success_ratio is None or ratio >= float(policy.min_success_ratio)
        if enough_count and enough_ratio:
            return (
                "partial_success",
                (
                    f"Loaded {success_count}/{total_tables} table(s); "
                    f"threshold satisfied (ratio={ratio:.3f})."
                ),
                tuple(warnings),
            )
        return "failed", f"Partial-success threshold was not satisfied (loaded {success_count}/{total_tables}).", tuple(warnings)

    return "failed", f"Unsupported partial-success mode: {policy.mode}", tuple(warnings)


def run_catalog_with_partial_success(
    *,
    config: ResolvedSqlDbConfig,
    ctx: RunContext,
    pipeline: Any,
    write_disposition: str,
) -> RunResult:
    policy = resolve_partial_success_policy(config.normalized_manifest)
    if policy is None:
        raise ValueError("Partial-success policy is not configured")

    table_specs = iter_catalog_table_specs(config)
    if not table_specs:
        raise ValueError("sqldb partial-success catalog execution requires at least one table")

    dataset = str((config.execution_manifest.get("pipeline") or {}).get("dataset") or ctx.dataset or "")
    execution_manifest = config.execution_manifest
    load_infos: list[Any] = []
    load_metrics = []
    table_stats: list[TableRunStats] = []

    ctx.logger.info(
        "Partial-success mode enabled: mode=%s tolerate_errors=%s tables=%d",
        policy.mode,
        policy.tolerate_errors,
        len(table_specs),
    )

    for spec in table_specs:
        started_at = _now_like_ctx(ctx)
        before_stats = _collect_clickhouse_stats(dataset, spec.target_table) if dataset else None
        ctx.logger.info(
            "Table load started: source=%s target=%s partial_success_mode=%s",
            spec.source_table,
            spec.target_table,
            policy.mode,
        )
        try:
            source_factory = build_single_catalog_table_source_factory(
                config,
                schema_name=spec.schema_name,
                source_table=spec.source_table,
            )
            single_manifest = build_single_table_execution_manifest(execution_manifest, spec)
            load_info = run_with_replace_protection(
                pipeline=pipeline,
                source_factory=source_factory,
                manifest=single_manifest,
                write_disposition=write_disposition,
                annotate_errors=True,
            )
        except Exception as exc:
            finished_at = _now_like_ctx(ctx)
            after_stats = _collect_clickhouse_stats(dataset, spec.target_table) if dataset else None
            stats = _build_table_stats(
                spec=spec,
                started_at=started_at,
                finished_at=finished_at,
                status="failed",
                exc=exc,
                before_stats=before_stats,
                after_stats=after_stats,
            )
            table_stats.append(stats)
            if _should_tolerate_error(policy, exc):
                ctx.logger.warning(
                    "Table load tolerated: source=%s target=%s stage=%s error=%s",
                    spec.source_table,
                    spec.target_table,
                    _load_stage_label(exc),
                    safe_exception_message(_extract_error(exc), limit=800),
                )
                continue

            message = (
                f"Partial-success aborted on table {spec.source_table}: "
                f"{safe_exception_message(_extract_error(exc), limit=1200)}"
            )
            return RunResult(
                status="failed",
                payload=load_infos[-1] if load_infos else None,
                load_metrics=merge_load_metrics(load_metrics),
                warnings=(
                    f"Non-tolerated table error at stage={_load_stage_label(exc)} "
                    f"for {spec.source_table}",
                ),
                table_stats=tuple(table_stats),
                message=message,
                finished_at=finished_at,
                duration_seconds=ctx.elapsed_seconds(finished_at),
            )

        finished_at = _now_like_ctx(ctx)
        after_stats = _collect_clickhouse_stats(dataset, spec.target_table) if dataset else None
        metrics = extract_load_metrics(load_info)
        if metrics is not None:
            load_metrics.append(metrics)
        load_infos.append(load_info)
        stats = _build_table_stats(
            spec=spec,
            started_at=started_at,
            finished_at=finished_at,
            status="success",
            load_info=load_info,
            before_stats=before_stats,
            after_stats=after_stats,
        )
        table_stats.append(stats)
        ctx.logger.info(
            "Table load finished: source=%s target=%s status=success duration_s=%.3f rows=%s",
            spec.source_table,
            spec.target_table,
            stats.duration_seconds,
            stats.rows_processed if stats.rows_processed is not None else "n/a",
        )

    status, message, warnings = _evaluate_policy(policy=policy, results=table_stats)
    finished_at = _now_like_ctx(ctx)
    return RunResult(
        status=status,
        payload=load_infos[-1] if load_infos else None,
        load_metrics=merge_load_metrics(load_metrics),
        warnings=warnings,
        table_stats=tuple(table_stats),
        message=message,
        finished_at=finished_at,
        duration_seconds=ctx.elapsed_seconds(finished_at),
    )


__all__ = [
    "CatalogTableSpec",
    "PartialSuccessPolicy",
    "build_single_table_execution_manifest",
    "iter_catalog_table_specs",
    "resolve_partial_success_policy",
    "run_catalog_with_partial_success",
]
