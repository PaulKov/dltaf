"""Persistent unit-level checkpoints.

This module intentionally stays small and framework-owned:

* Runners decide what a unit is and which rows it produced.
* The core checkpoint store persists successful unit payloads by
  ``(pipeline_name, load_uuid, batch_key, resume_key)``.
* Retries can restore rows from the checkpoint and still perform the normal
  destination cleanup/load path.

The important safety rule is that we checkpoint rows, not just a "unit done"
flag. A retry must never skip an API/Kafka unit if the previous attempt timed
out before the rows were loaded into the destination.
"""

from __future__ import annotations

import json
import os
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Dict, Iterable, Mapping, Optional, Sequence

from dlt_utils.core.run_result import UnitRunStats
from dltaf.services.execution.redaction import safe_exception_message, safe_json


DEFAULT_CHECKPOINT_TABLE = "_pipeline_unit_checkpoints"
SUCCESS_STATUSES = ("success",)


def _quote_ident(name: str) -> str:
    return "`" + str(name).replace("`", "``") + "`"


def _query_literal(value: str) -> str:
    return "'" + str(value).replace("\\", "\\\\").replace("'", "\\'") + "'"


def _json_default(value: Any) -> str:
    if isinstance(value, datetime):
        return value.isoformat()
    return str(value)


def _to_json(value: Any) -> str:
    try:
        return json.dumps(value, ensure_ascii=False, default=_json_default)
    except Exception:
        return safe_json(value)


def _loads_json(value: Optional[str], fallback: Any) -> Any:
    if value in {None, ""}:
        return fallback
    try:
        return json.loads(str(value))
    except Exception:
        return fallback


def _parse_datetime(value: Any) -> datetime:
    if isinstance(value, datetime):
        return value
    text = str(value or "").strip()
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    try:
        return datetime.fromisoformat(text)
    except Exception:
        return datetime.now(timezone.utc)


def build_unit_resume_key(unit_kind: str, unit_id: str) -> str:
    """Build the canonical resume key for a runner-defined unit."""

    return f"{str(unit_kind).strip()}:{str(unit_id).strip()}"


@dataclass(frozen=True)
class UnitCheckpointConfig:
    """Resolved checkpoint settings for a run."""

    enabled: bool = False
    load_uuid: str = ""
    batch_key: str = "default"
    table_name: str = DEFAULT_CHECKPOINT_TABLE
    resume_statuses: Sequence[str] = SUCCESS_STATUSES


@dataclass(frozen=True)
class UnitCheckpointRecord:
    """Previously checkpointed successful unit payload."""

    resume_key: str
    unit_stat: UnitRunStats
    rows: Sequence[Mapping[str, Any]]


def _boolish(value: Any, *, default: bool = False) -> bool:
    if value in {None, ""}:
        return default
    if isinstance(value, bool):
        return value
    return str(value).strip().lower() in {"1", "true", "yes", "y", "on"}


def _checkpoint_cfg(manifest: Mapping[str, Any]) -> Mapping[str, Any]:
    run_cfg = manifest.get("run") or {}
    if not isinstance(run_cfg, Mapping):
        return {}
    raw = run_cfg.get("checkpoint")
    if raw is None:
        raw = run_cfg.get("checkpoints")
    return raw if isinstance(raw, Mapping) else {}


def resolve_unit_checkpoint_config(
    manifest: Mapping[str, Any],
    ctx: Any,
    *,
    default_batch_key: str = "default",
) -> UnitCheckpointConfig:
    """Resolve manifest/env checkpoint config.

    Default ``load_uuid`` order is deliberately retry-friendly:
    ``run.checkpoint.load_uuid`` -> ``DLTAF_LOAD_UUID`` ->
    ``AIRFLOW_CTX_DAG_RUN_ID`` -> generated framework ``ctx.run_id``.
    """

    cfg = _checkpoint_cfg(manifest)
    if not cfg:
        return UnitCheckpointConfig()

    enabled = _boolish(cfg.get("enabled"), default=False)
    load_uuid = str(
        cfg.get("load_uuid")
        or os.getenv("DLTAF_LOAD_UUID")
        or os.getenv("AIRFLOW_CTX_DAG_RUN_ID")
        or getattr(ctx, "run_id", "")
        or ""
    ).strip()
    batch_key = str(cfg.get("batch_key") or default_batch_key or "default").strip() or "default"
    table_name = str(cfg.get("table") or cfg.get("table_name") or DEFAULT_CHECKPOINT_TABLE).strip()
    if not table_name:
        table_name = DEFAULT_CHECKPOINT_TABLE

    statuses_raw = cfg.get("resume_statuses") or cfg.get("statuses") or SUCCESS_STATUSES
    if isinstance(statuses_raw, str):
        statuses = tuple(item.strip().lower() for item in statuses_raw.split(",") if item.strip())
    elif isinstance(statuses_raw, Iterable):
        statuses = tuple(str(item).strip().lower() for item in statuses_raw if str(item).strip())
    else:
        statuses = SUCCESS_STATUSES

    return UnitCheckpointConfig(
        enabled=enabled,
        load_uuid=load_uuid,
        batch_key=batch_key,
        table_name=table_name,
        resume_statuses=statuses or SUCCESS_STATUSES,
    )


def unit_stats_from_dict(data: Mapping[str, Any]) -> UnitRunStats:
    """Rehydrate UnitRunStats from JSON-safe dict storage."""

    return UnitRunStats(
        unit_kind=str(data.get("unit_kind") or ""),
        unit_id=str(data.get("unit_id") or ""),
        ordinal=int(data.get("ordinal") or 0),
        started_at=_parse_datetime(data.get("started_at")),
        finished_at=_parse_datetime(data.get("finished_at")),
        duration_seconds=float(data.get("duration_seconds") or 0.0),
        status=str(data.get("status") or ""),
        stage=str(data.get("stage") or ""),
        rows_emitted=data.get("rows_emitted"),
        retry_count=data.get("retry_count"),
        warnings_count=data.get("warnings_count"),
        external_id=data.get("external_id"),
        outcome_code=data.get("outcome_code"),
        error_kind=data.get("error_kind"),
        error_message=data.get("error_message"),
        details=data.get("details") if isinstance(data.get("details"), Mapping) else None,
        load_uuid=data.get("load_uuid"),
        batch_key=data.get("batch_key"),
        resume_key=data.get("resume_key"),
    )


def build_resumed_unit_stat(record: UnitCheckpointRecord, *, ordinal: Optional[int] = None) -> UnitRunStats:
    """Build a fresh success stat for a checkpoint-resumed unit."""

    now = datetime.now(timezone.utc)
    previous = record.unit_stat
    return UnitRunStats(
        unit_kind=previous.unit_kind,
        unit_id=previous.unit_id,
        ordinal=int(ordinal if ordinal is not None else previous.ordinal),
        started_at=now,
        finished_at=now,
        duration_seconds=0.0,
        status="success",
        stage="checkpoint_resume",
        rows_emitted=len(record.rows),
        retry_count=previous.retry_count,
        warnings_count=previous.warnings_count,
        external_id=previous.external_id,
        outcome_code="resumed",
        error_kind=None,
        error_message=None,
        details=previous.details,
        load_uuid=previous.load_uuid,
        batch_key=previous.batch_key,
        resume_key=record.resume_key,
    )


class ClickHouseUnitCheckpointStore:
    """ClickHouse-backed checkpoint store.

    The store is intentionally best-effort at the call sites. This class raises
    regular exceptions so runners can decide whether to warn or fail.
    """

    def __init__(
        self,
        *,
        client: Any,
        database: str,
        table_name: str = DEFAULT_CHECKPOINT_TABLE,
    ) -> None:
        self.client = client
        self.database = str(database or "default")
        self.table_name = str(table_name or DEFAULT_CHECKPOINT_TABLE)

    @classmethod
    def from_env(cls, *, table_name: str = DEFAULT_CHECKPOINT_TABLE) -> Optional["ClickHouseUnitCheckpointStore"]:
        from dlt_utils.clickhouse_helpers import get_clickhouse_client

        client = get_clickhouse_client()
        if client is None:
            return None
        database = os.getenv("DESTINATION__CLICKHOUSE__CREDENTIALS__DATABASE", "default")
        return cls(client=client, database=database, table_name=table_name)

    def ensure_table(self) -> None:
        self.client.command(f"CREATE DATABASE IF NOT EXISTS {_quote_ident(self.database)}")
        ddl = f"""
CREATE TABLE IF NOT EXISTS {_quote_ident(self.database)}.{_quote_ident(self.table_name)} (
  pipeline_name String,
  source_kind LowCardinality(String),
  load_uuid String,
  batch_key String,
  resume_key String,
  run_id String,
  unit_kind LowCardinality(String),
  unit_id String,
  status LowCardinality(String),
  outcome_code Nullable(String),
  rows_emitted Nullable(UInt64),
  updated_at DateTime64(3, 'UTC'),
  unit_stats_json String,
  rows_json String,
  details_json Nullable(String)
) ENGINE = ReplacingMergeTree(updated_at)
ORDER BY (pipeline_name, load_uuid, batch_key, resume_key)
""".strip()
        self.client.command(ddl)

    def record_unit(
        self,
        *,
        ctx: Any,
        load_uuid: str,
        batch_key: str,
        resume_key: str,
        unit_stat: UnitRunStats,
        rows: Sequence[Mapping[str, Any]],
    ) -> None:
        self.ensure_table()
        stat = UnitRunStats(
            unit_kind=unit_stat.unit_kind,
            unit_id=unit_stat.unit_id,
            ordinal=unit_stat.ordinal,
            started_at=unit_stat.started_at,
            finished_at=unit_stat.finished_at,
            duration_seconds=unit_stat.duration_seconds,
            status=unit_stat.status,
            stage=unit_stat.stage,
            rows_emitted=unit_stat.rows_emitted,
            retry_count=unit_stat.retry_count,
            warnings_count=unit_stat.warnings_count,
            external_id=unit_stat.external_id,
            outcome_code=unit_stat.outcome_code,
            error_kind=unit_stat.error_kind,
            error_message=unit_stat.error_message,
            details=unit_stat.details,
            load_uuid=load_uuid,
            batch_key=batch_key,
            resume_key=resume_key,
        )
        details_json = _to_json(dict(stat.details or {})) if stat.details else None
        cols = [
            "pipeline_name",
            "source_kind",
            "load_uuid",
            "batch_key",
            "resume_key",
            "run_id",
            "unit_kind",
            "unit_id",
            "status",
            "outcome_code",
            "rows_emitted",
            "updated_at",
            "unit_stats_json",
            "rows_json",
            "details_json",
        ]
        values = [
            getattr(ctx, "pipeline_name", ""),
            getattr(ctx, "source_kind", ""),
            load_uuid,
            batch_key,
            resume_key,
            getattr(ctx, "run_id", ""),
            stat.unit_kind,
            stat.unit_id,
            stat.status,
            stat.outcome_code,
            stat.rows_emitted,
            datetime.now(timezone.utc),
            stat.to_json(include_none=True),
            _to_json(list(rows)),
            details_json,
        ]
        self.client.insert(f"{self.database}.{self.table_name}", [values], column_names=cols)

    def load_completed_units(
        self,
        *,
        pipeline_name: str,
        load_uuid: str,
        batch_key: str,
        statuses: Sequence[str] = SUCCESS_STATUSES,
    ) -> Dict[str, UnitCheckpointRecord]:
        self.ensure_table()
        normalized_statuses = tuple(str(item).strip().lower() for item in statuses if str(item).strip())
        status_sql = ", ".join(_query_literal(item) for item in (normalized_statuses or SUCCESS_STATUSES))
        sql = f"""
SELECT resume_key, unit_stats_json, rows_json
FROM {_quote_ident(self.database)}.{_quote_ident(self.table_name)}
WHERE pipeline_name = {_query_literal(pipeline_name)}
  AND load_uuid = {_query_literal(load_uuid)}
  AND batch_key = {_query_literal(batch_key)}
  AND status IN ({status_sql})
ORDER BY updated_at ASC
""".strip()
        result = self.client.query(sql)
        records: Dict[str, UnitCheckpointRecord] = {}
        for row in getattr(result, "result_rows", []) or []:
            record = self.to_record(
                resume_key=str(row[0]),
                unit_stats_json=str(row[1] or "{}"),
                rows_json=str(row[2] or "[]"),
            )
            records[record.resume_key] = record
        return records

    @staticmethod
    def to_record(*, resume_key: str, unit_stats_json: str, rows_json: str) -> UnitCheckpointRecord:
        stat_payload = _loads_json(unit_stats_json, {})
        rows_payload = _loads_json(rows_json, [])
        stat = unit_stats_from_dict(stat_payload if isinstance(stat_payload, Mapping) else {})
        rows = rows_payload if isinstance(rows_payload, list) else []
        return UnitCheckpointRecord(
            resume_key=str(resume_key),
            unit_stat=stat,
            rows=[row for row in rows if isinstance(row, Mapping)],
        )


def load_checkpoint_records(
    *,
    ctx: Any,
    config: UnitCheckpointConfig,
) -> Dict[str, UnitCheckpointRecord]:
    """Best-effort convenience loader for runners."""

    if not config.enabled:
        return {}
    store = ClickHouseUnitCheckpointStore.from_env(table_name=config.table_name)
    if store is None:
        return {}
    try:
        return store.load_completed_units(
            pipeline_name=getattr(ctx, "pipeline_name", ""),
            load_uuid=config.load_uuid,
            batch_key=config.batch_key,
            statuses=config.resume_statuses,
        )
    except Exception as exc:
        logger = getattr(ctx, "logger", None)
        if logger is not None:
            logger.warning("Unit checkpoint load failed: %s", safe_exception_message(exc))
        return {}


def record_unit_checkpoint(
    *,
    ctx: Any,
    config: UnitCheckpointConfig,
    resume_key: str,
    unit_stat: UnitRunStats,
    rows: Sequence[Mapping[str, Any]],
) -> None:
    """Best-effort convenience writer for runners."""

    if not config.enabled or not rows:
        return
    store = ClickHouseUnitCheckpointStore.from_env(table_name=config.table_name)
    if store is None:
        return
    try:
        store.record_unit(
            ctx=ctx,
            load_uuid=config.load_uuid,
            batch_key=config.batch_key,
            resume_key=resume_key,
            unit_stat=unit_stat,
            rows=rows,
        )
    except Exception as exc:
        logger = getattr(ctx, "logger", None)
        if logger is not None:
            logger.warning("Unit checkpoint write failed: %s", safe_exception_message(exc))


__all__ = [
    "ClickHouseUnitCheckpointStore",
    "DEFAULT_CHECKPOINT_TABLE",
    "UnitCheckpointConfig",
    "UnitCheckpointRecord",
    "build_resumed_unit_stat",
    "build_unit_resume_key",
    "load_checkpoint_records",
    "record_unit_checkpoint",
    "resolve_unit_checkpoint_config",
    "unit_stats_from_dict",
]
