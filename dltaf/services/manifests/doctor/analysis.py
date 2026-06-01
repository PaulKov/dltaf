from __future__ import annotations

from pathlib import Path
from typing import Any, List, Mapping

from .models import Issue


def expected_pipeline_name(path: Path) -> str:
    return path.stem


def pick_source_kind(data: Mapping[str, Any]) -> str:
    source = data.get("source") or {}
    if not isinstance(source, Mapping):
        return ""
    return str(source.get("kind") or "").strip()


def bins_strategy_count(bins_cfg: Mapping[str, Any]) -> int:
    strategies = 0
    values = bins_cfg.get("values")
    if isinstance(values, list) and any(str(item).strip() for item in values):
        strategies += 1
    if str(bins_cfg.get("from_file") or "").strip():
        strategies += 1
    if str(bins_cfg.get("from_env") or "").strip():
        strategies += 1
    from_clickhouse = bins_cfg.get("from_clickhouse")
    if isinstance(from_clickhouse, Mapping) and len(from_clickhouse) > 0:
        strategies += 1
    return strategies


def _append_sqldb_canonicalization_issues(issues: List[Issue], *, source: Mapping[str, Any], kind: str) -> None:
    """Manifest doctor guidance for deprecated/non-canonical SQL source kinds.

    We intentionally only fix root ``source.kind`` + ``source.dialect`` + ``source.mode``.
    Legacy top-level keys (``schema``, ``tables``, ``queries``, ``init_sql``, ...) remain
    valid thanks to the SQLDB normalizer, so ``--apply`` can stay safe and comment-preserving.
    """
    mapping = {
        "sql_database": ("generic", "catalog", "deprecated legacy alias"),
        "oracle_custom_sql": ("oracle", "query", "deprecated legacy alias"),
        "oracle": ("oracle", "query", "non-canonical preset alias"),
    }
    if kind not in mapping:
        return
    dialect, mode, qualifier = mapping[kind]
    issues.append(
        Issue(
            "FIX",
            "SQL_KIND_CANONICALIZE",
            f"`source.kind={kind}` is a {qualifier}. Preferred canonical form is "
            f"`source.kind: sqldb` with `dialect: {dialect}` and `mode: {mode}`.",
            ("source", "kind", "sqldb"),
        )
    )
    if str(source.get("dialect") or "").strip() != dialect:
        issues.append(
            Issue(
                "FIX",
                "SQL_KIND_SET_DIALECT",
                f"Add/update `source.dialect: {dialect}` for canonical `sqldb` manifest.",
                ("source", "dialect", dialect),
            )
        )
    if str(source.get("mode") or "").strip() != mode:
        issues.append(
            Issue(
                "FIX",
                "SQL_KIND_SET_MODE",
                f"Add/update `source.mode: {mode}` for canonical `sqldb` manifest.",
                ("source", "mode", mode),
            )
        )


def _append_sqldb_layout_guidance(issues: List[Issue], *, source: Mapping[str, Any]) -> None:
    """Warn when sqldb manifests still use pre-canonical top-level SQL keys."""
    legacy_catalog_keys = {"schema", "schemas", "tables", "table_adapter_callback", "reflection_level", "backend", "defer_table_reflect"}
    legacy_query_keys = {"queries", "fetch_batch_size", "init_sql", "thick_mode"}
    has_legacy_catalog = any(key in source for key in legacy_catalog_keys)
    has_legacy_query = any(key in source for key in legacy_query_keys)
    mode = str(source.get("mode") or "").strip()

    if has_legacy_catalog and mode == "catalog":
        issues.append(
            Issue(
                "WARN",
                "SQLDB_LEGACY_CATALOG_LAYOUT",
                "Canonical `sqldb` manifests should place table/schema extraction options under "
                "`source.catalog`. Legacy top-level SQL keys are still supported, but only as a "
                "compatibility layout.",
            )
        )
    if has_legacy_query and mode == "query":
        issues.append(
            Issue(
                "WARN",
                "SQLDB_LEGACY_QUERY_LAYOUT",
                "Canonical `sqldb` manifests should place query extraction options under "
                "`source.query` and Oracle-specific tweaks under `source.dialect_options`. "
                "Legacy top-level SQL keys are still supported, but only as a compatibility layout.",
            )
        )


def analyze_manifest(path: Path, data: Mapping[str, Any]) -> List[Issue]:
    issues: List[Issue] = []
    expected_name = expected_pipeline_name(path)

    version = data.get("version")
    if version is None:
        issues.append(Issue("FIX", "VERSION_MISSING", "Отсутствует ключ `version`. Рекомендуется добавить `version: 1`.", ("__root__", "version", "1")))
    elif version != 1:
        issues.append(Issue("ERROR", "VERSION_UNSUPPORTED", f"Неподдерживаемая версия манифеста: version={version!r} (ожидается 1)"))

    pipeline = data.get("pipeline")
    if not isinstance(pipeline, Mapping):
        issues.append(Issue("ERROR", "PIPELINE_MISSING", "Отсутствует или некорректна секция `pipeline:` (должна быть mapping)."))
        return issues

    name = str(pipeline.get("name") or "").strip()
    if not name:
        issues.append(Issue("FIX", "PIPELINE_NAME_MISSING", f"`pipeline.name` отсутствует. Рекомендуется установить: {expected_name}", ("pipeline", "name", expected_name)))
    elif name != expected_name:
        issues.append(Issue("FIX", "PIPELINE_NAME_MISMATCH", "`pipeline.name` должен совпадать с именем файла манифеста. " f"Сейчас: {name!r}, ожидается: {expected_name!r}", ("pipeline", "name", expected_name)))

    airflow = data.get("airflow")
    if airflow is not None:
        if not isinstance(airflow, Mapping):
            issues.append(Issue("ERROR", "AIRFLOW_NOT_MAPPING", "Секция `airflow:` должна быть mapping (YAML object)."))
        else:
            dag_id = str(airflow.get("dag_id") or "").strip()
            if not dag_id:
                issues.append(Issue("FIX", "AIRFLOW_DAG_ID_MISSING", f"`airflow.dag_id` отсутствует. Рекомендуется установить: {expected_name}", ("airflow", "dag_id", expected_name)))
            elif dag_id != expected_name:
                issues.append(Issue("FIX", "AIRFLOW_DAG_ID_MISMATCH", "`airflow.dag_id` должен совпадать с `pipeline.name`/именем файла. " f"Сейчас: {dag_id!r}, ожидается: {expected_name!r}", ("airflow", "dag_id", expected_name)))

    kind = pick_source_kind(data)
    source = data.get("source") or {}
    if isinstance(source, Mapping):
        _append_sqldb_canonicalization_issues(issues, source=source, kind=kind)
        if kind == "sqldb":
            _append_sqldb_layout_guidance(issues, source=source)

    if isinstance(source, Mapping) and kind in {"uploader_b057", "pkb_conclusion"}:
        bins_cfg = source.get("bins") or {}
        if not isinstance(bins_cfg, Mapping):
            issues.append(Issue("WARN", "BINS_NOT_MAPPING", "`source.bins` должен быть mapping (YAML object)."))
        else:
            strategies = bins_strategy_count(bins_cfg)
            if strategies == 0:
                issues.append(Issue("WARN", "BINS_STRATEGY_EMPTY", "Не выбран источник BIN-ов. Укажите одну из стратегий: values / from_file / from_env / from_clickhouse."))
            elif strategies > 1:
                issues.append(Issue("WARN", "BINS_STRATEGY_MULTIPLE", "Выбрано сразу несколько стратегий BIN-ов. Оставьте только одну: values / from_file / from_env / from_clickhouse."))

    if isinstance(source, Mapping) and kind == "pkb_conclusion":
        if "base_url" in source and str(source.get("base_url") or "").strip() == "":
            issues.append(Issue("WARN", "PKB_BASE_URL_EMPTY", "`source.base_url` задан как пустая строка. Это переопределит дефолт и сломает HTTP URL. Удалите ключ или укажите корректный base_url (или ENV-placeholder)."))
        kafka_cfg = source.get("kafka") or {}
        if isinstance(kafka_cfg, Mapping) and "topic" in kafka_cfg and str(kafka_cfg.get("topic") or "").strip() == "":
            issues.append(Issue("WARN", "PKB_KAFKA_TOPIC_EMPTY", "`source.kafka.topic` задан как пустая строка. Это переопределит дефолт и сломает ожидание Kafka. Удалите ключ или укажите корректный topic."))

    return issues


__all__ = ["analyze_manifest", "bins_strategy_count", "expected_pipeline_name", "pick_source_kind"]
