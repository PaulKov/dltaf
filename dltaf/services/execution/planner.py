from __future__ import annotations

import os
from typing import Any, Dict, List, Mapping, Optional, Sequence, cast

from dlt_utils.clickhouse_helpers import get_expected_table_names
from dltaf.services.execution.redaction import RedactionOptions, redact_obj
from dlt_utils.kafka_config import kafka_connection_from_manifest_or_env
from dltaf.extensions.infra_checks import (
    build_infra_check_registry,
    evaluate_infra_checks,
    run_infra_checks,
)


class DryRunStrictError(RuntimeError):
    def __init__(self, *, errors: Sequence[str], report: Mapping[str, Any], plan: Mapping[str, Any]):
        msg = "Dry-run strict checks failed:\n" + "\n".join([f"- {e}" for e in errors])
        super().__init__(msg)
        self.errors = list(errors)
        self.report = dict(report)
        self.plan = dict(plan)


class ExecutionPlanner:
    """Build run plans and evaluate non-executing run modes."""

    def summarize_env_sources(self, env_sources: Mapping[str, str]) -> Dict[str, int]:
        out: Dict[str, int] = {}
        for _env_key, src in (env_sources or {}).items():
            key = str(src or "").strip() or "unknown"
            out[key] = int(out.get(key, 0)) + 1
        return out

    def expected_tables_for_manifest(self, manifest: Mapping[str, Any]) -> List[str]:
        expected = get_expected_table_names(dict(manifest))
        if expected:
            return expected
        kind = str((manifest.get("source") or {}).get("kind") or "").strip()
        if kind == "pkb_conclusion":
            return ["pkb_conclusion"]
        if kind == "uploader_b057":
            return ["uploader_b057"]
        return []

    def build_run_plan(self, *, manifest: Mapping[str, Any], ctx: Any, hook_names: Sequence[str]) -> Dict[str, Any]:
        pipeline_cfg = manifest.get("pipeline") or {}
        run_cfg = manifest.get("run") or {}
        source_cfg = manifest.get("source") or {}
        write_disposition = str(run_cfg.get("write_disposition") or "replace")

        plan: Dict[str, Any] = {
            "pipeline": {
                "name": str(pipeline_cfg.get("name") or ""),
                "destination": str(pipeline_cfg.get("destination") or ""),
                "dataset": str(pipeline_cfg.get("dataset") or ""),
            },
            "source": {"kind": str(source_cfg.get("kind") or "")},
            "run": {
                "write_disposition": write_disposition,
                "validate_only": bool(getattr(ctx.options, "validate_only", False)),
                "explain_config": bool(getattr(ctx.options, "explain_config", False)),
                "plan": bool(getattr(ctx.options, "plan", False)),
                "dry_run": bool(getattr(ctx.options, "dry_run", False)),
                "dry_run_online": bool(getattr(ctx.options, "dry_run_online", False)),
                "dry_run_strict": bool(getattr(ctx.options, "dry_run_strict", False)),
            },
            "hooks": list(hook_names),
            "expected_tables": self.expected_tables_for_manifest(manifest),
            "env": {
                "keys": sorted(list((ctx.env_sources or {}).keys())),
                "sources_summary": self.summarize_env_sources(ctx.env_sources or {}),
            },
            "notes": [],
        }

        overrides = getattr(getattr(ctx, "options", None), "overrides", None) or {}
        if isinstance(overrides, Mapping) and overrides:
            sorted_overrides = {
                str(k): v for k, v in sorted(overrides.items(), key=lambda kv: str(kv[0]))
            }
            plan["overrides"] = redact_obj(
                sorted_overrides,
                options=RedactionOptions(max_depth=4, max_list=50, max_str=2048),
            )

        tw = source_cfg.get("time_window") if isinstance(source_cfg, Mapping) else None
        if isinstance(tw, Mapping) and (tw.get("start") or tw.get("end") or tw.get("timezone")):
            plan["source"]["time_window"] = {
                "start": tw.get("start"),
                "end": tw.get("end"),
                "timezone": tw.get("timezone"),
            }

        if str(source_cfg.get("kind") or "") in {"pkb_conclusion", "uploader_b057"}:
            bins_cfg = source_cfg.get("bins") or {}
            bins_hint: Dict[str, Any] = {}
            if isinstance(bins_cfg, Mapping):
                if bins_cfg.get("values") is not None:
                    vals = bins_cfg.get("values")
                    if isinstance(vals, (list, tuple)):
                        bins_hint["strategy"] = "values"
                        bins_hint["count"] = len([x for x in vals if str(x).strip()])
                    else:
                        bins_hint["strategy"] = "values"
                        bins_hint["value"] = "<non-list>"
                elif bins_cfg.get("from_env") is not None:
                    bins_hint["strategy"] = "from_env"
                    bins_hint["env"] = str(bins_cfg.get("from_env"))
                elif bins_cfg.get("from_file") is not None:
                    bins_hint["strategy"] = "from_file"
                    bins_hint["path"] = str(bins_cfg.get("from_file"))
                elif bins_cfg.get("from_clickhouse") is not None:
                    bins_hint["strategy"] = "from_clickhouse"
                    bins_hint["query"] = str((bins_cfg.get("from_clickhouse") or {}).get("query") or "")
            plan["source"]["bins"] = bins_hint

            conc_cfg = source_cfg.get("concurrency")
            if isinstance(conc_cfg, Mapping) and conc_cfg:
                plan["source"]["concurrency"] = {
                    "bins": conc_cfg.get("bins"),
                    "companies": conc_cfg.get("companies"),
                    "projects": conc_cfg.get("projects"),
                }

        kafka_cfg = (source_cfg.get("kafka") or {}) if isinstance(source_cfg, Mapping) else {}
        if isinstance(kafka_cfg, Mapping) and kafka_cfg:
            topic = kafka_cfg.get("topic")
            plan["source"]["kafka"] = {
                "topic": str(topic) if topic is not None else None,
                "bootstrap_servers_provided": bool(kafka_cfg.get("bootstrap_servers")),
            }
        return cast(Dict[str, Any], plan)

    def dry_run_checks(self, *, manifest: Mapping[str, Any], ctx: Any) -> Dict[str, Any]:
        pipeline_cfg = manifest.get("pipeline") or {}
        source_cfg = manifest.get("source") or {}
        connections_cfg = manifest.get("connections") or {}
        checks: Dict[str, Any] = {}

        if str(pipeline_cfg.get("destination") or "") == "clickhouse":
            host = os.getenv("DESTINATION__CLICKHOUSE__CREDENTIALS__HOST")
            username = os.getenv("DESTINATION__CLICKHOUSE__CREDENTIALS__USERNAME")
            password = os.getenv("DESTINATION__CLICKHOUSE__CREDENTIALS__PASSWORD")
            checks["clickhouse"] = {
                "host_present": bool(host),
                "http_port": os.getenv("DESTINATION__CLICKHOUSE__CREDENTIALS__HTTP_PORT") or None,
                "database": os.getenv("DESTINATION__CLICKHOUSE__CREDENTIALS__DATABASE") or None,
                "secure": os.getenv("DESTINATION__CLICKHOUSE__CREDENTIALS__SECURE") or None,
                "username_present": bool(username),
                "password_present": bool(password),
            }

        kind = str(source_cfg.get("kind") or "").strip()
        if kind in {"pkb_conclusion", "uploader_b057"}:
            kafka_cfg = source_cfg.get("kafka") or {}
            env_prefix = "KAFKA__"
            if isinstance(connections_cfg, Mapping):
                kconn = connections_cfg.get("kafka")
                if isinstance(kconn, Mapping) and kconn.get("env_prefix"):
                    env_prefix = str(kconn.get("env_prefix"))
            conn = kafka_connection_from_manifest_or_env(kafka_cfg, env_prefix=env_prefix)
            checks["kafka"] = {
                "env_prefix": env_prefix,
                "bootstrap_servers": list(conn.bootstrap_servers),
                "bootstrap_servers_present": bool(conn.bootstrap_servers),
                "topic": str((kafka_cfg or {}).get("topic") or "") if isinstance(kafka_cfg, Mapping) else None,
                "security_protocol": conn.security.security_protocol,
                "sasl_mechanism": conn.security.sasl_mechanism,
                "ssl_cafile_present": bool(conn.security.ssl_cafile),
            }

        if kind == "uploader_b057":
            grant_type = os.getenv("SOURCES__KEYCLOAK__GRANT_TYPE") or "password"
            checks["keycloak"] = {
                "token_url_present": bool(os.getenv("SOURCES__KEYCLOAK__TOKEN_URL")),
                "client_id_present": bool(os.getenv("SOURCES__KEYCLOAK__CLIENT_ID")),
                "client_secret_present": bool(os.getenv("SOURCES__KEYCLOAK__CLIENT_SECRET")),
                "username_present": bool(os.getenv("SOURCES__KEYCLOAK__USERNAME")),
                "password_present": bool(os.getenv("SOURCES__KEYCLOAK__PASSWORD")),
                "grant_type": str(grant_type),
            }

        if kind in {"pkb_conclusion", "uploader_b057"}:
            bins_cfg = source_cfg.get("bins") or {}
            bins_check: Dict[str, Any] = {"strategy": None}
            if isinstance(bins_cfg, Mapping):
                if bins_cfg.get("values") is not None:
                    vals = bins_cfg.get("values")
                    if isinstance(vals, (list, tuple)):
                        non_empty = [str(x).strip() for x in vals if str(x).strip()]
                        bins_check.update({"strategy": "values", "count": len(non_empty)})
                    else:
                        bins_check.update({"strategy": "values", "count": None})
                elif bins_cfg.get("from_env") is not None:
                    env_name = str(bins_cfg.get("from_env") or "").strip()
                    bins_check.update(
                        {
                            "strategy": "from_env",
                            "env": env_name,
                            "env_present": bool(os.getenv(env_name)) if env_name else False,
                        }
                    )
                elif bins_cfg.get("from_file") is not None:
                    path = str(bins_cfg.get("from_file") or "").strip()
                    bins_check.update(
                        {"strategy": "from_file", "path": path, "exists": bool(path and os.path.exists(path))}
                    )
                elif bins_cfg.get("from_clickhouse") is not None:
                    bins_check.update({"strategy": "from_clickhouse"})
            checks["bins"] = bins_check
        return checks

    def dry_run_online_checks(self, *, manifest: Mapping[str, Any], ctx: Any, timeout_seconds: float = 5.0) -> Dict[str, Any]:
        return run_infra_checks(manifest=manifest, ctx=ctx, timeout_seconds=float(timeout_seconds))

    def evaluate_dry_run_online_checks(
        self,
        checks: Mapping[str, Any],
        *,
        manifest: Optional[Mapping[str, Any]] = None,
        ctx: Any = None,
    ) -> Dict[str, Any]:
        if manifest is not None:
            registry, _plugin_report = build_infra_check_registry(
                manifest=manifest,
                ctx=ctx,
                include_env_plugins=True,
            )
            return evaluate_infra_checks(checks, registry=registry)
        return evaluate_infra_checks(checks)

    def merge_reports(self, *reports: Mapping[str, Any]) -> Dict[str, Any]:
        errors: List[str] = []
        warnings: List[str] = []
        for report in reports:
            if not isinstance(report, Mapping):
                continue
            for err in (report.get("errors") or []) if isinstance(report.get("errors"), list) else []:
                if str(err).strip():
                    errors.append(str(err))
            for warn in (report.get("warnings") or []) if isinstance(report.get("warnings"), list) else []:
                if str(warn).strip():
                    warnings.append(str(warn))
        return {"ok": len(errors) == 0, "errors": errors, "warnings": warnings}

    def evaluate_dry_run_checks(self, checks: Mapping[str, Any]) -> Dict[str, Any]:
        errors: List[str] = []
        warnings: List[str] = []

        ch = checks.get("clickhouse")
        if isinstance(ch, Mapping):
            if not bool(ch.get("host_present")):
                errors.append("ClickHouse host is not configured (DESTINATION__CLICKHOUSE__CREDENTIALS__HOST).")
            if not ch.get("database"):
                warnings.append("ClickHouse database is not configured (DESTINATION__CLICKHOUSE__CREDENTIALS__DATABASE).")
            if not bool(ch.get("username_present")):
                warnings.append("ClickHouse username is not explicitly set (DESTINATION__CLICKHOUSE__CREDENTIALS__USERNAME).")
            if not bool(ch.get("password_present")):
                warnings.append(
                    "ClickHouse password is not set (DESTINATION__CLICKHOUSE__CREDENTIALS__PASSWORD). This may be OK for dev, but usually required in prod."
                )

        kafka = checks.get("kafka")
        if isinstance(kafka, Mapping):
            if not bool(kafka.get("bootstrap_servers_present")):
                errors.append(f"Kafka bootstrap servers are missing (env_prefix={kafka.get('env_prefix')!r}).")
            topic = str(kafka.get("topic") or "").strip()
            if not topic:
                warnings.append("Kafka topic is not set (source.kafka.topic). Default may be used.")

        keycloak = checks.get("keycloak")
        if isinstance(keycloak, Mapping):
            if not bool(keycloak.get("token_url_present")):
                errors.append("Keycloak token URL is missing (SOURCES__KEYCLOAK__TOKEN_URL).")
            if not bool(keycloak.get("client_id_present")):
                errors.append("Keycloak client_id is missing (SOURCES__KEYCLOAK__CLIENT_ID).")
            grant_type = str(keycloak.get("grant_type") or "password").strip()
            if grant_type == "password":
                if not bool(keycloak.get("username_present")):
                    errors.append("Keycloak username is required for grant_type=password (SOURCES__KEYCLOAK__USERNAME).")
                if not bool(keycloak.get("password_present")):
                    errors.append("Keycloak password is required for grant_type=password (SOURCES__KEYCLOAK__PASSWORD).")
            elif grant_type == "client_credentials":
                if not bool(keycloak.get("client_secret_present")):
                    errors.append(
                        "Keycloak client_secret is required for grant_type=client_credentials (SOURCES__KEYCLOAK__CLIENT_SECRET)."
                    )
            else:
                warnings.append(f"Keycloak grant_type={grant_type!r} is not recognized by dry-run checks.")

        bins = checks.get("bins")
        if isinstance(bins, Mapping):
            strategy = str(bins.get("strategy") or "").strip()
            if strategy == "values":
                try:
                    count = int(bins.get("count") or 0)
                except Exception:
                    count = -1
                if count == 0:
                    warnings.append("BINs list is empty in source.bins.values.")
            elif strategy == "from_env":
                env_name = str(bins.get("env") or "").strip()
                if env_name and not bool(bins.get("env_present")):
                    warnings.append(f"BINs env var is not set or empty: {env_name}")
            elif strategy == "from_file":
                path = str(bins.get("path") or "").strip()
                if path and not bool(bins.get("exists")):
                    warnings.append(f"BINs file does not exist: {path}")

        return {"ok": len(errors) == 0, "errors": errors, "warnings": warnings}

    def execute_plan_or_dry_run(
        self,
        *,
        manifest: Mapping[str, Any],
        ctx: Any,
        hooks: Sequence[Any],
        plan_output: Optional[str],
        plan_format: Optional[str],
        online_timeout_seconds: float,
    ) -> Mapping[str, Any]:
        hook_names = [getattr(h, "name", h.__class__.__name__) for h in hooks]
        plan = self.build_run_plan(manifest=manifest, ctx=ctx, hook_names=hook_names)

        offline_report = {"ok": True, "errors": [], "warnings": []}
        online_report = {"ok": True, "errors": [], "warnings": []}

        if bool(getattr(ctx.options, "dry_run", False)):
            offline_warnings = cast(List[str], offline_report.get("warnings") or [])
            offline_errors = cast(List[str], offline_report.get("errors") or [])
            if offline_warnings:
                ctx.logger.warning("Dry-run warnings:\n%s", "\n".join([f"- {w}" for w in offline_warnings]))
            if offline_errors:
                ctx.logger.error("Dry-run errors:\n%s", "\n".join([f"- {e}" for e in offline_errors]))
            if bool(getattr(ctx.options, "dry_run_online", False)):
                online_warnings = cast(List[str], online_report.get("warnings") or [])
                online_errors = cast(List[str], online_report.get("errors") or [])
                if online_warnings:
                    ctx.logger.warning(
                        "Dry-run online warnings:\n%s",
                        "\n".join([f"- {w}" for w in online_warnings]),
                    )
                if online_errors:
                    ctx.logger.error(
                        "Dry-run online errors:\n%s",
                        "\n".join([f"- {e}" for e in online_errors]),
                    )
            merged = self.merge_reports(offline_report, online_report)
            if bool(getattr(ctx.options, "dry_run_strict", False)) and not bool(merged.get("ok")):
                raise DryRunStrictError(errors=cast(List[str], merged.get("errors") or []), report=merged, plan=plan)

        return cast(Dict[str, Any], plan)


_default_planner = ExecutionPlanner()


def build_run_plan(*, manifest: Mapping[str, Any], ctx: Any, hook_names: Sequence[str]) -> Dict[str, Any]:
    return _default_planner.build_run_plan(manifest=manifest, ctx=ctx, hook_names=hook_names)


def dry_run_checks(*, manifest: Mapping[str, Any], ctx: Any) -> Dict[str, Any]:
    return _default_planner.dry_run_checks(manifest=manifest, ctx=ctx)


def dry_run_online_checks(*, manifest: Mapping[str, Any], ctx: Any, timeout_seconds: float = 5.0) -> Dict[str, Any]:
    return _default_planner.dry_run_online_checks(manifest=manifest, ctx=ctx, timeout_seconds=timeout_seconds)


def evaluate_dry_run_checks(checks: Mapping[str, Any]) -> Dict[str, Any]:
    return _default_planner.evaluate_dry_run_checks(checks)


def evaluate_dry_run_online_checks(
    checks: Mapping[str, Any],
    *,
    manifest: Optional[Mapping[str, Any]] = None,
    ctx: Any = None,
) -> Dict[str, Any]:
    return _default_planner.evaluate_dry_run_online_checks(checks, manifest=manifest, ctx=ctx)


def merge_reports(*reports: Mapping[str, Any]) -> Dict[str, Any]:
    return _default_planner.merge_reports(*reports)


def execute_plan_or_dry_run(
    *,
    manifest: Mapping[str, Any],
    ctx: Any,
    hooks: Sequence[Any],
    plan_output: Optional[str],
    plan_format: Optional[str],
    online_timeout_seconds: float,
) -> Mapping[str, Any]:
    return _default_planner.execute_plan_or_dry_run(
        manifest=manifest,
        ctx=ctx,
        hooks=hooks,
        plan_output=plan_output,
        plan_format=plan_format,
        online_timeout_seconds=online_timeout_seconds,
    )


__all__ = [
    "DryRunStrictError",
    "ExecutionPlanner",
    "build_run_plan",
    "dry_run_checks",
    "dry_run_online_checks",
    "evaluate_dry_run_checks",
    "evaluate_dry_run_online_checks",
    "merge_reports",
    "execute_plan_or_dry_run",
]
