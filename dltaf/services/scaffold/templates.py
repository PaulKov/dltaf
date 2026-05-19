from __future__ import annotations


def render_manifest_yaml(
    *,
    pipeline_name: str,
    source_kind: str,
    package_name: str,
    destination: str,
    dataset: str,
    schedule: str,
    runner_plugin_module: str,
    infra_checks_module: str,
) -> str:
    return f'''version: 1
pipeline:
  name: {pipeline_name}
  destination: {destination}
  dataset: {dataset}
  progress: log
  dev_mode: false

run:
  write_disposition: merge

  runners:
    plugins:
      - {runner_plugin_module}

  online_checks:
    plugins:
      - {infra_checks_module}

connections:
  destination:
    kind: clickhouse
    vault: ${{ENV:CLICKHOUSE__VAULT_REF|}}
    overrides:
      database: ${{ENV:{pipeline_name.upper()}__CLICKHOUSE_DB|{dataset}}}
      dataset_table_separator: __

  kafka:
    kind: kafka
    vault: ${{ENV:KAFKA__VAULT_REF|}}
    env_prefix: KAFKA__
    airflow_variable_prefix: KAFKA__

source:
  kind: {source_kind}

  # TODO: add source-specific config keys below.
  # base_url: https://example.com
  # endpoint: /api/v1/export
  #
  # For the built-in SQL family, prefer canonical SQLDB manifests instead of
  # plugin scaffolding:
  #   dltaf manifest doctor --template-kind sqldb_catalog
  #   dltaf manifest doctor --template-kind sqldb_query
  #
  # Canonical SQL shape:
  #   kind: sqldb
  #   dialect: generic|oracle
  #   mode: catalog|query

  # Controlled parallelism (Stage 29)
  # concurrency:
  #   bins: 1
  #   projects: 5

  # Backfill time window (Stage 30)
  # time_window:
  #   start: "2026-01-01"
  #   end: "2026-02-01"

  # Payload contract (Stage 27)
  # payload_contract:
  #   mode: off
  #   schema_path: dlt_pipelines/{package_name}/contracts/payload.schema.json
  #   max_errors: 20

airflow:
  dag_id: {pipeline_name}
  schedule: "{schedule}"
  catchup: false
  max_active_runs: 1
'''


def render_integration_readme(*, pipeline_name: str, source_kind: str, package_name: str, template: str) -> str:
    return f'''# Integration: `{source_kind}`

> Scaffold template: `{template}`

This folder contains a **plugin-first** integration scaffold.\n\n> For the built-in SQL family, prefer `dltaf manifest doctor --template-kind sqldb_catalog|sqldb_query`\n> and canonical `source.kind: sqldb` instead of scaffolding a custom plugin package.

## Files

- `{source_kind}_source.py` — dlt source (edit this)
- `runner_plugin.py` — registers SourceRunner for `source.kind={source_kind}`
- `infra_checks.py` — optional online checks plugin (`--dry-run-online`)
- `contracts/payload.schema.json` — JSON Schema contract
- `README.md` — this document

## Architecture

```mermaid
flowchart TD
  A[Airflow DAG] --> MR[dlt_utils.manifest_runner]
  MR --> ORCH[Executor]
  ORCH --> REG[RunnerRegistry]
  REG --> R[Runner plugin: {source_kind}]
  R --> HTTP[HTTP/API]
  R --> K[Kafka wait by key]
  R --> DLT[dlt.pipeline.run]
  DLT --> CH[(ClickHouse)]
```

## How to run

```bash
dltaf manifest lint --manifest dlt_pipelines/manifests/{pipeline_name}.yaml
dltaf manifest run --manifest dlt_pipelines/manifests/{pipeline_name}.yaml --plan
```

## Backfill example

```bash
dltaf manifest run --manifest dlt_pipelines/manifests/{pipeline_name}.yaml \
  --set source.some_param=123 \
  --time-from 2026-01-01 --time-to 2026-02-01 \
  --plan
```

## Payload contract

```yaml
source:
  payload_contract:
    mode: warn
    schema_path: dlt_pipelines/{package_name}/contracts/payload.schema.json
```
'''


def render_runner_plugin(*, source_kind: str, source_module: str, source_factory: str) -> str:
    cls_name = f"{source_kind.title().replace('_', '')}Runner"
    return f'''from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Any, Mapping

from dltaf.app.runtime import RunContext
from dltaf.extensions.runners.registry import RunnerRegistry

logger = logging.getLogger(__name__)


@dataclass
class {cls_name}:
    kind: str = "{source_kind}"

    def validate(self, manifest: Mapping[str, Any]) -> None:
        pipeline = manifest.get("pipeline") or {{}}
        if not str(pipeline.get("name") or "").strip():
            raise ValueError("pipeline.name is required")
        source = manifest.get("source") or {{}}
        if str(source.get("kind") or "").strip() != self.kind:
            raise ValueError("source.kind must be '{source_kind}'")

    def run(self, manifest: Mapping[str, Any], ctx: RunContext) -> Any:
        import dlt
        pipeline_cfg = manifest.get("pipeline") or {{}}
        run_cfg = manifest.get("run") or {{}}
        pipeline = dlt.pipeline(
            pipeline_name=str(pipeline_cfg.get("name")),
            destination=str(pipeline_cfg.get("destination") or "clickhouse"),
            dataset_name=pipeline_cfg.get("dataset"),
            progress=pipeline_cfg.get("progress", "log"),
            dev_mode=bool(pipeline_cfg.get("dev_mode", False)),
        )
        from {source_module} import {source_factory}
        source_cfg = manifest.get("source") or {{}}
        source = {source_factory}(source_cfg=source_cfg, services=ctx.services)
        write_disposition = str(run_cfg.get("write_disposition") or "merge")
        load_info = pipeline.run(source, write_disposition=write_disposition)
        logger.info("dlt load finished: %s", load_info)
        return load_info


def register_runners(registry: RunnerRegistry) -> None:
    registry.register({cls_name}())
'''


def render_source_api_kafka_json(*, source_kind: str) -> str:
    return f'''from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Any, Iterator, Mapping, Optional, Sequence

import dlt

from dltaf.app.services import ApplicationServices, ApplicationServicesFactory
from dlt_utils.patterns import KafkaAsyncJobSettings, KafkaJsonAsyncJob, KafkaJsonAsyncJobRunner

logger = logging.getLogger(__name__)

DEFAULT_BASE_URL = "https://example.com"
DEFAULT_EXPORT_PATH = "/api/v1/export"
DEFAULT_KAFKA_TOPIC = "my.topic"
DEFAULT_KAFKA_WAIT_TIMEOUT_SECONDS = 600
DEFAULT_KAFKA_POLL_INTERVAL_SECONDS = 5


@dataclass(frozen=True)
class ExportRequest:
    year: int
    period: str


def _extract_correlation_id(resp_json: Mapping[str, Any]) -> Optional[str]:
    payload = resp_json.get("payload") if isinstance(resp_json, Mapping) else None
    if isinstance(payload, Mapping):
        mid = payload.get("messageId")
        if mid is not None and str(mid).strip():
            return str(mid).strip()
    return None


@dlt.source(name="{source_kind}")
def {source_kind}_source(*, source_cfg: Mapping[str, Any], services: Optional[ApplicationServices] = None) -> Any:
    svc = services or ApplicationServicesFactory.default()
    base_url = str(source_cfg.get("base_url") or DEFAULT_BASE_URL).strip() or DEFAULT_BASE_URL
    export_path = str(source_cfg.get("export_path") or DEFAULT_EXPORT_PATH).strip() or DEFAULT_EXPORT_PATH
    kafka_cfg = source_cfg.get("kafka") if isinstance(source_cfg, Mapping) else None
    kafka_topic = DEFAULT_KAFKA_TOPIC
    kafka_wait_timeout_seconds = DEFAULT_KAFKA_WAIT_TIMEOUT_SECONDS
    kafka_poll_interval_seconds = DEFAULT_KAFKA_POLL_INTERVAL_SECONDS
    if isinstance(kafka_cfg, Mapping):
        kafka_topic = str(kafka_cfg.get("topic") or kafka_topic).strip() or kafka_topic
        if kafka_cfg.get("wait_timeout_seconds") is not None:
            kafka_wait_timeout_seconds = int(kafka_cfg.get("wait_timeout_seconds"))
        if kafka_cfg.get("poll_interval_seconds") is not None:
            kafka_poll_interval_seconds = int(kafka_cfg.get("poll_interval_seconds"))
    kafka_bootstrap_servers: Sequence[str] = ["localhost:9092"]
    kafka_consumer_kwargs: Mapping[str, Any] = {{}}
    http = svc.http.create(base_url=base_url, logger=logger)
    waiter = svc.kafka.create(bootstrap_servers=list(kafka_bootstrap_servers), consumer_kwargs=dict(kafka_consumer_kwargs), logger=logger)
    async_runner = KafkaJsonAsyncJobRunner(waiter=waiter, logger=logger)

    @dlt.resource(name="{source_kind}_payload")
    def export_payload() -> Iterator[Mapping[str, Any]]:
        req = ExportRequest(year=2024, period="QUARTER_1")
        def trigger() -> Optional[str]:
            resp_json = http.post_json(export_path, body={{"year": req.year, "period": req.period}})
            return _extract_correlation_id(resp_json)
        job = KafkaJsonAsyncJob(name="{source_kind}_export", correlation_id_name="messageId", settings=KafkaAsyncJobSettings(topic=kafka_topic, timeout_seconds=int(kafka_wait_timeout_seconds), poll_interval_seconds=float(kafka_poll_interval_seconds), start_from="end"), trigger=trigger)
        res = async_runner.run_job_safe(job)
        if res is None:
            return
        payload = res.payload
        if isinstance(payload, Mapping):
            yield payload
        else:
            yield {{"payload": payload}}

    return export_payload
'''


def render_source_http_pull(*, source_kind: str) -> str:
    return f'''from __future__ import annotations

import logging
from typing import Any, Iterator, Mapping, Optional

import dlt

from dltaf.app.services import ApplicationServices, ApplicationServicesFactory

logger = logging.getLogger(__name__)

DEFAULT_BASE_URL = "https://example.com"
DEFAULT_ENDPOINT = "/api/v1/items"


@dlt.source(name="{source_kind}")
def {source_kind}_source(*, source_cfg: Mapping[str, Any], services: Optional[ApplicationServices] = None) -> Any:
    svc = services or ApplicationServicesFactory.default()
    base_url = str(source_cfg.get("base_url") or DEFAULT_BASE_URL).strip() or DEFAULT_BASE_URL
    endpoint = str(source_cfg.get("endpoint") or DEFAULT_ENDPOINT).strip() or DEFAULT_ENDPOINT
    http = svc.http.create(base_url=base_url, logger=logger)

    @dlt.resource(name="{source_kind}_items")
    def items() -> Iterator[Mapping[str, Any]]:
        data = http.get_json(endpoint)
        if isinstance(data, list):
            for item in data:
                yield item if isinstance(item, Mapping) else {{"value": item}}
            return
        if isinstance(data, Mapping):
            yield data
            return
        yield {{"value": data}}

    return items
'''


def render_source_noop(*, source_kind: str) -> str:
    return f'''from __future__ import annotations

import dlt


@dlt.source(name="{source_kind}")
def {source_kind}_source(*, source_cfg, services=None):
    @dlt.resource(name="{source_kind}_noop")
    def noop():
        yield {{"status": "noop"}}
    return noop
'''


def render_infra_checks_stub(*, source_kind: str, default_base_url: str = "https://example.com") -> str:
    return f'''from __future__ import annotations

import time
from dataclasses import dataclass
from typing import Any, Dict, List, Mapping

import requests

DEFAULT_BASE_URL = "{default_base_url}"


def _http_probe(url: str, *, timeout_seconds: float) -> Dict[str, Any]:
    started = time.time()
    try:
        response = requests.get(url, timeout=float(timeout_seconds), allow_redirects=False)
        elapsed_ms = int((time.time() - started) * 1000)
        status = int(getattr(response, "status_code", 0) or 0)
        ok = status > 0 and status < 500
        return {{"attempted": True, "ok": ok, "status_code": status, "latency_ms": elapsed_ms, "error": None}}
    except Exception as exc:
        elapsed_ms = int((time.time() - started) * 1000)
        return {{"attempted": True, "ok": False, "status_code": None, "latency_ms": elapsed_ms, "error": f"{{exc.__class__.__name__}}: {{exc}}"}}


@dataclass
class ApiConnectivityCheck:
    name: str = "{source_kind}_api"
    description: str = "API reachability (HTTP probe)."

    def applies(self, manifest: Mapping[str, Any], ctx: Any) -> bool:
        source = manifest.get("source") or {{}}
        return str(source.get("kind") or "").strip() == "{source_kind}"

    def run(self, manifest: Mapping[str, Any], ctx: Any, *, timeout_seconds: float) -> Mapping[str, Any]:
        source = manifest.get("source") or {{}}
        base_url = str(source.get("base_url") or DEFAULT_BASE_URL).strip() or DEFAULT_BASE_URL
        return {{"url": base_url, "http": _http_probe(base_url, timeout_seconds=float(timeout_seconds))}}

    def evaluate(self, result: Any) -> Mapping[str, List[str]]:
        errors: List[str] = []
        warnings: List[str] = []
        if not isinstance(result, Mapping):
            errors.append("Invalid result from ApiConnectivityCheck")
            return {{"errors": errors, "warnings": warnings}}
        http = result.get("http")
        if isinstance(http, Mapping):
            if not bool(http.get("ok")):
                errors.append("API probe failed")
            else:
                status = http.get("status_code")
                if isinstance(status, int) and 400 <= status < 500:
                    warnings.append(f"API responded with HTTP {{status}} (reachable, but check auth/routes)")
        return {{"errors": errors, "warnings": warnings}}


def register_infra_checks(registry: Any) -> None:
    registry.register(ApiConnectivityCheck())
'''


def render_init_py() -> str:
    return '"""Integration package."""\n'


def render_contract_schema(*, source_kind: str) -> str:
    return (
        "{\n"
        '  "$schema": "https://json-schema.org/draft/2020-12/schema",\n'
        f'  "title": "Payload contract for {source_kind}",\n'
        '  "type": "object",\n'
        '  "additionalProperties": true\n'
        "}\n"
    )


def render_contract_sample() -> str:
    return "{\n  \"_example\": true\n}\n"


__all__ = [
    "render_contract_sample",
    "render_contract_schema",
    "render_infra_checks_stub",
    "render_init_py",
    "render_integration_readme",
    "render_manifest_yaml",
    "render_runner_plugin",
    "render_source_api_kafka_json",
    "render_source_http_pull",
    "render_source_noop",
]
