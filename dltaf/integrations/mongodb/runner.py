from __future__ import annotations

import logging
from typing import Any, Mapping, Optional

from dltaf.app.runtime import RunContext
from dltaf.extensions.runners.protocol import SourceRunner
from dlt_utils.runners.common import run_with_replace_protection

from .config import MongoDBConfigParser
from .workflow import MongoDBWorkflow

logger = logging.getLogger(__name__)


class MongoDBRunner(SourceRunner):
    kind = "mongodb"

    def __init__(
        self,
        *,
        config_parser: Optional[MongoDBConfigParser] = None,
        workflow_factory: Optional[type[MongoDBWorkflow]] = None,
    ) -> None:
        self._config_parser = config_parser or MongoDBConfigParser()
        self._workflow_factory = workflow_factory or MongoDBWorkflow

    def validate(self, manifest: Mapping[str, Any]) -> None:
        self._config_parser.validate(manifest)

    def run(self, manifest: Mapping[str, Any], ctx: RunContext) -> Any:
        cfg = self._config_parser.parse(manifest, ctx)
        pipeline_cfg = manifest.get("pipeline") or {}
        run_cfg = manifest.get("run") or {}

        import dlt

        pipeline = dlt.pipeline(
            pipeline_name=pipeline_cfg["name"],
            destination=pipeline_cfg.get("destination", "clickhouse"),
            dataset_name=pipeline_cfg.get("dataset"),
            progress=pipeline_cfg.get("progress", "log"),
            dev_mode=bool(pipeline_cfg.get("dev_mode", False)),
        )

        write_disposition = str(run_cfg.get("write_disposition") or "replace")
        workflow = self._workflow_factory(logger_=ctx.logger)
        source_factory = workflow.build_source_factory(cfg, write_disposition=write_disposition)

        load_info = run_with_replace_protection(
            pipeline=pipeline,
            source_factory=source_factory,
            manifest=manifest,
            write_disposition=write_disposition,
        )
        logger.info("dlt load finished: %s", load_info)
        return load_info


__all__ = ["MongoDBRunner"]
