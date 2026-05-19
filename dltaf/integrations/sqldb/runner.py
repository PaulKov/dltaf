from __future__ import annotations

import logging
from typing import Any, Mapping, Optional

from dltaf.app.runtime import RunContext
from dltaf.extensions.runners.protocol import SourceRunner

from .config import SqlDbConfigParser
from .workflow import SqlDbWorkflow

logger = logging.getLogger(__name__)


class SqlDbRunner(SourceRunner):
    kind = "sqldb"

    def __init__(
        self,
        *,
        config_parser: Optional[SqlDbConfigParser] = None,
        workflow_factory: Optional[type[SqlDbWorkflow]] = None,
    ) -> None:
        self._config_parser = config_parser or SqlDbConfigParser()
        self._workflow_factory = workflow_factory or SqlDbWorkflow

    def validate(self, manifest: Mapping[str, Any]) -> None:
        self._config_parser.validate(manifest)

    def run(self, manifest: Mapping[str, Any], ctx: RunContext) -> Any:
        cfg = self._config_parser.parse(manifest, ctx)
        workflow = self._workflow_factory(logger_=ctx.logger)
        return workflow.execute(cfg, ctx)


class OraclePresetRunner(SqlDbRunner):
    kind = "oracle"


__all__ = ["OraclePresetRunner", "SqlDbRunner"]
