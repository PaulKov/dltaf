from __future__ import annotations

import logging
from typing import Any, Callable

from .config import ResolvedMongoDBConfig


class MongoDBWorkflow:
    def __init__(self, *, logger_: logging.Logger | None = None) -> None:
        self._logger = logger_ or logging.getLogger(__name__)

    def build_source_factory(self, config: ResolvedMongoDBConfig, *, write_disposition: str) -> Callable[[], Any]:
        from dlt_pipelines.mongodb_runtime.mongodb import mongodb

        def mongodb_source_factory():
            source = mongodb(
                connection_url=config.connection_url,
                database=config.database,
                collection_names=list(config.collection_names or ()),
                write_disposition=str(write_disposition),
            )
            if config.max_table_nesting is not None:
                source.max_table_nesting = int(config.max_table_nesting)
            return source

        return mongodb_source_factory


__all__ = ["MongoDBWorkflow"]
