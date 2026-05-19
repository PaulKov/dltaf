from __future__ import annotations

import os
from dataclasses import dataclass, field
from typing import Any, Mapping, Optional, Sequence

from dltaf.app.runtime import RunContext
from dltaf.services.manifests.schema import SourceMongoDB
from dlt_utils.runners.source_config import parse_source_config


@dataclass(frozen=True)
class ResolvedMongoDBConfig:
    connection_url: str
    database: str
    collection_names: Sequence[str] = field(default_factory=tuple)
    max_table_nesting: Optional[int] = None


class MongoDBConfigParser:
    def validate(self, manifest: Mapping[str, Any]) -> None:
        parse_source_config(manifest, SourceMongoDB)

    def parse(self, manifest: Mapping[str, Any], ctx: RunContext) -> ResolvedMongoDBConfig:
        _ = manifest, ctx
        cfg = parse_source_config(manifest, SourceMongoDB)
        connection_url = str(os.getenv("SOURCES__MONGODB__CONNECTION_URL", "")).strip()
        if not connection_url:
            raise ValueError("Mongo credentials are not configured in ENV")
        max_table_nesting = None
        if cfg.max_table_nesting is not None:
            max_table_nesting = int(cfg.max_table_nesting)
        return ResolvedMongoDBConfig(
            connection_url=connection_url,
            database=str(cfg.database),
            collection_names=tuple(cfg.collection_names or ()),
            max_table_nesting=max_table_nesting,
        )


__all__ = ["ResolvedMongoDBConfig", "MongoDBConfigParser"]
