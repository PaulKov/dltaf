from __future__ import annotations

from dltaf.integrations.sqldb.config import ResolvedSqlDbConfig


class GenericSqlDialect:
    name = "generic"

    def validate(self, config: ResolvedSqlDbConfig) -> None:
        if config.mode == "query":
            raise ValueError(
                "Generic sqldb dialect does not support mode=query in Stage 46 foundation. "
                "Use dialect=oracle for query mode, or switch to mode=catalog."
            )
