from __future__ import annotations

import contextlib
import os
from dataclasses import dataclass
from typing import Iterator, Mapping, Optional

from dltaf.app.services import ApplicationServices


@dataclass(frozen=True)
class ExecutionEnvironment:
    values: Mapping[str, str]
    sources: Mapping[str, str]


class ExecutionEnvironmentBuilder:
    def build(
        self,
        *,
        manifest_connections: Mapping[str, object],
        services: ApplicationServices,
        extra_env: Mapping[str, str],
    ) -> ExecutionEnvironment:
        resolved_env = services.secrets.resolve_connections(manifest_connections)
        env_values = resolved_env.to_env_dict()
        env_sources = resolved_env.sources()
        for key, value in extra_env.items():
            env_values[str(key)] = str(value)
            env_sources[str(key)] = "extra_env"
        return ExecutionEnvironment(values=env_values, sources=env_sources)


@contextlib.contextmanager
def temporary_environ(env: Mapping[str, str]) -> Iterator[None]:
    """Temporarily overlay ``os.environ`` for in-process manifest execution."""

    previous: dict[str, Optional[str]] = {}
    for key, value in dict(env or {}).items():
        previous[str(key)] = os.environ.get(str(key))
        os.environ[str(key)] = str(value)
    try:
        yield
    finally:
        for key, value in previous.items():
            if value is None:
                os.environ.pop(key, None)
            else:
                os.environ[key] = value
