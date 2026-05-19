from __future__ import annotations

from dataclasses import dataclass
from typing import Mapping

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
