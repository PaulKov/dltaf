from __future__ import annotations

from typing import Mapping, Protocol


class SecretsProvider(Protocol):
    def resolve_connections(self, connections: Mapping[str, object]): ...
