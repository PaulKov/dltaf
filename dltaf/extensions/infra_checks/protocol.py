from __future__ import annotations

from typing import Any, List, Mapping, Protocol


class InfraCheck(Protocol):
    name: str
    description: str

    def applies(self, manifest: Mapping[str, Any], ctx: Any) -> bool: ...

    def run(self, manifest: Mapping[str, Any], ctx: Any, *, timeout_seconds: float) -> Mapping[str, Any]: ...

    def evaluate(self, result: Any) -> Mapping[str, List[str]]: ...


__all__ = ["InfraCheck"]
