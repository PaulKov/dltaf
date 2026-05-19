from __future__ import annotations

from typing import Any, Mapping, Protocol

from dltaf.app.runtime import RunContext


class SourceRunner(Protocol):
    kind: str

    def validate(self, manifest: Mapping[str, Any]) -> None:
        ...

    def run(self, manifest: Mapping[str, Any], ctx: RunContext) -> Any:
        ...


__all__ = ["SourceRunner"]
