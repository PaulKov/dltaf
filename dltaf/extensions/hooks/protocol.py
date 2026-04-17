from __future__ import annotations

from typing import Any, Mapping, Protocol

from dltaf.app.runtime import RunContext


class Hook(Protocol):
    name: str

    def pre_run(self, manifest: Mapping[str, Any], ctx: RunContext) -> None: ...

    def post_run(self, manifest: Mapping[str, Any], ctx: RunContext, result: Any) -> None: ...

    def on_error(self, manifest: Mapping[str, Any], ctx: RunContext, exc: Exception) -> None: ...


__all__ = ["Hook"]
