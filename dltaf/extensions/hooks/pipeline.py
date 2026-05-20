from __future__ import annotations

from dataclasses import dataclass
from typing import Any, List, Mapping

from dltaf.app.runtime import RunContext
from dltaf.services.execution.redaction import safe_exception_message
from dlt_utils.core.run_result import RunResult, build_run_result

from .protocol import Hook


@dataclass
class HookPipeline:
    hooks: List[Hook]

    def run(self, *, manifest: Mapping[str, Any], ctx: RunContext, fn) -> Any:
        executed: List[Hook] = []
        try:
            for hook in self.hooks:
                self._safe_call(hook, "pre_run", manifest, ctx)
                executed.append(hook)
            payload = fn()
        except Exception as exc:
            for hook in reversed(executed):
                self._safe_call(hook, "on_error", manifest, ctx, exc)
            raise
        else:
            if isinstance(payload, RunResult):
                rr = payload
            else:
                if ctx.options.plan:
                    status = "planned"
                elif getattr(ctx.options, "dry_run_online", False):
                    status = "dry_run_online"
                elif ctx.options.dry_run:
                    status = "dry_run"
                else:
                    status = "success"
                rr = build_run_result(ctx=ctx, manifest=manifest, status=status, payload=payload)
            for hook in reversed(executed):
                self._safe_call(hook, "post_run", manifest, ctx, rr)
            if rr.status == "failed":
                raise RuntimeError(rr.message or "Run failed")
            return rr.payload if isinstance(payload, RunResult) else payload

    @staticmethod
    def _safe_call(hook: Hook, method: str, *args: Any) -> None:
        try:
            getattr(hook, method)(*args)
        except Exception as hook_exc:
            ctx = None
            for arg in args:
                if isinstance(arg, RunContext):
                    ctx = arg
                    break
            if ctx is not None:
                safe_msg = safe_exception_message(hook_exc, limit=1200)
                ctx.logger.warning(
                    "Hook '%s' failed in %s: %s",
                    getattr(hook, "name", hook.__class__.__name__),
                    method,
                    safe_msg,
                    exc_info=True,
                )


__all__ = ["HookPipeline"]
