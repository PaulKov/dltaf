from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Mapping

from dlt_utils.hooks.audit_run import AuditRunHook


@dataclass
class AuditService:
    """Best-effort adapter around the production audit hook.

    The runtime still executes auditing through hooks. This service exists to give
    the new `dltaf.services.execution` layer an explicit audit entry point and a
    stable seam for future refactors.
    """

    hook: AuditRunHook = field(default_factory=AuditRunHook)

    def audit_success(self, *, manifest: Mapping[str, Any], ctx: Any, result: Any) -> None:
        self.hook.post_run(manifest, ctx, result)

    def audit_error(self, *, manifest: Mapping[str, Any], ctx: Any, exc: Exception) -> None:
        self.hook.on_error(manifest, ctx, exc)


__all__ = ["AuditService"]
