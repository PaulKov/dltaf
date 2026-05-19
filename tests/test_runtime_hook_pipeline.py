from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Mapping

import pytest

from dltaf.app.runtime import RunContext, RunOptions
from dltaf.extensions.hooks.pipeline import HookPipeline
from dlt_utils.core.run_result import RunResult


def _ctx() -> RunContext:
    return RunContext(
        run_id="hook-pipeline-test",
        started_at=datetime.now(timezone.utc),
        manifest_path=Path("manifest.yaml"),
        pipeline_name="dlt__sample__to__clickhouse__raw",
        source_kind="sqldb",
        destination="clickhouse",
        dataset="raw",
        logger=__import__("logging").getLogger("hook-pipeline-test"),
        options=RunOptions(),
    )


@dataclass
class CaptureHook:
    name: str = "capture"
    seen_statuses: list[str] = field(default_factory=list)

    def pre_run(self, manifest: Mapping[str, Any], ctx: RunContext) -> None:
        return

    def post_run(self, manifest: Mapping[str, Any], ctx: RunContext, result: Any) -> None:
        self.seen_statuses.append(getattr(result, "status", "unknown"))

    def on_error(self, manifest: Mapping[str, Any], ctx: RunContext, exc: Exception) -> None:
        return


def test_hook_pipeline_propagates_failed_run_result_after_post_run() -> None:
    hook = CaptureHook()
    pipeline = HookPipeline([hook])

    with pytest.raises(RuntimeError, match="policy failed"):
        pipeline.run(
            manifest={"pipeline": {"name": "dlt__sample__to__clickhouse__raw"}},
            ctx=_ctx(),
            fn=lambda: RunResult(status="failed", message="policy failed"),
        )

    assert hook.seen_statuses == ["failed"]


def test_hook_pipeline_returns_payload_for_partial_success() -> None:
    hook = CaptureHook()
    pipeline = HookPipeline([hook])

    result = pipeline.run(
        manifest={"pipeline": {"name": "dlt__sample__to__clickhouse__raw"}},
        ctx=_ctx(),
        fn=lambda: RunResult(status="partial_success", payload={"ok": True}, message="loaded 1/2"),
    )

    assert result == {"ok": True}
    assert hook.seen_statuses == ["partial_success"]
