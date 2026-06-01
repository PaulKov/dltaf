from __future__ import annotations

import logging
from pathlib import Path
from typing import Any, Mapping, Optional
from uuid import uuid4

from dltaf.app.services import ApplicationServicesFactory
from dltaf.extensions.hooks import HookPipeline, build_hooks
from dltaf.services.execution.context_builder import RunContextBuilder
from dltaf.services.execution.environment import ExecutionEnvironmentBuilder, temporary_environ
from dltaf.services.execution.planner import execute_plan_or_dry_run
from dltaf.services.execution.request import ManifestExecutionRequest
from dltaf.services.manifests.loader import load_manifest
from dltaf.services.manifests.overrides import OverrideSpec, apply_overrides
from dltaf.services.manifests.validator import get_runner_for_manifest, validate_manifest

logger = logging.getLogger(__name__)


class ManifestExecutor:
    """Execute manifest-driven runs using the new `dltaf.services` layout."""

    def __init__(self) -> None:
        self._env_builder = ExecutionEnvironmentBuilder()
        self._context_builder = RunContextBuilder()

    def run_manifest(
        self,
        manifest_path: str | Path,
        *,
        write_disposition: Optional[str] = None,
        overrides: Optional[Mapping[str, Any]] = None,
        extra_env: Optional[Mapping[str, str]] = None,
        validate_only: bool = False,
        explain_config: bool = False,
        plan: bool = False,
        dry_run: bool = False,
        dry_run_online: bool = False,
        dry_run_strict: bool = False,
        online_timeout_seconds: float = 5.0,
        plan_output: Optional[str] = None,
        plan_format: Optional[str] = None,
        configure_logging: bool = True,
        log_level: str = "INFO",
    ) -> Any:
        request = ManifestExecutionRequest(
            manifest_path=manifest_path,
            write_disposition=write_disposition,
            overrides=dict(overrides or {}),
            extra_env=dict(extra_env or {}),
            validate_only=validate_only,
            explain_config=explain_config,
            plan=plan,
            dry_run=dry_run,
            dry_run_online=dry_run_online,
            dry_run_strict=dry_run_strict,
            online_timeout_seconds=float(online_timeout_seconds),
            plan_output=plan_output,
            plan_format=plan_format,
            configure_logging=configure_logging,
            log_level=log_level,
        )
        request.validate()

        if request.configure_logging:
            logging.basicConfig(
                level=getattr(logging, request.log_level.upper(), logging.INFO),
                format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
            )

        manifest = load_manifest(request.manifest_path)
        if request.overrides:
            specs = [OverrideSpec(path=str(k), value=v) for k, v in request.overrides.items()]
            apply_overrides(manifest, specs)
        if request.write_disposition:
            apply_overrides(
                manifest,
                [OverrideSpec(path="run.write_disposition", value=str(request.write_disposition))],
            )

        validate_manifest(manifest)
        if request.validate_only:
            logger.info("Manifest validated: %s", request.manifest_path)
            return None

        run_id = str(uuid4())
        services = ApplicationServicesFactory.default(request_id=run_id)
        env = self._env_builder.build(
            manifest_connections=manifest.get("connections") or {},
            services=services,
            extra_env=request.extra_env,
        )
        ctx = self._context_builder.build(
            manifest=manifest,
            request=request,
            env=env,
            services=services,
            run_id=run_id,
        )
        runner = get_runner_for_manifest(manifest)
        hooks = build_hooks(manifest, ctx)
        pipeline = HookPipeline(hooks)

        def _execute() -> Any:
            if ctx.options.plan or ctx.options.dry_run:
                return execute_plan_or_dry_run(
                    manifest=manifest,
                    ctx=ctx,
                    hooks=hooks,
                    plan_output=request.plan_output,
                    plan_format=request.plan_format,
                    online_timeout_seconds=float(request.online_timeout_seconds),
                )
            return runner.run(manifest, ctx)

        with temporary_environ(dict(env.values)):
            return pipeline.run(manifest=manifest, ctx=ctx, fn=_execute)


_default_executor = ManifestExecutor()


def run_manifest(manifest_path: str | Path, **kwargs: Any) -> Any:
    return _default_executor.run_manifest(manifest_path, **kwargs)


__all__ = ["ManifestExecutor", "run_manifest", "ManifestExecutionRequest"]
