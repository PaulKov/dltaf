from __future__ import annotations

import os
import re
from dataclasses import dataclass
from typing import Any, Mapping, Optional

from dltaf.app.runtime import RunContext


def _boolish(v: Any) -> bool:
    if v is None:
        return False
    if isinstance(v, bool):
        return v
    if isinstance(v, int):
        return v != 0
    s = str(v).strip().lower()
    return s in {"1", "true", "yes", "y", "on"}


@dataclass
class ReplacePolicyHook:
    """Optional safety policy for write_disposition=replace.

    This hook is **disabled by default**. Enable it per manifest:

    run:
      hooks:
        enable: [replace_policy]
      replace_policy:
        enabled: true
        enforce: prod_only   # or: all
        prod_dataset_regex: ".*prod.*"  # optional
        allow_datasets: ["raw_dev"]
        allow_pipelines: ["dlt__pipeline__dev"]
        bypass_env_var: "DLT_ALLOW_REPLACE"  # optional
        mode: deny  # or: warn

    Design:
    - If not enabled, it is a no-op.
    - It never logs secret values.
    - It can be bypassed via env var (emergency / break-glass).
    """

    name: str = "replace_policy"

    def pre_run(self, manifest: Mapping[str, Any], ctx: RunContext) -> None:
        write_disposition = str(ctx.options.write_disposition or "").strip().lower()
        if write_disposition != "replace":
            return

        run_cfg = manifest.get("run") or {}
        policy_cfg = {}
        if isinstance(run_cfg, Mapping):
            policy_cfg = run_cfg.get("replace_policy") or {}
        if not isinstance(policy_cfg, Mapping):
            return

        if not _boolish(policy_cfg.get("enabled")):
            return

        bypass_env_var = str(policy_cfg.get("bypass_env_var") or "DLT_ALLOW_REPLACE").strip()
        if bypass_env_var and _boolish(os.getenv(bypass_env_var)):
            ctx.logger.warning(
                "ReplacePolicyHook bypassed via env var %s=1 for pipeline=%s dataset=%s",
                bypass_env_var,
                ctx.pipeline_name,
                ctx.dataset,
            )
            return

        mode = str(policy_cfg.get("mode") or "deny").strip().lower()
        enforce = str(policy_cfg.get("enforce") or "prod_only").strip().lower()

        allow_datasets = [str(x).strip() for x in (policy_cfg.get("allow_datasets") or []) if str(x).strip()]
        allow_pipelines = [str(x).strip() for x in (policy_cfg.get("allow_pipelines") or []) if str(x).strip()]

        if ctx.dataset in allow_datasets or ctx.pipeline_name in allow_pipelines:
            return

        is_prod = True
        if enforce == "prod_only":
            is_prod = self._is_prod_dataset(ctx.dataset, policy_cfg.get("prod_dataset_regex"))

        if not is_prod:
            return

        msg = (
            "ReplacePolicyHook blocked write_disposition=replace for "
            f"pipeline={ctx.pipeline_name} dataset={ctx.dataset}. "
            "Configure run.replace_policy.allow_datasets/allow_pipelines or set bypass env var."
        )

        if mode == "warn":
            ctx.logger.warning(msg)
            return

        raise RuntimeError(msg)

    @staticmethod
    def _is_prod_dataset(dataset: str, prod_dataset_regex: Optional[Any]) -> bool:
        ds = str(dataset or "").strip()
        if not ds:
            return False

        if prod_dataset_regex:
            try:
                return re.search(str(prod_dataset_regex), ds, flags=re.IGNORECASE) is not None
            except Exception:
                # fallback to substring
                return "prod" in ds.lower()

        return "prod" in ds.lower()

    def post_run(self, manifest: Mapping[str, Any], ctx: RunContext, result: Any) -> None:
        return

    def on_error(self, manifest: Mapping[str, Any], ctx: RunContext, exc: Exception) -> None:
        return
