from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Mapping, Optional, Sequence

from dlt_utils.contracts import (
    PayloadContractConfig,
    build_payload_contract,
    check_contract_samples,
    result_to_dict,
)
from dlt_utils.core.plan_output import infer_plan_format, write_plan_output
from dltaf.services.execution.redaction_service import RedactionOptions, redact_obj, redact_text
from dltaf.services.manifests.loader import load_manifest


@dataclass(frozen=True)
class ContractTestReport:
    payload: Dict[str, Any]

    @property
    def failed(self) -> int:
        summary = self.payload.get("summary") if isinstance(self.payload, Mapping) else None
        if isinstance(summary, Mapping):
            return int(summary.get("failed") or 0)
        return 0


def add_contract_test_arguments(parser) -> None:
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--manifest", help="Path to a single manifest YAML")
    group.add_argument("--manifests-dir", help="Directory with manifest YAMLs")
    parser.add_argument(
        "--output",
        default="-",
        help="Output path (file) or '-' for stdout (default '-')",
    )
    parser.add_argument(
        "--format",
        default=None,
        help="Output format: json|yaml (default inferred from output path)",
    )
    parser.add_argument(
        "--max-errors",
        type=int,
        default=None,
        help="Override max errors per sample (default from contract config)",
    )
    parser.add_argument(
        "--allow-no-samples",
        action="store_true",
        help="Treat missing samples as OK (default: missing samples = failure)",
    )
    parser.add_argument(
        "--include-skipped",
        action="store_true",
        help="Include skipped manifests in the output report",
    )


class ContractTestService:
    def __init__(self, *, logger) -> None:
        self.logger = logger

    def collect_manifest_paths(self, *, manifest: Optional[str], manifests_dir: Optional[str]) -> List[Path]:
        if manifest:
            return [Path(manifest).expanduser().resolve()]
        if manifests_dir:
            base = Path(manifests_dir).expanduser().resolve()
            if not base.exists() or not base.is_dir():
                raise ValueError(f"manifests-dir not found or not a directory: {base}")
            return [p.resolve() for p in sorted([*base.glob("*.yaml"), *base.glob("*.yml")])]
        raise ValueError("Either --manifest or --manifests-dir is required")

    def _safe_get(self, data: Mapping[str, Any], *keys: str, default: Any = None) -> Any:
        cur: Any = data
        for key in keys:
            if not isinstance(cur, Mapping):
                return default
            cur = cur.get(key)
        return cur if cur is not None else default

    def run_contract_tests(
        self,
        *,
        manifest_paths: Sequence[Path],
        max_errors: Optional[int],
        allow_no_samples: bool,
        include_skipped: bool,
    ) -> ContractTestReport:
        results: List[Dict[str, Any]] = []
        total = skipped = failed = passed = 0
        for manifest_path in manifest_paths:
            total += 1
            data = load_manifest(manifest_path)
            resolved_path = Path(str(data.get("__manifest_path__") or manifest_path)).resolve()
            pipeline_name = str(self._safe_get(data, "pipeline", "name", default="")).strip() or None
            kind = str(self._safe_get(data, "source", "kind", default="")).strip() or None
            payload_contract_raw = self._safe_get(data, "source", "payload_contract")
            if not payload_contract_raw:
                skipped += 1
                if include_skipped:
                    results.append(
                        {
                            "manifest": str(resolved_path),
                            "pipeline": pipeline_name,
                            "kind": kind,
                            "status": "skipped",
                            "reason": "no_payload_contract",
                        }
                    )
                continue
            try:
                cfg = PayloadContractConfig.model_validate(payload_contract_raw)
            except Exception as exc:
                failed += 1
                results.append(
                    {
                        "manifest": str(resolved_path),
                        "pipeline": pipeline_name,
                        "kind": kind,
                        "status": "failed",
                        "reason": str(redact_text(f"invalid_payload_contract_config: {exc}")),
                    }
                )
                continue
            if str(cfg.mode) == "off":
                skipped += 1
                if include_skipped:
                    results.append(
                        {
                            "manifest": str(resolved_path),
                            "pipeline": pipeline_name,
                            "kind": kind,
                            "status": "skipped",
                            "reason": "mode_off",
                        }
                    )
                continue
            contract_name = f"{pipeline_name or kind or 'pipeline'}.payload_contract"
            try:
                contract = build_payload_contract(
                    cfg,
                    name=contract_name,
                    manifest_path=resolved_path,
                )
            except Exception as exc:
                failed += 1
                results.append(
                    {
                        "manifest": str(resolved_path),
                        "pipeline": pipeline_name,
                        "kind": kind,
                        "status": "failed",
                        "reason": str(redact_text(f"contract_build_failed: {exc}")),
                    }
                )
                continue
            if contract is None:
                skipped += 1
                if include_skipped:
                    results.append(
                        {
                            "manifest": str(resolved_path),
                            "pipeline": pipeline_name,
                            "kind": kind,
                            "status": "skipped",
                            "reason": "contract_disabled",
                        }
                    )
                continue
            check = check_contract_samples(
                contract,
                cfg=cfg,
                manifest_path=resolved_path,
                max_errors=max_errors,
                allow_no_samples=allow_no_samples,
            )
            item = {
                "manifest": str(resolved_path),
                "pipeline": pipeline_name,
                "kind": kind,
                "contract": result_to_dict(check),
            }
            if check.ok:
                passed += 1
                item["status"] = "passed"
            else:
                failed += 1
                item["status"] = "failed"
            results.append(item)
        return ContractTestReport(
            payload={
                "summary": {
                    "total_manifests": total,
                    "passed": passed,
                    "failed": failed,
                    "skipped": skipped,
                },
                "results": results,
            }
        )

    def write_report(self, report: ContractTestReport, *, output: str, fmt: Optional[str]) -> None:
        resolved_format = infer_plan_format(output_path=str(output), explicit_format=fmt)
        safe_report = redact_obj(
            report.payload,
            options=RedactionOptions(max_depth=12, max_list=500, max_str=4096, redact_keys=False),
        )
        write_plan_output(safe_report, output_path=str(output), fmt=resolved_format)

    def run(self, args) -> int:
        manifest_paths = self.collect_manifest_paths(
            manifest=getattr(args, "manifest", None),
            manifests_dir=getattr(args, "manifests_dir", None),
        )
        report = self.run_contract_tests(
            manifest_paths=manifest_paths,
            max_errors=getattr(args, "max_errors", None),
            allow_no_samples=bool(getattr(args, "allow_no_samples", False)),
            include_skipped=bool(getattr(args, "include_skipped", False)),
        )
        self.write_report(
            report,
            output=str(getattr(args, "output", "-")),
            fmt=getattr(args, "format", None),
        )
        return 0 if report.failed == 0 else 3


__all__ = [
    "ContractTestReport",
    "ContractTestService",
    "add_contract_test_arguments",
]
