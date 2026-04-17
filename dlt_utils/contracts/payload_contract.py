"""Payload contracts (schema drift protection).

Stage 27 goal
------------
Provide a small, reusable mechanism to validate external payloads (API/Kafka)
against a contract and detect schema drift early.

Stage 28 goal
------------
Make payload contracts *operationally useful*:
- support contract samples (one or many)
- provide a CLI to validate contracts/samples in CI
- add optional capture mode to store failing payload samples safely (redacted)

Design principles
-----------------
- Contracts are optional and configurable per manifest/source.
- Error messages must be safe (no payload values, no secrets).
- Support enforcement modes:
    * off: disabled
    * warn: log warnings, continue
    * deny: raise exception, fail the run
- Keep dependencies light: use `jsonschema` only when enabled.
"""

from __future__ import annotations

import json
import logging
import re
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Literal, Mapping, Optional, Sequence, Tuple

from pydantic import BaseModel, ConfigDict, field_validator, model_validator

from dltaf.services.execution.redaction import RedactionOptions, redact_obj, safe_exception_message


PayloadContractMode = Literal["off", "warn", "deny"]


class PayloadContractCaptureConfig(BaseModel):
    """Optional capture settings.

    Capture is useful when you run in `warn` mode and want to collect real-world
    drift samples to update the schema.

    IMPORTANT:
        Captured payloads may contain sensitive data. We redact obvious secrets
        by key name and apply truncation/size limits.
    """

    model_config = ConfigDict(extra="forbid")

    enabled: bool = False

    # Where to write captured samples. If not set, defaults to:
    #   <schema_dir>/captured
    # or, for inline schema:
    #   <manifest_dir>/contracts/captured
    dir: Optional[str] = None

    # Filename template. Available placeholders:
    #   {contract} {pipeline} {kind} {run_id} {ts} {rand}
    filename_template: str = "{contract}_{pipeline}_{kind}_{run_id}_{ts}_{rand}.json"

    # Max bytes for the captured JSON file.
    max_bytes: int = 262_144

    # Whether to redact obvious secret fields by key name.
    redact: bool = True

    @field_validator("filename_template")
    @classmethod
    def _tmpl_non_empty(cls, v: Any) -> str:
        s = str(v or "").strip()
        if not s:
            raise ValueError("filename_template must be a non-empty string")
        return s

    @field_validator("max_bytes")
    @classmethod
    def _max_bytes_positive(cls, v: Any) -> int:
        n = int(v)
        if n <= 0:
            raise ValueError("max_bytes must be > 0")
        return n


class PayloadContractConfig(BaseModel):
    """Manifest-driven contract configuration.

    Examples:

    ```yaml
    source:
      payload_contract:
        mode: warn
        schema_path: dlt_pipelines/my_integration/contracts/payload.schema.json

        # Optional: explicit sample list.
        # If omitted, samples are discovered next to schema_path:
        #   - <stem>.sample.json
        #   - <stem>.sample.*.json
        #   - contracts/samples/*.json
        samples:
          - dlt_pipelines/my_integration/contracts/payload.sample.json

        # Optional: capture failing payloads (redacted + size-limited)
        capture:
          enabled: true
          dir: dlt_pipelines/my_integration/contracts/captured
    ```
    """

    model_config = ConfigDict(extra="forbid")

    mode: PayloadContractMode = "off"

    # Contract schema source (one of):
    schema_path: Optional[str] = None
    schema_inline: Optional[Dict[str, Any]] = None

    # Optional: explicit sample file list for contract tests.
    samples: Optional[List[str]] = None

    # Optional: capture failing payloads.
    capture: Optional[PayloadContractCaptureConfig] = None

    max_errors: int = 20

    @model_validator(mode="after")
    def _schema_required_when_enabled(self) -> "PayloadContractConfig":
        if self.mode != "off" and not (self.schema_path or self.schema_inline):
            raise ValueError(
                "payload_contract.schema_path or payload_contract.schema_inline is required when mode != off"
            )
        return self

    @field_validator("samples")
    @classmethod
    def _samples_non_empty_strings(cls, v: Any) -> Any:
        if v is None:
            return v
        if not isinstance(v, list):
            raise ValueError("samples must be a list of file paths")
        out: List[str] = []
        for item in v:
            s = str(item or "").strip()
            if not s:
                raise ValueError("samples must contain non-empty strings")
            out.append(s)
        return out


class PayloadContractViolation(ValueError):
    """Raised when payload does not satisfy the configured contract."""

    def __init__(
        self,
        message: str,
        *,
        contract_name: str,
        schema_ref: Optional[str],
        errors: Sequence[str],
        capture_path: Optional[str] = None,
        capture_meta_path: Optional[str] = None,
    ) -> None:
        super().__init__(message)
        self.contract_name = contract_name
        self.schema_ref = schema_ref
        self.errors = list(errors)
        self.capture_path = capture_path
        self.capture_meta_path = capture_meta_path


@dataclass(frozen=True)
class PayloadContractRuntimeContext:
    """Runtime context (used for capture file naming and safe diagnostics)."""

    run_id: str
    pipeline_name: str
    source_kind: str


@dataclass(frozen=True)
class PayloadContractCaptureSettings:
    enabled: bool
    dir: Path
    filename_template: str
    max_bytes: int
    redact: bool


@dataclass(frozen=True)
class PayloadContract:
    """Compiled payload contract."""

    name: str
    mode: Literal["warn", "deny"]
    schema_ref: Optional[str]
    validator: Any
    max_errors: int = 20
    capture: Optional[PayloadContractCaptureSettings] = None
    runtime: Optional[PayloadContractRuntimeContext] = None

    def validate(self, payload: Any, *, logger: logging.Logger, context: str) -> bool:
        """Validate payload.

        Returns True if valid.
        In warn mode returns False on violation (and logs warning).
        In deny mode raises PayloadContractViolation.

        Capture mode:
            If enabled, stores a redacted + size-limited JSON sample and a meta
            file with safe error details.
        """

        errors = list(_iter_validation_errors_safe(self.validator, payload))
        if not errors:
            return True

        formatted = _format_validation_errors(errors, limit=self.max_errors)

        capture_path: Optional[str] = None
        capture_meta_path: Optional[str] = None
        if self.capture and self.capture.enabled:
            capture_path, capture_meta_path = _try_capture_payload(
                payload,
                errors=formatted,
                contract=self,
                context=context,
                logger=logger,
            )

        summary = (
            f"Payload contract failed: contract={self.name}, mode={self.mode}, "
            f"context={context}, errors={len(errors)}"
        )
        if self.schema_ref:
            summary += f", schema={self.schema_ref}"
        if capture_path:
            summary += f", captured={capture_path}"

        # Never include payload values.
        details = "; ".join(formatted)

        if self.mode == "warn":
            logger.warning("%s; %s", summary, details)
            return False

        # deny
        raise PayloadContractViolation(
            summary,
            contract_name=self.name,
            schema_ref=self.schema_ref,
            errors=formatted,
            capture_path=capture_path,
            capture_meta_path=capture_meta_path,
        )


def build_payload_contract(
    cfg: Optional[PayloadContractConfig],
    *,
    name: str,
    manifest_path: Path,
    logger: Optional[logging.Logger] = None,
    runtime: Optional[PayloadContractRuntimeContext] = None,
) -> Optional[PayloadContract]:
    """Build (compile) a payload contract.

    Args:
        cfg: contract config from manifest (may be None)
        name: contract identifier (integration/job name)
        manifest_path: path to the manifest file (used for relative schema paths)
        runtime: optional runtime context (capture file naming)

    Returns:
        Compiled PayloadContract or None (if disabled).
    """

    if cfg is None:
        return None

    mode = str(cfg.mode or "off")
    if mode == "off":
        return None
    if mode not in {"warn", "deny"}:
        raise ValueError(f"Unsupported payload_contract.mode: {mode!r}")

    log = logger or logging.getLogger(__name__)

    schema_ref: Optional[str] = None
    schema_obj: Optional[Mapping[str, Any]] = None
    schema_path_resolved: Optional[Path] = None

    if cfg.schema_inline is not None:
        schema_obj = cfg.schema_inline
        schema_ref = "inline"
    elif cfg.schema_path:
        schema_path_resolved = resolve_contract_path(
            str(cfg.schema_path),
            manifest_path,
            must_exist=True,
            what="payload_contract.schema_path",
        )
        schema_ref = str(schema_path_resolved)
        try:
            schema_obj = json.loads(schema_path_resolved.read_text(encoding="utf-8"))
        except Exception as e:
            raise ValueError(f"Failed to read JSON schema from {schema_path_resolved}: {e}") from e
    else:
        # Pydantic validator should prevent this.
        raise ValueError("payload_contract.schema_path or payload_contract.schema_inline is required")

    if not isinstance(schema_obj, Mapping):
        raise ValueError("payload_contract schema must be a JSON object")

    max_errors = int(cfg.max_errors or 20)
    max_errors = max(1, min(200, max_errors))

    validator = _compile_json_schema(schema_obj, log)

    capture_settings: Optional[PayloadContractCaptureSettings] = None
    if cfg.capture is not None and bool(cfg.capture.enabled):
        capture_settings = _build_capture_settings(
            cfg.capture,
            manifest_path=manifest_path,
            schema_path=schema_path_resolved,
        )

    return PayloadContract(
        name=name,
        mode=mode,  # type: ignore[arg-type]
        schema_ref=schema_ref,
        validator=validator,
        max_errors=max_errors,
        capture=capture_settings,
        runtime=runtime,
    )


def resolve_contract_path(
    path_str: str,
    manifest_path: Path,
    *,
    must_exist: bool,
    what: str,
) -> Path:
    """Resolve a path relative to manifest dir or project root."""

    p = Path(path_str).expanduser()
    if p.is_absolute():
        if must_exist and not p.exists():
            raise FileNotFoundError(f"{what} not found: {p}")
        return p

    cand1 = (manifest_path.parent / p).resolve()
    if cand1.exists() or not must_exist:
        return cand1

    root = _find_project_root(manifest_path)
    cand2 = (root / p).resolve()
    if cand2.exists() or not must_exist:
        return cand2

    raise FileNotFoundError(f"{what} not found. Tried: {cand1} and {cand2}")


def resolve_payload_contract_schema_path(
    cfg: PayloadContractConfig,
    *,
    manifest_path: Path,
) -> Optional[Path]:
    """Resolve schema path if schema_path is used, otherwise return None."""

    if not cfg.schema_path:
        return None
    return resolve_contract_path(
        str(cfg.schema_path),
        manifest_path,
        must_exist=True,
        what="payload_contract.schema_path",
    )


def discover_samples_for_schema(schema_path: Path) -> List[Path]:
    """Discover sample files near schema.

    Conventions:
      - <stem>.sample.json
      - <stem>.sample.*.json
      - <schema_dir>/samples/*.json

    Example:
      payload.schema.json -> payload.sample.json, payload.sample.v2.json
    """

    schema_path = schema_path.resolve()
    dir_path = schema_path.parent

    name = schema_path.name
    stem: str
    if name.endswith(".schema.json"):
        stem = name[: -len(".schema.json")]
    else:
        stem = schema_path.stem

    candidates: List[Path] = []

    # Primary: <stem>.sample.json
    primary = dir_path / f"{stem}.sample.json"
    if primary.exists():
        candidates.append(primary)

    # Additional: <stem>.sample.*.json (versioned)
    for p in sorted(dir_path.glob(f"{stem}.sample.*.json")):
        if p.exists() and p not in candidates:
            candidates.append(p)

    # Additional folder: samples/*.json
    samples_dir = dir_path / "samples"
    if samples_dir.exists() and samples_dir.is_dir():
        for p in sorted(samples_dir.glob("*.json")):
            if p.exists() and p not in candidates:
                candidates.append(p)

    return candidates


def resolve_payload_contract_sample_paths(
    cfg: PayloadContractConfig,
    *,
    manifest_path: Path,
    schema_path: Optional[Path],
) -> List[Path]:
    """Resolve sample file paths.

    Priority:
      1) cfg.samples (explicit list)
      2) discovery based on schema_path
    """

    if cfg.samples:
        out: List[Path] = []
        for s in cfg.samples:
            p = resolve_contract_path(
                str(s),
                manifest_path,
                must_exist=True,
                what="payload_contract.samples[]",
            )
            out.append(p)
        return out

    if schema_path is None:
        return []

    return discover_samples_for_schema(schema_path)


def _compile_json_schema(schema_obj: Mapping[str, Any], logger: logging.Logger) -> Any:
    try:
        from jsonschema import Draft202012Validator

        return Draft202012Validator(schema_obj)
    except ModuleNotFoundError as e:
        raise RuntimeError(
            "jsonschema dependency is required for payload contracts. "
            "Add 'jsonschema' to requirements.txt"
        ) from e
    except Exception as e:
        logger.error("Failed to compile JSON schema: %s", safe_exception_message(e))
        raise


def _find_project_root(start: Path) -> Path:
    """Find project root by searching for pyproject.toml upwards."""

    p = start.resolve()
    for parent in [p.parent, *p.parents]:
        if (parent / "pyproject.toml").exists():
            return parent
    # fallback
    return p.parent


def _iter_validation_errors_safe(validator: Any, payload: Any) -> Iterable[Any]:
    """Iterate validation errors safely."""

    try:
        return validator.iter_errors(payload)
    except Exception:
        # If validator fails unexpectedly, treat it as a single error.
        class _Dummy:
            path: Tuple[Any, ...] = ()
            message: str = "validator_failed"

        return [_Dummy()]


def _format_validation_errors(errors: Sequence[Any], *, limit: int) -> List[str]:
    out: List[str] = []
    for e in errors[: max(1, int(limit))]:
        try:
            path = _format_error_path(getattr(e, "path", None))
            msg = str(getattr(e, "message", ""))
            if path:
                out.append(f"{path}: {msg}")
            else:
                out.append(msg)
        except Exception:
            out.append("validation_error")
    return out


def _format_error_path(path: Any) -> str:
    try:
        if path is None:
            return ""
        parts = list(path)
        if not parts:
            return "$"
        s = "$"
        for p in parts:
            if isinstance(p, int):
                s += f"[{p}]"
            else:
                # keys: avoid quoting for readability; this is safe (no values)
                s += f".{p}"
        return s
    except Exception:
        return ""


# ---------------------------------------------------------------------------
# Capture helpers
# ---------------------------------------------------------------------------


def _build_capture_settings(
    cfg: PayloadContractCaptureConfig,
    *,
    manifest_path: Path,
    schema_path: Optional[Path],
) -> PayloadContractCaptureSettings:
    max_bytes = int(cfg.max_bytes)
    max_bytes = max(1_024, min(10_000_000, max_bytes))

    if cfg.dir:
        dir_path = resolve_contract_path(
            str(cfg.dir),
            manifest_path,
            must_exist=False,
            what="payload_contract.capture.dir",
        )
    else:
        if schema_path is not None:
            dir_path = (schema_path.parent / "captured").resolve()
        else:
            dir_path = (manifest_path.parent / "contracts" / "captured").resolve()

    return PayloadContractCaptureSettings(
        enabled=True,
        dir=dir_path,
        filename_template=str(cfg.filename_template),
        max_bytes=max_bytes,
        redact=bool(cfg.redact),
    )


def _try_capture_payload(
    payload: Any,
    *,
    errors: Sequence[str],
    contract: PayloadContract,
    context: str,
    logger: logging.Logger,
) -> Tuple[Optional[str], Optional[str]]:
    """Try to capture a failing payload sample.

    Never raises. Returns (payload_path, meta_path).
    """

    settings = contract.capture
    if settings is None or not settings.enabled:
        return None, None

    # Ensure dir exists.
    try:
        settings.dir.mkdir(parents=True, exist_ok=True)
    except Exception as e:
        logger.warning(
            "payload_contract capture: failed to create dir %s: %s",
            settings.dir,
            safe_exception_message(e),
        )
        return None, None

    runtime = contract.runtime
    pipeline = runtime.pipeline_name if runtime else "unknown_pipeline"
    kind = runtime.source_kind if runtime else "unknown_kind"
    run_id = runtime.run_id if runtime else "unknown_run_id"

    ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    rand = uuid.uuid4().hex[:8]

    fname = settings.filename_template.format(
        contract=_safe_filename(contract.name),
        pipeline=_safe_filename(pipeline),
        kind=_safe_filename(kind),
        run_id=_safe_filename(run_id),
        ts=ts,
        rand=rand,
    )

    # Ensure .json
    if not fname.lower().endswith(".json"):
        fname += ".json"

    payload_path = (settings.dir / fname).resolve()
    meta_path = payload_path.with_suffix(".meta.json")

    prepared = _prepare_payload_for_capture(payload, redact=settings.redact)

    raw = _safe_json_dumps(prepared)
    if len(raw.encode("utf-8", errors="ignore")) > settings.max_bytes:
        # If still too big, store a tiny payload and keep details in meta.
        raw = _safe_json_dumps({"__capture__": "payload_too_large", "note": "see meta"})

    try:
        payload_path.write_text(raw, encoding="utf-8")
    except Exception as e:
        logger.warning(
            "payload_contract capture: failed to write %s: %s",
            payload_path,
            safe_exception_message(e),
        )
        payload_path = None  # type: ignore[assignment]

    meta = {
        "ts": ts,
        "contract": contract.name,
        "schema": contract.schema_ref,
        "mode": contract.mode,
        "pipeline": pipeline,
        "kind": kind,
        "run_id": run_id,
        "context": context,
        "errors": list(errors),
        "note": "values redacted/truncated; do not commit sensitive data",
    }

    try:
        meta_path.write_text(_safe_json_dumps(meta), encoding="utf-8")
    except Exception as e:
        logger.warning(
            "payload_contract capture: failed to write meta %s: %s",
            meta_path,
            safe_exception_message(e),
        )
        meta_path = None  # type: ignore[assignment]

    return str(payload_path) if payload_path else None, str(meta_path) if meta_path else None


def _safe_filename(text: str) -> str:
    s = str(text or "").strip()
    if not s:
        return "unknown"
    # Replace path separators and other problematic chars.
    s = s.replace("/", "_").replace("\\", "_")
    s = re.sub(r"[^A-Za-z0-9_.-]+", "_", s)
    return s[:120]


def _safe_json_dumps(obj: Any) -> str:
    try:
        return json.dumps(obj, ensure_ascii=False, indent=2, sort_keys=True)
    except Exception:
        return json.dumps({"__capture__": "json_dump_failed"}, ensure_ascii=True)


def _prepare_payload_for_capture(payload: Any, *, redact: bool) -> Any:
    """Redact and truncate payload for safe capture."""

    opts = RedactionOptions(
        max_depth=8,
        max_list=100,
        max_str=1024,
        redact_keys=bool(redact),
        redact_text=bool(redact),
    )
    return redact_obj(payload, options=opts)
