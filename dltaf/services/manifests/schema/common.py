from __future__ import annotations

from datetime import date, datetime
from enum import Enum
from typing import Any, Dict, List, Optional, Union

from pydantic import field_validator, model_validator

from dlt_utils.contracts import PayloadContractConfig
from dlt_utils.naming import validate_pipeline_name

from .base import AllowExtraModel, ForbidExtraModel, coerce_bool_or_env, coerce_int_or_env, is_env_placeholder


class PipelineConfig(AllowExtraModel):
    name: str
    destination: str
    dataset: str
    progress: Optional[str] = None
    dev_mode: Optional[Union[bool, str]] = False

    @field_validator("name", "destination", "dataset")
    @classmethod
    def non_empty(cls, value: Any) -> str:
        s = str(value or "").strip()
        if not s:
            raise ValueError("value must be a non-empty string")
        return s

    @field_validator("name")
    @classmethod
    def validate_name_convention(cls, value: Any) -> str:
        s = str(value or "").strip()
        validate_pipeline_name(s)
        return s

    @field_validator("dev_mode")
    @classmethod
    def dev_mode_boolish(cls, value: Any) -> Any:
        if value is None:
            return value
        return coerce_bool_or_env(value)


class PipelineConfigStrict(PipelineConfig, ForbidExtraModel):
    pass


class HooksConfig(AllowExtraModel):
    enable: Optional[List[str]] = None
    disable: Optional[List[str]] = None
    only: Optional[List[str]] = None
    plugins: Optional[List[str]] = None

    @field_validator("enable", "disable", "only", "plugins")
    @classmethod
    def normalize_lists(cls, value: Any) -> Any:
        if value is None:
            return value
        if isinstance(value, str):
            return [value]
        return [str(item).strip() for item in (value or []) if str(item).strip()]

    @model_validator(mode="after")
    def only_exclusive(self) -> "HooksConfig":
        if self.only and (self.enable or self.disable):
            raise ValueError("run.hooks.only cannot be combined with enable/disable")
        return self


class RunnersConfig(AllowExtraModel):
    plugins: Optional[List[str]] = None

    @field_validator("plugins")
    @classmethod
    def normalize_lists(cls, value: Any) -> Any:
        if value is None:
            return value
        if isinstance(value, str):
            return [value]
        return [str(item).strip() for item in (value or []) if str(item).strip()]


class RunnersConfigStrict(RunnersConfig, ForbidExtraModel):
    pass


class OnlineChecksConfig(AllowExtraModel):
    enable: Optional[List[str]] = None
    disable: Optional[List[str]] = None
    only: Optional[List[str]] = None
    plugins: Optional[List[str]] = None

    @field_validator("enable", "disable", "only", "plugins")
    @classmethod
    def normalize_lists(cls, value: Any) -> Any:
        if value is None:
            return value
        if isinstance(value, str):
            return [value]
        return [str(item).strip() for item in (value or []) if str(item).strip()]

    @model_validator(mode="after")
    def only_exclusive(self) -> "OnlineChecksConfig":
        if self.only and (self.enable or self.disable):
            raise ValueError("run.online_checks.only cannot be combined with enable/disable")
        return self


class ReplacePolicyConfig(AllowExtraModel):
    enabled: Optional[Union[bool, str]] = None
    enforce: Optional[str] = None
    prod_dataset_regex: Optional[str] = None
    allow_datasets: Optional[List[str]] = None
    allow_pipelines: Optional[List[str]] = None
    bypass_env_var: Optional[str] = None
    mode: Optional[str] = None

    @field_validator("enabled")
    @classmethod
    def enabled_boolish(cls, value: Any) -> Any:
        if value is None:
            return value
        return coerce_bool_or_env(value)

    @field_validator("allow_datasets", "allow_pipelines")
    @classmethod
    def normalize_lists(cls, value: Any) -> Any:
        if value is None:
            return value
        if isinstance(value, str):
            return [value]
        return [str(item).strip() for item in (value or []) if str(item).strip()]


class ReplacePolicyConfigStrict(ReplacePolicyConfig, ForbidExtraModel):
    pass


class PartialSuccessModeEnum(str, Enum):
    ANY_SUCCESS = "any_success"
    CRITICAL_TABLES = "critical_tables"
    THRESHOLD = "threshold"


class PartialSuccessToleranceEnum(str, Enum):
    ANY_PER_TABLE = "any_per_table"
    SOURCE_ONLY = "source_only"
    MISSING_TABLE_ONLY = "missing_table_only"


class PartialSuccessConfig(AllowExtraModel):
    mode: PartialSuccessModeEnum
    tolerate_errors: PartialSuccessToleranceEnum = PartialSuccessToleranceEnum.SOURCE_ONLY
    critical_tables: Optional[List[str]] = None
    min_success_tables: Optional[Union[int, str]] = None
    min_success_ratio: Optional[Union[float, str]] = None

    @field_validator("critical_tables")
    @classmethod
    def normalize_critical_tables(cls, value: Any) -> Any:
        if value is None:
            return value
        if isinstance(value, str):
            value = [value]
        items = [str(item).strip() for item in (value or []) if str(item).strip()]
        if not items:
            raise ValueError("run.partial_success.critical_tables must be a non-empty list")
        return items

    @field_validator("min_success_tables")
    @classmethod
    def min_success_tables_intish(cls, value: Any) -> Any:
        if value is None:
            return value
        parsed = coerce_int_or_env(value)
        try:
            if not is_env_placeholder(parsed) and int(parsed) <= 0:
                raise ValueError("run.partial_success.min_success_tables must be > 0")
        except ValueError:
            raise
        except Exception:
            pass
        return parsed

    @field_validator("min_success_ratio")
    @classmethod
    def min_success_ratio_floatish(cls, value: Any) -> Any:
        if value is None:
            return value
        if is_env_placeholder(value):
            return value
        try:
            parsed = float(value)
        except Exception as exc:
            raise ValueError("run.partial_success.min_success_ratio must be a float in [0, 1]") from exc
        if parsed < 0.0 or parsed > 1.0:
            raise ValueError("run.partial_success.min_success_ratio must be in [0, 1]")
        return parsed

    @model_validator(mode="after")
    def validate_mode_requirements(self) -> "PartialSuccessConfig":
        if self.mode == PartialSuccessModeEnum.ANY_SUCCESS:
            return self
        if self.mode == PartialSuccessModeEnum.CRITICAL_TABLES:
            if not self.critical_tables:
                raise ValueError("run.partial_success.critical_tables is required for mode=critical_tables")
            return self
        if self.mode == PartialSuccessModeEnum.THRESHOLD:
            if self.min_success_tables is None and self.min_success_ratio is None:
                raise ValueError(
                    "run.partial_success requires min_success_tables and/or min_success_ratio for mode=threshold"
                )
            return self
        return self


class PartialSuccessConfigStrict(PartialSuccessConfig, ForbidExtraModel):
    pass


class ObservabilityVerbosityEnum(str, Enum):
    COMPACT = "compact"
    VERBOSE = "verbose"


class DltProgressModeEnum(str, Enum):
    DEFAULT = "default"
    SUMMARY_ONLY = "summary_only"


class ObservabilityConfig(AllowExtraModel):
    verbosity: ObservabilityVerbosityEnum = ObservabilityVerbosityEnum.COMPACT
    dlt_progress: DltProgressModeEnum = DltProgressModeEnum.DEFAULT


class ObservabilityConfigStrict(ObservabilityConfig, ForbidExtraModel):
    pass


class CheckpointConfig(AllowExtraModel):
    enabled: Optional[Union[bool, str]] = False
    load_uuid: Optional[str] = None
    batch_key: Optional[str] = None
    table_name: Optional[str] = None
    table: Optional[str] = None
    resume_statuses: Optional[List[str]] = None

    @field_validator("enabled")
    @classmethod
    def enabled_boolish(cls, value: Any) -> Any:
        if value is None:
            return value
        return coerce_bool_or_env(value)


class CheckpointConfigStrict(CheckpointConfig, ForbidExtraModel):
    pass


class RunConfig(AllowExtraModel):
    write_disposition: Optional[str] = None
    replace_scope: Optional[str] = None
    hooks: Optional[HooksConfig] = None
    runners: Optional[RunnersConfig] = None
    online_checks: Optional[OnlineChecksConfig] = None
    replace_policy: Optional[ReplacePolicyConfig] = None
    partial_success: Optional[PartialSuccessConfig] = None
    observability: Optional[ObservabilityConfig] = None
    checkpoint: Optional[CheckpointConfig] = None


class RunConfigStrict(RunConfig, ForbidExtraModel):
    runners: Optional[RunnersConfigStrict] = None
    replace_policy: Optional[ReplacePolicyConfigStrict] = None
    partial_success: Optional[PartialSuccessConfigStrict] = None
    observability: Optional[ObservabilityConfigStrict] = None
    checkpoint: Optional[CheckpointConfigStrict] = None


class ConnectionSpec(AllowExtraModel):
    kind: str
    vault: Optional[Union[str, "VaultRefConfig"]] = None
    airflow_variable: Optional[str] = None
    airflow_variable_prefix: Optional[str] = None
    airflow_variables: Optional[Dict[str, str]] = None
    env_prefix: Optional[str] = None
    overrides: Optional[Dict[str, Any]] = None

    @field_validator("kind")
    @classmethod
    def kind_non_empty(cls, value: Any) -> str:
        s = str(value or "").strip()
        if not s:
            raise ValueError("connections.*.kind is required")
        return s


class ConnectionSpecStrict(ConnectionSpec, ForbidExtraModel):
    vault: Optional[Union[str, "VaultRefConfigStrict"]] = None


class ConnectionsConfig(AllowExtraModel):
    source: Optional[ConnectionSpec] = None
    destination: Optional[ConnectionSpec] = None
    kafka: Optional[ConnectionSpec] = None


class ConnectionsConfigStrict(ForbidExtraModel):
    source: Optional[ConnectionSpecStrict] = None
    destination: Optional[ConnectionSpecStrict] = None
    kafka: Optional[ConnectionSpecStrict] = None


class VaultRefConfig(AllowExtraModel):
    ref: Optional[str] = None
    mount_point: Optional[str] = None
    path: Optional[str] = None
    kv_version: Optional[Union[int, str]] = None

    @field_validator("ref", "mount_point", "path")
    @classmethod
    def normalize_optional_str(cls, value: Any) -> Any:
        if value is None:
            return value
        s = str(value or "").strip()
        if not s:
            raise ValueError("value must be a non-empty string")
        return s

    @field_validator("kv_version")
    @classmethod
    def normalize_kv_version(cls, value: Any) -> Any:
        if value is None:
            return value
        return str(value).strip()

    @model_validator(mode="after")
    def validate_shape(self) -> "VaultRefConfig":
        has_ref = bool(self.ref)
        has_mount_parts = bool(self.mount_point) or bool(self.path)

        if has_ref and has_mount_parts:
            raise ValueError("vault mapping must use either ref or mount_point/path, not both")

        if has_ref:
            return self

        if bool(self.mount_point) != bool(self.path):
            raise ValueError("vault mapping requires both mount_point and path")

        if not self.mount_point or not self.path:
            raise ValueError("vault mapping requires ref or mount_point/path")

        return self


class VaultRefConfigStrict(VaultRefConfig, ForbidExtraModel):
    pass


class DependsOnRef(AllowExtraModel):
    path: Optional[str] = None
    group: Optional[str] = None

    @model_validator(mode="after")
    def exactly_one(self) -> "DependsOnRef":
        if bool(self.path) == bool(self.group):
            raise ValueError("depends_on item must have exactly one of: path | group")
        return self


class DependsOnRefStrict(DependsOnRef, ForbidExtraModel):
    pass


class AirflowConfig(AllowExtraModel):
    dag_id: str
    schedule: str
    start_date: str
    catchup: Optional[Union[bool, str]] = False
    max_active_runs: Optional[Union[int, str]] = None
    tags: Optional[List[str]] = None
    retries: Optional[Union[int, str]] = None
    retry_delay_minutes: Optional[Union[int, str]] = None
    execution_timeout_hours: Optional[Union[int, str]] = None
    default_args: Optional[Dict[str, Any]] = None
    task: Optional[Dict[str, Any]] = None
    params: Optional[Dict[str, Any]] = None
    runtime_overrides: Optional[Dict[str, Any]] = None

    @field_validator("dag_id", "schedule", "start_date")
    @classmethod
    def required_non_empty(cls, value: Any) -> str:
        s = str(value or "").strip()
        if not s:
            raise ValueError("value must be a non-empty string")
        return s

    @field_validator("catchup")
    @classmethod
    def catchup_boolish(cls, value: Any) -> Any:
        if value is None:
            return value
        return coerce_bool_or_env(value)

    @field_validator("max_active_runs", "retries", "retry_delay_minutes", "execution_timeout_hours")
    @classmethod
    def int_fields(cls, value: Any) -> Any:
        if value is None:
            return value
        return coerce_int_or_env(value)


class AirflowConfigStrict(AirflowConfig, ForbidExtraModel):
    pass


class TimeWindowConfig(ForbidExtraModel):
    start: Optional[str] = None
    end: Optional[str] = None
    timezone: Optional[str] = None

    @staticmethod
    def parse_iso(value: str) -> None:
        s = str(value).strip()
        if not s:
            raise ValueError("value must be a non-empty ISO date/datetime")
        if is_env_placeholder(s):
            return
        s2 = s.replace("Z", "+00:00")
        try:
            datetime.fromisoformat(s2)
            return
        except Exception:
            pass
        try:
            date.fromisoformat(s)
            return
        except Exception as exc:
            raise ValueError(f"expected ISO date/datetime, got: {value!r}") from exc

    @field_validator("start", "end")
    @classmethod
    def iso_dates(cls, value: Any) -> Any:
        if value is None:
            return value
        cls.parse_iso(str(value))
        return value

    @field_validator("timezone")
    @classmethod
    def timezone_non_empty_if_set(cls, value: Any) -> Any:
        if value is None:
            return value
        s = str(value or "").strip()
        if not s:
            raise ValueError("timezone must be a non-empty string when provided")
        return s


class SourceBase(AllowExtraModel):
    kind: str
    name: Optional[str] = None
    payload_contract: Optional[PayloadContractConfig] = None
    time_window: Optional[TimeWindowConfig] = None

    @field_validator("kind")
    @classmethod
    def kind_required(cls, value: Any) -> str:
        s = str(value or "").strip()
        if not s:
            raise ValueError("source.kind is required")
        return s


__all__ = [
    "AirflowConfig",
    "AirflowConfigStrict",
    "ConnectionSpec",
    "ConnectionSpecStrict",
    "ConnectionsConfig",
    "ConnectionsConfigStrict",
    "DependsOnRef",
    "DependsOnRefStrict",
    "HooksConfig",
    "OnlineChecksConfig",
    "PipelineConfig",
    "PipelineConfigStrict",
    "ReplacePolicyConfig",
    "ReplacePolicyConfigStrict",
    "RunConfig",
    "RunConfigStrict",
    "RunnersConfig",
    "RunnersConfigStrict",
    "SourceBase",
    "TimeWindowConfig",
]
