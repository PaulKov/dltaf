"""Backward-compatible infra checks facade.

Stage 36 / Wave 3 migrated infra check registries into `dltaf.extensions.infra_checks.*`.
This module remains as a compatibility import path for legacy code and plugins.
"""

from dltaf.app.deprecation import warn_legacy_module

warn_legacy_module(__name__, replacement="dltaf.extensions.infra_checks")

from dltaf.extensions.infra_checks import (
    InfraCheck,
    InfraCheckInfo,
    InfraCheckRegistry,
    available_infra_check_names,
    build_infra_check_registry,
    evaluate_infra_checks,
    get_default_infra_check_registry,
    load_infra_check_plugins,
    resolve_infra_check_plugins,
    run_infra_checks,
    select_infra_check_names,
)
from dltaf.extensions.infra_checks.builtins import ClickHouseConnectivityCheck, KafkaConnectivityCheck

__all__ = [
    "ClickHouseConnectivityCheck",
    "InfraCheck",
    "InfraCheckInfo",
    "InfraCheckRegistry",
    "KafkaConnectivityCheck",
    "available_infra_check_names",
    "build_infra_check_registry",
    "evaluate_infra_checks",
    "get_default_infra_check_registry",
    "load_infra_check_plugins",
    "resolve_infra_check_plugins",
    "run_infra_checks",
    "select_infra_check_names",
]
