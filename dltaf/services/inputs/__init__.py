"""Input value resolution helpers for manifest-driven runners.

The resolver is deliberately source-agnostic: integrations can use it for BINs,
project ids, tenant ids, or any other partition key while keeping source-specific
business meaning outside the framework core.
"""

from dltaf.services.inputs.list_resolution import parse_csv_or_json_list, resolve_values_list

__all__ = ["parse_csv_or_json_list", "resolve_values_list"]
