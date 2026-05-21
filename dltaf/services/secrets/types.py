"""Native secret resolution value objects.

The runtime core uses these tiny types instead of the compatibility
``dlt_utils`` package, so manifest execution can stay package-first and safe
when consumer repositories still carry old embedded framework copies.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Dict, Mapping


@dataclass(frozen=True)
class ResolvedValue:
    """A resolved ENV value together with a safe-to-log provenance label."""

    value: str
    source: str


@dataclass
class ResolvedEnv:
    """ENV mapping with provenance metadata."""

    values: Dict[str, ResolvedValue] = field(default_factory=dict)

    def to_env_dict(self) -> Dict[str, str]:
        return {key: resolved.value for key, resolved in self.values.items()}

    def sources(self) -> Dict[str, str]:
        return {key: resolved.source for key, resolved in self.values.items()}

    def update(self, other: Mapping[str, ResolvedValue]) -> None:
        self.values.update(dict(other))
