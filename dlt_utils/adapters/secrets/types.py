"""Types for secret / env resolution."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Dict, Mapping


@dataclass(frozen=True)
class ResolvedValue:
    """A resolved value together with its provenance.

    The `value` may contain a secret, so it must not be logged.
    The `source` is safe to log.
    """

    value: str
    source: str


@dataclass
class ResolvedEnv:
    """Env mapping with provenance information."""

    values: Dict[str, ResolvedValue] = field(default_factory=dict)

    def to_env_dict(self) -> Dict[str, str]:
        return {k: rv.value for k, rv in self.values.items()}

    def sources(self) -> Dict[str, str]:
        return {k: rv.source for k, rv in self.values.items()}

    def update(self, other: Mapping[str, ResolvedValue]) -> None:
        self.values.update(dict(other))
