from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any, Dict, Iterable, List, MutableMapping, Sequence, Union

PathToken = Union[str, int]


@dataclass(frozen=True)
class OverrideSpec:
    path: str
    value: Any


class ManifestOverridesService:
    """Parse and apply runtime manifest overrides."""

    def split_kv(self, spec: str) -> tuple[str, str]:
        if "=" not in spec:
            raise ValueError(f"Invalid override {spec!r}. Expected KEY=VALUE.")
        key, value = spec.split("=", 1)
        normalized_key = str(key).strip()
        if not normalized_key:
            raise ValueError(f"Invalid override {spec!r}. KEY cannot be empty.")
        return normalized_key, value

    def parse_value(self, raw: str) -> Any:
        if raw == "":
            return ""
        text = str(raw)
        try:
            return json.loads(text)
        except Exception:
            return text

    def parse_set_args(self, set_args: Sequence[str]) -> List[OverrideSpec]:
        specs: List[OverrideSpec] = []
        for item in set_args or []:
            key, value = self.split_kv(str(item))
            specs.append(OverrideSpec(path=key, value=self.parse_value(value)))
        return specs

    def parse_path(self, path: str) -> List[PathToken]:
        normalized = str(path or "").strip()
        if not normalized:
            raise ValueError("Override path cannot be empty")

        tokens: List[PathToken] = []
        i = 0
        while i < len(normalized):
            ch = normalized[i]
            if ch == ".":
                i += 1
                continue
            if ch == "[":
                end = normalized.find("]", i + 1)
                if end < 0:
                    raise ValueError(f"Invalid override path {path!r}: missing closing ']'")
                idx_raw = normalized[i + 1 : end].strip()
                if idx_raw == "" or not idx_raw.isdigit():
                    raise ValueError(
                        f"Invalid override path {path!r}: list index must be an integer, got {idx_raw!r}"
                    )
                tokens.append(int(idx_raw))
                i = end + 1
                continue

            end = i
            while end < len(normalized) and normalized[end] not in ".[":
                end += 1
            key = normalized[i:end].strip()
            if not key:
                raise ValueError(f"Invalid override path {path!r}: empty key segment")
            tokens.append(key)
            i = end

        return tokens

    def apply_override(self, manifest: MutableMapping[str, Any], path: str, value: Any) -> None:
        tokens = self.parse_path(path)
        if not tokens:
            raise ValueError("Override path cannot be empty")

        cur: Any = manifest
        for idx, tok in enumerate(tokens[:-1]):
            nxt = tokens[idx + 1]
            want_list = isinstance(nxt, int)

            if isinstance(tok, str):
                if not isinstance(cur, MutableMapping):
                    raise TypeError(
                        f"Cannot apply override {path!r}: segment {tok!r} expects a mapping, got {type(cur)}"
                    )
                if tok not in cur or cur[tok] is None:
                    cur[tok] = [] if want_list else {}
                cur = cur[tok]
                continue

            if not isinstance(cur, list):
                raise TypeError(
                    f"Cannot apply override {path!r}: segment [{tok}] expects a list, got {type(cur)}"
                )
            while len(cur) <= tok:
                cur.append([] if want_list else {})
            if cur[tok] is None:
                cur[tok] = [] if want_list else {}
            cur = cur[tok]

        last = tokens[-1]
        if isinstance(last, str):
            if not isinstance(cur, MutableMapping):
                raise TypeError(
                    f"Cannot apply override {path!r}: final segment {last!r} expects a mapping, got {type(cur)}"
                )
            cur[last] = value
            return

        if not isinstance(cur, list):
            raise TypeError(
                f"Cannot apply override {path!r}: final segment [{last}] expects a list, got {type(cur)}"
            )
        while len(cur) <= last:
            cur.append(None)
        cur[last] = value

    def apply_overrides(self, manifest: MutableMapping[str, Any], overrides: Iterable[OverrideSpec]) -> None:
        for spec in overrides:
            self.apply_override(manifest, spec.path, spec.value)

    def specs_to_mapping(self, specs: Sequence[OverrideSpec]) -> Dict[str, Any]:
        out: Dict[str, Any] = {}
        for spec in specs or []:
            out[str(spec.path)] = spec.value
        return out


_default_service = ManifestOverridesService()


def parse_set_args(set_args: Sequence[str]) -> List[OverrideSpec]:
    return _default_service.parse_set_args(set_args)


def parse_path(path: str) -> List[PathToken]:
    return _default_service.parse_path(path)


def apply_override(manifest: MutableMapping[str, Any], path: str, value: Any) -> None:
    _default_service.apply_override(manifest, path, value)


def apply_overrides(manifest: MutableMapping[str, Any], overrides: Iterable[OverrideSpec]) -> None:
    _default_service.apply_overrides(manifest, overrides)


def specs_to_mapping(specs: Sequence[OverrideSpec]) -> Dict[str, Any]:
    return _default_service.specs_to_mapping(specs)


__all__ = [
    "OverrideSpec",
    "ManifestOverridesService",
    "parse_set_args",
    "parse_path",
    "apply_override",
    "apply_overrides",
    "specs_to_mapping",
]
