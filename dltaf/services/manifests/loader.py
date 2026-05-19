from __future__ import annotations

from pathlib import Path
from typing import Any, Dict, Mapping, Optional

import yaml

from .resolver import ManifestResolver, resolve_manifest


class ManifestLoader:
    """Load YAML manifests and attach internal metadata.

    This service is intentionally small and dependency-light.
    """

    def __init__(self, *, resolver: Optional[ManifestResolver] = None) -> None:
        self.resolver = resolver or ManifestResolver()

    def load(self, path: str | Path) -> Dict[str, Any]:
        manifest_path = Path(path).expanduser().resolve()
        raw = yaml.safe_load(manifest_path.read_text(encoding="utf-8"))
        if not isinstance(raw, Mapping):
            raise ValueError(f"Manifest must be a YAML mapping, got: {type(raw)}")
        data = self.resolver.resolve(dict(raw))
        data["__manifest_path__"] = str(manifest_path)
        return dict(data)


_default_loader = ManifestLoader()


def load_manifest(path: str | Path) -> Dict[str, Any]:
    return _default_loader.load(path)


__all__ = ["ManifestLoader", "load_manifest", "resolve_manifest"]
