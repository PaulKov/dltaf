from __future__ import annotations

from pathlib import Path

from .manifest import manifest_json_schema_str


class ManifestSchemaService:
    def __init__(self, *, logger) -> None:
        self.logger = logger

    def generate(self, *, strict_common: bool = True, strict_source: bool = True) -> str:
        return manifest_json_schema_str(strict=strict_common, strict_source=strict_source)

    def write_or_print(self, *, out: str | None, strict_common: bool = True, strict_source: bool = True) -> int:
        content = self.generate(strict_common=strict_common, strict_source=strict_source)
        if out:
            target = Path(str(out)).expanduser().resolve()
            target.parent.mkdir(parents=True, exist_ok=True)
            target.write_text(content, encoding="utf-8")
            print(f"Wrote schema: {target}")
            return 0
        print(content)
        return 0


__all__ = ["ManifestSchemaService"]
