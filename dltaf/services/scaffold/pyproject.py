from __future__ import annotations

import re
from pathlib import Path


def ensure_package_in_pyproject(pyproject_path: Path, package: str) -> bool:
    if not pyproject_path.exists():
        return False
    text = pyproject_path.read_text(encoding="utf-8")
    if f'"{package}"' in text or f"'{package}'" in text:
        return False
    lines = text.splitlines(True)
    start_idx = None
    for i, line in enumerate(lines):
        if line.strip() == "[tool.setuptools]":
            start_idx = i
            break
    if start_idx is None:
        return False
    pkg_start = None
    for i in range(start_idx, len(lines)):
        if "packages" in lines[i] and "[" in lines[i] and re.match(r"\s*packages\s*=\s*\[", lines[i]):
            pkg_start = i
            break
    if pkg_start is None:
        return False
    pkg_end = None
    for i in range(pkg_start + 1, len(lines)):
        if "]" in lines[i]:
            pkg_end = i
            break
    if pkg_end is None:
        return False
    insert_at = pkg_end
    last_dlt_pipelines = None
    for i in range(pkg_start + 1, pkg_end):
        if "dlt_pipelines." in lines[i]:
            last_dlt_pipelines = i
    if last_dlt_pipelines is not None:
        insert_at = last_dlt_pipelines + 1
    else:
        for i in range(pkg_start + 1, pkg_end):
            if '"dlt_pipelines"' in lines[i] or "'dlt_pipelines'" in lines[i]:
                insert_at = i + 1
                break
    lines.insert(insert_at, f'  "{package}",\n')
    pyproject_path.write_text("".join(lines), encoding="utf-8")
    return True


__all__ = ["ensure_package_in_pyproject"]
