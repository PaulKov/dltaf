from __future__ import annotations

import re
from typing import List, Sequence, Tuple

from .models import Issue

_BLOCK_RE_TEMPLATE = r"^(?P<indent>\s*){key}\s*:\s*(?:#.*)?$"
_FIELD_RE_TEMPLATE = r"^(?P<indent>\s*){key}\s*:\s*(?P<rest>.*)$"


def ensure_version(lines: List[str], version_value: str = "1") -> bool:
    for line in lines:
        if line.lstrip().startswith("#"):
            continue
        if re.match(r"^\s*version\s*:\s*", line):
            return False

    idx = 0
    if lines and lines[0].strip() == "---":
        idx = 1
    else:
        while idx < len(lines) and (lines[idx].strip() == "" or lines[idx].lstrip().startswith("#")):
            idx += 1

    lines.insert(idx, f"version: {version_value}\n")
    return True


def set_scalar_in_block(lines: List[str], *, block: str, field: str, value: str) -> bool:
    block_re = re.compile(_BLOCK_RE_TEMPLATE.format(key=re.escape(block)))
    field_re = re.compile(_FIELD_RE_TEMPLATE.format(key=re.escape(field)))

    block_idx = None
    block_indent = 0
    for i, line in enumerate(lines):
        match = block_re.match(line)
        if match:
            block_idx = i
            block_indent = len(match.group("indent") or "")
            break
    if block_idx is None:
        return False

    end = block_idx + 1
    while end < len(lines):
        line = lines[end]
        if line.strip() == "" or line.lstrip().startswith("#"):
            end += 1
            continue
        indent = len(line) - len(line.lstrip())
        if indent <= block_indent:
            break
        end += 1

    for idx in range(block_idx + 1, end):
        line = lines[idx]
        if line.strip() == "" or line.lstrip().startswith("#"):
            continue
        indent = len(line) - len(line.lstrip())
        if indent <= block_indent:
            break
        match = field_re.match(line)
        if not match:
            continue
        rest = match.group("rest")
        comment = ""
        value_part = rest
        if "#" in rest:
            before, after = rest.split("#", 1)
            value_part = before.rstrip()
            comment = "#" + after.rstrip("\n")
        current = value_part.strip()
        quote = current[0] if len(current) >= 2 and current[0] == current[-1] and current[0] in {'"', "'"} else None
        new_value = f"{quote}{value}{quote}" if quote else str(value)
        new_line = f"{match.group('indent')}{field}: {new_value}"
        if comment:
            new_line += "  " + comment.lstrip()
        if line.endswith("\n"):
            new_line += "\n"
        lines[idx] = new_line
        return True

    insert_at = block_idx + 1
    while insert_at < len(lines):
        line = lines[insert_at]
        if line.strip() == "" or line.lstrip().startswith("#"):
            insert_at += 1
            continue
        indent = len(line) - len(line.lstrip())
        if indent <= block_indent:
            break
        break
    indent_str = " " * (block_indent + 2)
    lines.insert(insert_at, f"{indent_str}{field}: {value}\n")
    return True


def apply_fixes_to_text(text: str, issues: Sequence[Issue]) -> Tuple[str, List[str]]:
    lines = text.splitlines(keepends=True)
    applied: List[str] = []
    if any(issue.fix and issue.fix[0] == "__root__" and issue.fix[1] == "version" for issue in issues):
        if ensure_version(lines, "1"):
            applied.append("version: 1")
    for issue in issues:
        if not issue.fix:
            continue
        block, field, value = issue.fix
        if block == "__root__":
            continue
        if set_scalar_in_block(lines, block=block, field=field, value=value):
            applied.append(f"{block}.{field} -> {value}")
    return "".join(lines), applied


__all__ = ["apply_fixes_to_text", "ensure_version", "set_scalar_in_block"]
