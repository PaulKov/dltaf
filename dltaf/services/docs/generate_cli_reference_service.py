from __future__ import annotations

import difflib
from pathlib import Path
from typing import List

from dltaf.commands.base import LegacyPassthroughCommand


class GenerateCliReferenceService:
    def __init__(self, *, ctx, logger) -> None:
        self.ctx = ctx
        self.logger = logger

    def _render(self) -> str:
        from dltaf.commands.registry import command_groups

        groups = command_groups()
        lines: List[str] = []
        lines.append("# Справочник CLI")
        lines.append("")
        lines.append(
            "Этот файл генерируется командой `dltaf docs generate-cli-reference`. "
            "Не редактируйте его вручную."
        )
        lines.append("")
        lines.append("## Корневая команда")
        lines.append("")
        lines.append("```bash")
        lines.append("dltaf <group> <command> [args]")
        lines.append("```")
        lines.append("")
        lines.append(
            "На текущем этапе `dltaf` сочетает native subcommand'ы миграционных волн и compatibility-first "
            "делегаты к legacy entrypoint'ам."
        )
        lines.append("")
        lines.append("## Карта команд")
        lines.append("")
        lines.append("| Group | Command | Purpose | Implementation | Legacy entrypoint | Alias status |")
        lines.append("| --- | --- | --- | --- | --- | --- |")
        for group in groups:
            for command in group.commands:
                impl = getattr(command, "implementation", "native")
                legacy = command.legacy_entrypoint or "—"
                alias_status = "deprecated alias" if command.legacy_entrypoint else "—"
                lines.append(f"| {group.name} | {command.name} | {command.help} | {impl} | {legacy} | {alias_status} |")
        lines.append("")

        for group in groups:
            lines.append(f"## {group.name}")
            lines.append("")
            lines.append(group.description)
            lines.append("")
            for command in group.commands:
                lines.append(f"### `dltaf {group.name} {command.name}`")
                lines.append("")
                lines.append(command.description)
                lines.append("")
                if isinstance(command, LegacyPassthroughCommand):
                    lines.append("Implementation: legacy passthrough")
                    lines.append("")
                    if command.legacy_entrypoint:
                        lines.append(f"Legacy entrypoint: `{command.legacy_entrypoint}`")
                        lines.append("")
                else:
                    lines.append("Реализация: native команда dltaf")
                    lines.append("")
                    if command.legacy_entrypoint:
                        lines.append(f"Совместимый legacy alias (deprecated): `{command.legacy_entrypoint}`")
                        lines.append("")
                if command.examples:
                    lines.append("Примеры:")
                    lines.append("")
                    lines.append("```bash")
                    for example in command.examples:
                        lines.append(example)
                    lines.append("```")
                    lines.append("")
        return "\n".join(lines).rstrip() + "\n"

    def run(self, args) -> int:
        docs_dir = Path(self.ctx.repo_root, str(args.docs_dir)).resolve()
        target = docs_dir / "CLI_REFERENCE.md"
        content = self._render()
        current = target.read_text(encoding="utf-8") if target.exists() else None

        if bool(args.check):
            if current == content:
                return 0
            if bool(args.show_diff):
                diff = list(
                    difflib.unified_diff(
                        (current or "").splitlines(),
                        content.splitlines(),
                        fromfile=str(target),
                        tofile="generated",
                        lineterm="",
                    )
                )
                max_lines = max(0, int(args.diff_lines or 0))
                for line in diff[:max_lines]:
                    print(line)
            return 1

        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text(content, encoding="utf-8")
        self.logger.info("CLI reference updated: %s", target)
        return 0
