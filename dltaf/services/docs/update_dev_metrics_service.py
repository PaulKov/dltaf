from __future__ import annotations

import difflib
from pathlib import Path

START = "<!-- DLTAF_DEV_METRICS_START -->"
END = "<!-- DLTAF_DEV_METRICS_END -->"


class UpdateDevMetricsService:
    def __init__(self, *, ctx, logger) -> None:
        self.ctx = ctx
        self.logger = logger

    def _replace_block(self, text: str, block: str) -> str:
        replacement = f"{START}\n{block}\n{END}"
        if START in text and END in text:
            before, rest = text.split(START, 1)
            _, after = rest.split(END, 1)
            return before.rstrip() + "\n\n" + replacement + after
        return text.rstrip() + "\n\n" + replacement + "\n"

    def run(self, args) -> int:
        from ci_scripts.code_metrics import (
            analyze_project,
            render_dev_metrics_block_ru,
            render_report_ru,
            summarize_metrics,
        )

        docs_dir = Path(self.ctx.repo_root, str(args.docs_dir)).resolve()
        dev_path = docs_dir / "DEVELOPERS.md"
        metrics_path = docs_dir / "CODE_METRICS.md"

        files = analyze_project(self.ctx.repo_root, include_dags=False)
        summary = summarize_metrics(files)
        block = render_dev_metrics_block_ru(summary)
        metrics_report = render_report_ru(files)

        current_dev = dev_path.read_text(encoding="utf-8")
        new_dev = self._replace_block(current_dev, block)
        current_metrics = metrics_path.read_text(encoding="utf-8") if metrics_path.exists() else ""

        if bool(args.check):
            changed = False
            if current_dev != new_dev:
                changed = True
                if bool(args.show_diff):
                    diff = list(
                        difflib.unified_diff(
                            current_dev.splitlines(),
                            new_dev.splitlines(),
                            fromfile=str(dev_path),
                            tofile="generated",
                            lineterm="",
                        )
                    )
                    for line in diff[: max(0, int(args.diff_lines or 0))]:
                        print(line)
            if current_metrics != metrics_report:
                changed = True
                if bool(args.show_diff):
                    diff = list(
                        difflib.unified_diff(
                            current_metrics.splitlines(),
                            metrics_report.splitlines(),
                            fromfile=str(metrics_path),
                            tofile="generated",
                            lineterm="",
                        )
                    )
                    for line in diff[: max(0, int(args.diff_lines or 0))]:
                        print(line)
            return 1 if changed else 0

        metrics_path.write_text(metrics_report, encoding="utf-8")
        dev_path.write_text(new_dev, encoding="utf-8")
        self.logger.info("Developer metrics updated: %s, %s", metrics_path, dev_path)
        return 0
