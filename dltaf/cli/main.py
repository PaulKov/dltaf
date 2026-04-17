from __future__ import annotations

import sys
from typing import Optional, Sequence

from dltaf.app.context import AppContext
from dltaf.cli.parser import build_parser
from dltaf.commands.base import LegacyPassthroughCommand


def main(argv: Optional[Sequence[str]] = None) -> int:
    parser = build_parser()
    args, unknown = parser.parse_known_args(argv)
    command = getattr(args, "_command", None)
    if command is None:
        parser.print_help()
        return 2
    if isinstance(command, LegacyPassthroughCommand):
        setattr(args, "_passthrough_args", list(unknown))
    elif unknown:
        parser.error("unrecognized arguments: " + " ".join(unknown))
    ctx = AppContext.build(repo_root=getattr(args, "repo_root", "."))
    return int(command.run(args, ctx))


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main(sys.argv[1:]))
