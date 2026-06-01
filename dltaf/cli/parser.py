from __future__ import annotations

import argparse

from dltaf.commands.registry import command_groups


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="dltaf",
        description=(
            "dp-dlt-af umbrella CLI. Native dltaf command surface plus "
            "compatibility shims for commands not migrated yet."
        ),
    )
    parser.add_argument(
        "--repo-root",
        default=".",
        help="Repository root (default: current directory).",
    )

    groups = command_groups()
    subparsers = parser.add_subparsers(dest="_group")
    for group in groups:
        gp = subparsers.add_parser(
            group.name,
            help=group.help,
            description=group.description,
        )
        gp_sub = gp.add_subparsers(dest="_subcommand")
        for command in group.commands:
            command.register(gp_sub)

    return parser
