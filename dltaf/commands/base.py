from __future__ import annotations

import argparse
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Callable, Optional, Sequence, cast


class Command(ABC):
    name: str
    help: str
    description: str
    examples: Sequence[str] = ()
    legacy_entrypoint: Optional[str] = None
    implementation: str = "native"

    @abstractmethod
    def register(self, subparsers: argparse._SubParsersAction) -> argparse.ArgumentParser:
        raise NotImplementedError

    @abstractmethod
    def run(self, args: argparse.Namespace, ctx) -> int:
        raise NotImplementedError




def ensure_parser(parser: object) -> argparse.ArgumentParser:
    return cast(argparse.ArgumentParser, parser)


class LegacyPassthroughCommand(Command):
    """Compatibility-first command.

    The new umbrella CLI delegates to mature legacy entrypoints until the
    implementation is migrated into `dltaf`.
    """

    implementation = "legacy-passthrough"

    def __init__(
        self,
        *,
        name: str,
        help: str,
        description: str,
        delegate: Callable[[Optional[Sequence[str]]], object],
        legacy_entrypoint: str,
        examples: Optional[Sequence[str]] = None,
    ) -> None:
        self.name = name
        self.help = help
        self.description = description
        self.delegate = delegate
        self.legacy_entrypoint = legacy_entrypoint
        self.examples = tuple(examples or ())

    def register(self, subparsers: argparse._SubParsersAction) -> argparse.ArgumentParser:
        epilog_lines = [
            f"Legacy entrypoint: `{self.legacy_entrypoint}`.",
            "",
            "This subcommand is compatibility-first: all trailing arguments are passed to the legacy parser.",
            "",
            "Examples:",
        ]
        if self.examples:
            epilog_lines.extend(f"  {example}" for example in self.examples)
        else:
            epilog_lines.append(f"  {self.legacy_entrypoint} --help")

        parser = subparsers.add_parser(
            self.name,
            help=self.help,
            description=self.description,
            epilog="\n".join(epilog_lines),
            formatter_class=argparse.RawDescriptionHelpFormatter,
        )
        parser.set_defaults(_command=self)
        return ensure_parser(parser)

    def run(self, args: argparse.Namespace, ctx) -> int:
        argv = list(getattr(args, "_passthrough_args", []) or [])
        try:
            result = self.delegate(argv)
        except SystemExit as exc:
            code = exc.code
            if code is None:
                return 0
            if isinstance(code, int):
                return code
            print(code)
            return 1
        if isinstance(result, int):
            return result
        return 0


@dataclass(frozen=True)
class CommandGroup:
    name: str
    help: str
    description: str
    commands: Sequence[Command] = field(default_factory=tuple)
