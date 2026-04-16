from __future__ import annotations

import argparse
from pathlib import Path
from typing import Sequence

from dlt_utils.manifest_runner import get_source_registry, load_manifest
from dltaf.plugins import SourcePluginNotFoundError, scaffold_plugin_source


def _print_plugins() -> None:
    registry = get_source_registry()
    if not registry.records():
        print("No source plugins discovered.")
        return

    for record in registry.records():
        aliases = ", ".join(record.aliases) if record.aliases else "-"
        print(
            f"{record.kind} | origin={record.origin} | aliases={aliases} | source={record.source}"
        )

    if registry.discovery_errors:
        print("\nDiscovery errors:")
        for error in registry.discovery_errors:
            print(f"  - {error}")


def _inspect_plugin(kind: str) -> None:
    registry = get_source_registry()
    try:
        record = registry.resolve(kind)
    except SourcePluginNotFoundError as exc:
        raise SystemExit(str(exc)) from exc

    plugin = record.plugin
    print(f"requested kind: {record.requested_kind}")
    print(f"resolved kind: {record.kind}")
    print(f"origin: {record.origin}")
    print(f"source: {record.source}")
    print(f"aliases: {', '.join(plugin.aliases) if plugin.aliases else '-'}")
    print(f"display name: {plugin.display_name or '-'}")
    print(f"docs: {plugin.docs_url or '-'}")
    print(
        "required packages: "
        + (", ".join(plugin.required_packages) if plugin.required_packages else "-")
    )
    print(f"healthcheck hint: {plugin.healthcheck_hint or '-'}")


def _doctor(manifest_path: str | None) -> None:
    registry = get_source_registry()
    print("DLTAF plugin doctor")
    print(f"discovered source kinds: {', '.join(registry.available_kinds()) or '<none>'}")

    if registry.discovery_errors:
        print("discovery errors:")
        for error in registry.discovery_errors:
            print(f"  - {error}")

    if not manifest_path:
        return

    manifest = load_manifest(manifest_path)
    requested_kind = str((manifest.get("source") or {}).get("kind") or "").strip()
    print(f"manifest: {Path(manifest_path).resolve()}")
    print(f"requested source.kind: {requested_kind}")
    try:
        record = registry.resolve(requested_kind)
    except SourcePluginNotFoundError as exc:
        raise SystemExit(str(exc)) from exc
    print(f"resolved plugin: {record.kind}")
    print(f"origin: {record.origin}")
    print(f"source: {record.source}")


def _scaffold_plugin(kind: str, output_dir: str, module_name: str | None, legacy_alias: str | None) -> None:
    target_dir = Path(output_dir).expanduser().resolve()
    target_dir.mkdir(parents=True, exist_ok=True)
    module_stem = module_name or kind.replace(".", "_").replace("-", "_")
    target_file = target_dir / f"{module_stem}.py"
    if target_file.exists():
        raise SystemExit(f"Refusing to overwrite existing plugin file: {target_file}")
    target_file.write_text(
        scaffold_plugin_source(kind=kind, module_name=module_stem, legacy_alias=legacy_alias),
        encoding="utf-8",
    )
    print(f"Created plugin scaffold: {target_file}")


def main(argv: Sequence[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description="dltaf plugin and extension tooling")
    subparsers = parser.add_subparsers(dest="command", required=True)

    plugins_parser = subparsers.add_parser("plugins", help="Inspect discovered source plugins")
    plugins_subparsers = plugins_parser.add_subparsers(dest="plugins_command", required=True)

    plugins_subparsers.add_parser("list", help="List built-in and discovered source kinds")

    inspect_parser = plugins_subparsers.add_parser("inspect", help="Show detailed info for one source.kind")
    inspect_parser.add_argument("kind")

    doctor_parser = plugins_subparsers.add_parser("doctor", help="Explain current plugin discovery state")
    doctor_parser.add_argument("--manifest", dest="manifest_path")

    scaffold_parser = subparsers.add_parser("scaffold", help="Generate a local private plugin skeleton")
    scaffold_subparsers = scaffold_parser.add_subparsers(dest="scaffold_command", required=True)
    plugin_scaffold = scaffold_subparsers.add_parser("plugin", help="Generate a source plugin module")
    plugin_scaffold.add_argument("--kind", required=True)
    plugin_scaffold.add_argument("--output-dir", required=True)
    plugin_scaffold.add_argument("--module-name")
    plugin_scaffold.add_argument("--legacy-alias")

    args = parser.parse_args(list(argv) if argv is not None else None)

    if args.command == "plugins" and args.plugins_command == "list":
        _print_plugins()
        return
    if args.command == "plugins" and args.plugins_command == "inspect":
        _inspect_plugin(args.kind)
        return
    if args.command == "plugins" and args.plugins_command == "doctor":
        _doctor(args.manifest_path)
        return
    if args.command == "scaffold" and args.scaffold_command == "plugin":
        _scaffold_plugin(args.kind, args.output_dir, args.module_name, args.legacy_alias)
        return

    raise SystemExit(f"Unsupported command: {args}")


if __name__ == "__main__":  # pragma: no cover
    main()
