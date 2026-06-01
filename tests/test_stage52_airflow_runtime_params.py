from __future__ import annotations

from pathlib import Path

import pytest

from dltaf import build_runtime_overrides, extract_param_defaults, load_runtime_overrides_from_manifest


def test_extract_param_defaults_supports_literal_and_mapping_specs() -> None:
    defaults = extract_param_defaults(
        {
            "bin": {"default": "", "description": "Manual BIN override"},
            "all_periods": True,
            "start_year": {"default": 2024},
        }
    )

    assert defaults == {
        "bin": "",
        "all_periods": True,
        "start_year": 2024,
    }


def test_build_runtime_overrides_applies_transforms_and_skips_blanks() -> None:
    overrides = build_runtime_overrides(
        {
            "runtime_overrides": {
                "bin": {"path": "source.bins.values", "transform": "single_item_list"},
                "project_id": {"path": "source.projects.ids", "transform": "single_int_list"},
                "start_year": {"path": "source.periods.start_year", "transform": "int"},
                "all_periods": {"path": "source.periods.all_periods", "transform": "bool"},
                "replace_scope": "run.replace_scope",
            }
        },
        {
            "bin": "961040001237",
            "project_id": "40",
            "start_year": "2024",
            "all_periods": "true",
            "replace_scope": "window",
            "end_year": "",
        },
    )

    assert overrides == {
        "source.bins.values": ["961040001237"],
        "source.projects.ids": [40],
        "source.periods.start_year": 2024,
        "source.periods.all_periods": True,
        "run.replace_scope": "window",
    }


def test_load_runtime_overrides_from_manifest_reads_airflow_section(tmp_path: Path) -> None:
    manifest_path = tmp_path / "manifest.yaml"
    manifest_path.write_text(
        "\n".join(
            [
                "version: 1",
                "pipeline:",
                "  name: dlt__sample__to__clickhouse__raw",
                "  destination: clickhouse",
                "  dataset: raw",
                "source:",
                "  kind: mongodb",
                "airflow:",
                "  runtime_overrides:",
                "    bin:",
                "      path: source.bins.values",
                "      transform: single_item_list",
            ]
        ),
        encoding="utf-8",
    )

    assert load_runtime_overrides_from_manifest(manifest_path, {"bin": "123"}) == {
        "source.bins.values": ["123"]
    }


def test_build_runtime_overrides_rejects_invalid_transform() -> None:
    with pytest.raises(ValueError, match="Unsupported airflow runtime param transform"):
        build_runtime_overrides(
            {"runtime_overrides": {"bin": {"path": "source.bins.values", "transform": "wat"}}},
            {"bin": "123"},
        )
