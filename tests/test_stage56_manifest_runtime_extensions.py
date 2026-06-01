from __future__ import annotations

from pathlib import Path

from dltaf.services.manifests.extensions import build_runtime_manifest_payload
from dltaf.services.manifests.loader import load_manifest
from dltaf.services.manifests.schema import validate_manifest_schema


def _query_manifest(*, sql_file: str) -> dict:
    return {
        "version": 1,
        "pipeline": {
            "name": "dlt__oracle_custom_sql__to__clickhouse__raw",
            "destination": "clickhouse",
            "dataset": "raw",
        },
        "source": {
            "kind": "oracle_custom_sql",
            "name": "oracle_query",
            "queries": [
                {
                    "name": "sample",
                    "sql_file": sql_file,
                }
            ],
        },
        "catalog": {
            "owner": "data-platform",
            "description": "Consumer-owned metadata extension, not runtime config.",
        },
    }


def test_strict_manifest_schema_ignores_top_level_catalog_metadata() -> None:
    validated = validate_manifest_schema(
        _query_manifest(sql_file="sql/sample.sql"),
        strict=True,
        strict_source=False,
    )

    payload = validated.model_dump(mode="json", by_alias=True, exclude_none=True)
    assert "catalog" not in payload
    assert payload["source"]["kind"] == "oracle_custom_sql"


def test_loader_strips_top_level_catalog_metadata(tmp_path: Path) -> None:
    manifest_path = tmp_path / "dlt__oracle_custom_sql__to__clickhouse__raw.yaml"
    manifest_path.write_text(
        """
version: 1
pipeline:
  name: dlt__oracle_custom_sql__to__clickhouse__raw
  destination: clickhouse
  dataset: raw
source:
  kind: oracle_custom_sql
  name: oracle_query
  queries:
    - name: sample
      sql: select 1 from dual
catalog:
  owner: data-platform
""".strip(),
        encoding="utf-8",
    )

    loaded = load_manifest(manifest_path)

    assert "catalog" not in loaded
    assert loaded["__manifest_path__"] == str(manifest_path.resolve())


def test_runtime_manifest_payload_strips_metadata_and_resolves_sql_files(
    tmp_path: Path,
) -> None:
    manifest_dir = tmp_path / "dlt_manifests"
    sql_dir = tmp_path / "sql"
    manifest_dir.mkdir()
    sql_dir.mkdir()
    sql_file = sql_dir / "sample.sql"
    sql_file.write_text("select 1 from dual", encoding="utf-8")
    manifest_path = manifest_dir / "dlt__oracle_custom_sql__to__clickhouse__raw.yaml"

    payload = build_runtime_manifest_payload(
        _query_manifest(sql_file="../sql/sample.sql"),
        original_manifest_path=manifest_path,
    )

    assert "catalog" not in payload
    assert payload["source"]["queries"][0]["sql_file"] == str(sql_file.resolve())
