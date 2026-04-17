from __future__ import annotations

from pathlib import Path

from dltaf.services.manifests.doctor import analyze_manifest, apply_fixes_to_text, render_template
from dltaf.services.scaffold.service import ScaffoldIntegrationService


def test_manifest_doctor_flags_sql_database_as_deprecated_and_fixable(tmp_path: Path) -> None:
    manifest = tmp_path / "dlt__postgres__to__clickhouse__raw.yaml"
    manifest.write_text(
        """version: 1
pipeline:
  name: dlt__postgres__to__clickhouse__raw
source:
  kind: sql_database
  schema: public
  tables: [orders]
""",
        encoding="utf-8",
    )
    issues = analyze_manifest(
        manifest,
        {
            "version": 1,
            "pipeline": {"name": "dlt__postgres__to__clickhouse__raw"},
            "source": {"kind": "sql_database", "schema": "public", "tables": ["orders"]},
        },
    )
    codes = {issue.code for issue in issues}
    assert "SQL_KIND_CANONICALIZE" in codes
    assert "SQL_KIND_SET_DIALECT" in codes
    assert "SQL_KIND_SET_MODE" in codes

    fixed, applied = apply_fixes_to_text(manifest.read_text(encoding="utf-8"), [i for i in issues if i.fix])
    assert "kind: sqldb" in fixed
    assert "dialect: generic" in fixed
    assert "mode: catalog" in fixed
    assert any("source.kind -> sqldb" in item for item in applied)


def test_manifest_doctor_flags_oracle_aliases_and_templates_are_canonical() -> None:
    issues = analyze_manifest(
        Path("dlt__oracle__to__clickhouse__raw.yaml"),
        {
            "version": 1,
            "pipeline": {"name": "dlt__oracle__to__clickhouse__raw"},
            "source": {"kind": "oracle_custom_sql", "queries": [{"name": "q1"}]},
        },
    )
    assert any(issue.code == "SQL_KIND_CANONICALIZE" for issue in issues)

    canonical_from_legacy = render_template(
        kind="oracle_custom_sql",
        pipeline_name="dlt__oracle__to__clickhouse__raw",
        destination="clickhouse",
        dataset="raw",
    )
    assert "kind: sqldb" in canonical_from_legacy
    assert "dialect: oracle" in canonical_from_legacy
    assert "mode: query" in canonical_from_legacy

    canonical = render_template(
        kind="sqldb_query",
        pipeline_name="dlt__oracle__to__clickhouse__raw",
        destination="clickhouse",
        dataset="raw",
    )
    assert "kind: sqldb" in canonical
    assert "dialect: oracle" in canonical
    assert "mode: query" in canonical


def test_scaffold_rejects_sql_family_builtins(tmp_path: Path) -> None:
    service = ScaffoldIntegrationService(logger=None)
    parser = service.build_parser(prog="dltaf scaffold integration")
    args = parser.parse_args(
        [
            "--pipeline-name",
            "dlt__postgres__to__clickhouse__raw",
            "--source-kind",
            "sql_database",
            "--root-dir",
            str(tmp_path),
        ]
    )
    try:
        service.normalize_options(args)
    except SystemExit as exc:
        message = str(exc)
    else:  # pragma: no cover
        raise AssertionError("Expected SystemExit for sql_database scaffold")
    assert "canonical `sqldb` model" in message
    assert "sqldb_catalog" in message
    assert "sqldb_query" in message
